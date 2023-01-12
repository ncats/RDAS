from django.template.loader import render_to_string
from server.celery import app
from fcm_django.models import FCMDevice
from celery.schedules import crontab
from django.conf import settings
from django.db.models import (
    Case,
    BooleanField,
    Value,
    When,
    Q,
    F
)
import re
from django.core.mail import EmailMultiAlternatives
from django.contrib.contenttypes.models import ContentType
from django.core.mail import send_mail
from firebase_admin.messaging import Message, Notification, AndroidConfig, WebpushConfig, AndroidNotification
from firebase_admin import initialize_app, auth
from server.apps.core.constants import *
from firebase_admin import credentials

DIGEST_SEND_HOUR = 7
DIGEST_SEND_MINUTE = 5
base_url = f"https://{settings.FRONT_END_SUB_DOMAIN}{settings.FRONT_END_DOMAIN}"

# Digest email options
DIGEST_EMAIL_DAILY = "Daily"
DIGEST_EMAIL_WEEKLY = "Weekly"

CTGOV_UPDATES_HOUR = 6
CTGOV_UPDATES_MINUTE = 5


cred = credentials.Certificate(settings.FIREBASE_CREDENTIALS)
initialize_app(cred)

@app.on_after_finalize.connect
def app_ready(sender,**_):
    sender.add_periodic_task(
        crontab(hour=DIGEST_SEND_HOUR, minute=DIGEST_SEND_MINUTE),
        send_email_digest.s(1),
    )

    sender.add_periodic_task(
        crontab(hour=DIGEST_SEND_HOUR + 1, minute=DIGEST_SEND_MINUTE, day_of_week=2),
        send_email_digest.s(7),
    )

@app.task
def send_email_digest(DAYS):
    from server.apps.core.models import (CureReport,Discussion,Article,Event,Comment,Report, Profile, ClinicalTrial)
    from datetime import datetime, timedelta
    import pytz
    import copy

    not_empty = False
    yesterday = datetime.today() - timedelta(days=DAYS)

    date = datetime(
        yesterday.year,
        yesterday.month,
        yesterday.day,
        DIGEST_SEND_HOUR,
        DIGEST_SEND_MINUTE,
        0,
        0,
        pytz.UTC,
    )

    profiles = (
        Profile.objects.select_related("user").filter(notifications__notifications_do_not_disturb=False).only("user__first_name")
    )
    if DAYS== 1:
        profiles = profiles.exclude(notifications__email_period="weekly")
    else:
        profiles = profiles.exclude(notifications__email_period="daily")

    for profile in profiles:
        data = {
            "reports": {},
            "discussions": {},
            "jarticles": [],
            "narticles": [],
            "events": [],
            "comments": []
        }

        context = {
            "yesterday": date.strftime("%m-%d-%Y"),
            "data": data,
            "digest_date": datetime.today().strftime("%m-%d-%Y"),
        }
        reports = CureReport.objects.filter(
            (Q(created__gte=date) | Q(updated__gte=date)),
            status=APPROVED,
        ).select_related("report").order_by("created")
        if profile.notifications.get('notification_case_favor') and not profile.notifications.get('notification_case_all'):
            reports = reports.filter(report__disease__in = profile.favorited_diseases.all())
        if not profile.notifications.get('notification_case_favor') and not profile.notifications.get('notification_case_all'):
            reports = ()
        if reports:
            for r in reports:
                if r.report.disease.name not in data["reports"]:
                    data["reports"][r.report.disease.name] = [
                        f"{base_url}explore/cases/{r.report.disease.id}",
                        [],
                        [],
                    ]
                rep = {
                    "report_id": r.id,
                    "url": f"{base_url}explore/cases/{r.report.disease.id}/case-details/{r.id}",
                    "drugs": ", ".join([d.name for d in r.report.drugs.all()]),
                    "outcome": r.report.outcome,
                }

                data["reports"][r.report.disease.name][1].append(rep)
                not_empty=True

        discussions = Discussion.objects.filter(
                (Q(created__gte=date) | Q(updated__gte=date)),
                deleted=False,
                flagged=False,
                status=APPROVED,
            ).select_related("disease").annotate(
                discussion_id=F("id"),
                discussion__title=F("title"),
            ).values("discussion_id", "discussion__title", "disease_id", "disease__name")
        if profile.notifications.get('notification_post_favor') and not profile.notifications.get('notification_post_all'):
            discussions = discussions.filter(disease__in = profile.favorited_diseases.all())
        if not profile.notifications.get('notification_post_favor') and not profile.notifications.get('notification_post_all'):
            discussions = ()
        if discussions:
            for d in discussions:
                if d["disease__name"] not in data["discussions"]:
                    data["discussions"][d["disease__name"]] = [
                        f"{base_url}explore/discussions/{d['disease_id']}",
                        [],
                        [],
                    ]
                data["discussions"][d["disease__name"]][1].append(
                    {
                        "id": d['discussion_id'],
                        "title": d["discussion__title"],
                        "url": f"{base_url}explore/discussion/discussion-details/{d['discussion_id']}",
                        "is_comment": False,
                    }
                )
                not_empty=True

        journals = Article.objects.filter((Q(created__gte=date) | Q(updated__gte=date)),publication_type='journal', status=APPROVED)
        if profile.notifications.get('notification_journal_favor') and not profile.notifications.get('notification_journal_all'):
            journals = journals.filter(disease__in = profile.favorited_diseases.all())
        if not profile.notifications.get('notification_journal_favor') and not profile.notifications.get('notification_journal_all'):
            journals = ()
        if journals:
            for item in journals:
                data["jarticles"].append(
                    {
                        "id": item.id,
                        "title": item.title,
                        "url": f"{base_url}explore/article/article-details/{item.id}",
                        "journal_name": item.publication_name,
                        "abstract": item.abstract,
                    }
                )
            not_empty=True

        news = Article.objects.filter((Q(created__gte=date) | Q(updated__gte=date)), publication_type='news', status=APPROVED)
        if profile.notifications.get('notification_news_favor') and not profile.notifications.get('notification_news_all'):
            news = news.filter(disease__in = profile.favorited_diseases.all())
        if not profile.notifications.get('notification_news_favor') and not profile.notifications.get('notification_news_all'):
            news = ()
        if news:
            for item in news:
                data["narticles"].append(
                    {
                        "id": item.id,
                        "title": item.title,
                        "url": f"{base_url}explore/article/article-details/{item.id}",
                        "publication_name": item.publication_name,
                        "abstract": item.abstract,
                    }
                )
                not_empty=True

        events = Event.objects.filter((Q(created__gte=date) | Q(updated__gte=date)), status=APPROVED)
        if profile.notifications.get('notification_event_favor') and not profile.notifications.get('notification_event_all'):
            events = events.filter(disease__in = profile.favorited_diseases.all())
        if not profile.notifications.get('notification_event_favor') and not profile.notifications.get('notification_event_all'):
            events = ()
        if events:
            for item in events:
                data["events"].append(
                    {
                        "id":item.id,
                        "title": item.title,
                        "event_description": item.event_description,
                        "location": item.location,
                        "url": f"{base_url}explore/event/event-details/{item.id}",
                    }
                )
                not_empty=True
        # Need to figure out what informationnto send for comments
        #comments = Comment.objects.filter((Q(created__gte=date) | Q(updated__gte=date)), parent__isnull=True)
        # if profile.notifications.get('notification_comment_favor') and not profile.notifications.get('notification_comment_all'):
        #     comments = [comments for comment in comments if comment.content_object.disease in profile.favorited_diseases.all()]
        # if not profile.notifications.get('notification_comment_favor') and not profile.notifications.get('notification_comment_all'):
        #     comments = ()

        # if comments:
        #     for comment in comments:
        #         if isinstance(comment.content_object,Report):
        #             drugnames = comment.content_object.regimens.all().values_list('drug__name', flat=True)
        #             drugs = ', '.join(drugnames)
        #             parent= f"Treated with {drugs}, outcome {comment.content_object.outcome}\n\nSee details"
        #             parent_link = f"{base_url}explore/cases/case-details/{comment.content_object.id.id}"
        #         if isinstance(comment.content_object, ClinicalTrial):
        #             parent = f"{comment.content.title}\n\nSee details"
        #             parent_link = f"{base_url}explore/clinical-trials/clinical-trial-details/{comment.content_object.id}"
        #         if isinstance(comment.content_object,Discussion):
        #             parent = f"{comment.content_object.title}\n\nSee details"
        #             parent_link = f"{base_url}explore/discussions/discussion-details/{comment.content_object.id}"
        #         if isinstance(comment.content_object,Article):
        #             parent = f"{comment.content_object.title}\n\nSee details",
        #             parent_link = f"{base_url}explore/articles/article-details/{comment.content_object.id}"
        #         if isinstance(comment.content_object,Event):
        #             parent = f"{comment.content_object.title}\n\nSee details"
        #             parent_link = f"{base_url}explore/events/event-details/{comment.content_object.id}"

        #         data['comments'].append(
        #             {
        #                 "id": comment.id,
        #                 # "body": comment.body,
        #                 "parent": parent,
        #                 "parent_link": parent_link
        #             }
        #         )
        #         not_empty=True

        do_not_send = all(value=={} or value == [] for value in data.values())
        if not do_not_send:
            try:
                firebase_user=auth.get_user(profile.user.username)
                if not_empty and firebase_user:
                    data["period"] =  "24 hours" if DAYS == 1 else "week",
                    email = firebase_user.email
                    salt = profile.user.id * 2 + 27
                    context[
                        "unsubscribe"
                    ] = f"https://cure.api{settings.API_DOMAIN}"
                    context[
                        "unsubscribe"
                    ] += f"unsubscribe?email={email}&salt={salt}"
                    context["first_name"] = profile.user.first_name
                    context["email_address"] = email
                    send_email_task.delay(
                        "daily_notifications_digest_subject.txt",
                        "daily_notifications_digest_body.txt",
                        context,
                        "CURE ID <%s>" % settings.DEFAULT_FROM_EMAIL,
                        [
                            email,
                        ],
                        None,
                        None,
                        "daily_notifications_digest_body.html",
                        )
            except Exception as e:
                print(str(e))
                pass


@app.task
def send_email_task(
    subject_template_name,
    body_template_name,
    context,
    from_email,
    to_email_list,
    bcc_email_list=None,
    reply_to_email_list=None,
    html_message=None,
    promotion=False,
):
    """Equal to send_mail, but it is a background task"""
    if not promotion:
        body = render_to_string(body_template_name, context)
        subject = ''.join(render_to_string(subject_template_name, context).strip())
    else:
        body = body_template_name
        subject = subject_template_name

    email = EmailMultiAlternatives(
        subject=subject,
        body=body,
        from_email=from_email,
        to=to_email_list,
        bcc=bcc_email_list,
        reply_to=reply_to_email_list,
    )
    if html_message:
        html_body = render_to_string(html_message, context)
        email.attach_alternative(html_body, "text/html")
    email.send(fail_silently=False)

@app.task
def send_push_notification(data):
    from server.apps.core.models import UnseenNews
    disease_id = data.pop('disease_id', None)
    type = data.pop('obj_type')
    author = data.pop('author')
    import datetime as dt
    now = dt.datetime.utcnow().strftime("%H:%M")
    try:
        devices = FCMDevice.objects.filter(
            user__profile__notifications__notifications_do_not_disturb=False
        ).select_related("user", "user__profile")
        devices = devices.exclude(user_id=int(author))
        devices = devices.distinct("registration_id")
        # devices = devices.exclude(
        #     Q(user__profile__notifications__quiet_time=True)
        #     & (
        #         Q(user__profile__notifications__quiet_time_start__isnull=True)
        #         | Q(user__profile__notifications__quiet_time_start="")
        #         | Q(user__profile__notifications__quiet_time_end__isnull=True)
        #         | Q(user__profile__notifications__quiet_time_end="")
        #     )
        # )
        # devices = devices.annotate(
        #     during_silent_time=Case(
        #     # start == end
        #         When(
        #             user__profile__notifications__quiet_time=True,
        #             user__profile__notifications__quiet_time_end=F("user__profile__notifications__quiet_time_end"),
        #             user__profile__notifications__quiet_time_start=now,
        #             then=Value(True),
        #         ),
        #         # both times same day, for ex. 06:10 -> 23:20
        #         When(
        #             user__profile__notifications__quiet__time=True,
        #             user__profile__notifications__quiet_time_start__lt=F(
        #                 'user__profile__notifications__quiet_time_end'
        #             ),
        #             user__profile__notifications__quiet_time_start__lte=now,
        #             user__profile__notifications__quiet_time_end__gte=now,
        #             then=Value(True),
        #         ),
        #         # start and end time are in different days, meaning the silent
        #         # time is during the night, for ex. 22:10 -> 08:00
        #         #  and current time is "today" after start
        #         When(
        #             user__profile__notifications__quiet_time=True,
        #             user__profile__notifications__quiet_time_end__lt=F(
        #                 "user__profile__notifications__quiet_time_start"
        #             ),
        #             user__profile__notifications__quiet_time_start__lte=now,
        #             then=Value(True),
        #         ),
        #         # start and end time are in different days, meaning the silent
        #         # time is during the night, for ex. 22:10 -> 08:00
        #         #  and current time is "tomorrow" before end
        #         When(
        #             user__profile__notifications__quiet_time=True,
        #             user__profile__notifications__quiet_time_end__lt=F(
        #                 "user__profile__notifications__quiet_time_start"
        #             ),
        #             user__profile__notifications__quiet_time_end__gte=now,
        #             then=Value(True),
        #         ),
        #         default=Value(False),
        #         output_field=BooleanField(),
        #         )
        # ).exclude(during_silent_time=True)

        if type== "Report":
            devices = devices.exclude(
                user__profile__notifications__notification_case_all_push=False,
                user__profile__notifications__notification_case_favor_push=False,
            )
            devices = devices.exclude(
                Q(user__profile__notifications__notification_case_all_push=False)
                & Q(user__profile__notifications__notification_case_favor_push=True)
                & ~Q(user__profile__favorited_diseases=disease_id)
            )
        elif type == "Discussion":
            devices = devices.exclude(
                user__profile__notifications__notification_post_all_push=False,
                user__profile__notifications__notification_post_favor_push=False,
            )
            devices = devices.exclude(
                Q(user__profile__notifications__notification_post_all_push=False)
                & Q(user__profile__notifications__notification_post_favor_push=True)
                & ~Q(user__profile__favorited_diseases=disease_id)
            )
        elif type == "ClinicalTrial":
            devices = devices.exclude(
                user__profile__notifications__notification_clinical_trial_all_push=False,
                user__profile__notifications__notification_clinical_trial_favor_push=False,
            )
            devices = devices.exclude(
                Q(user__profile__notifications__notification_clinical_trial_all_push=False)
                & Q(user__profile__notifications__notification_clinical_trial_favor_push=True)
                & ~Q(user__profile__favorited_diseases=disease_id)
            )
        elif type == 'Article':
            if data['publication_type'] == 'news':
                devices = devices.exclude(
                    user__profile__notifications__notification_news_all_push=False,
                    user__profile__notifications__notification_news_favor_push=False,
                )
                devices = devices.exclude(
                    Q(user__profile__notifications__notification_news_all_push=False)
                    & Q(user__profile__notifications__notification_news_favor_push=True)
                    & ~Q(user__profile__favorited_diseases=disease_id)
                )
            else:
                devices = devices.exclude(
                    user__profile__notifications__notification_journal_all_push=False,
                    user__profile__notifications__notification_journal_favor_push=False,
                )
                devices = devices.exclude(
                    Q(user__profile__notifications__notification_journal_all_push=False)
                    & Q(user__profile__notifications__notification_journal_favor_push=True)
                    & ~Q(user__profile__favorited_diseases=disease_id)
                )

        elif type == "Event":
            devices = devices.exclude(
                user__profile__notifications__notification_event_all_push=False,
                user__profile__notifications__notification_event_favor_push=False,
            )
            devices = devices.exclude(
                Q(user__profile__notifications__notification_event_all_push=False)
                & Q(user__profile__notifications__notification_event_favor_push=True)
                & ~Q(user__profile__favorited_diseases=disease_id)
            )
        elif type in ["Comment"]:
            devices = devices.exclude(
                user__profile__notifications__notification_comment_all_push=False,
                user__profile__notifications__notification_comment_favor_push=False,
            )
            devices = devices.exclude(
                Q(user__profile__notifications__notification_comment_all_push=False)
                & Q(user__profile__notifications__notification_comment_favor_push=True)
                & ~Q(user__profile__favorited_diseases=disease_id)
            )

        for device in devices:
            message = Message(notification=Notification(title=data['title'], body=data['body']), data={"page": data["link"]}, token=device.registration_id)
            result = device.send_message(message)
    except Exception as e:
        print(str(e))


#sendapproved
#sendrejected
#send comment report,article,discussion,event,ct
#send report article event discussion ct for disease interest

@app.task
def clear_notifications_task(user_id, path):
    from server.apps.core.models import UnseenNews, Newsfeed
    import re

    if "v2/newsfeed" in path:
        UnseenNews.objects.filter(user_id=user_id).delete()
        return

    discussion = re.search(r"v2/discussions/(\d+)", path)
    if discussion:
        newsfeeds = Newsfeed.objects.filter(
            object_type="Discussion", object_id=discussion.groups()[0]
        ).values_list("id", flat=True)
        UnseenNews.objects.filter(user_id=user_id, newsfeed_id__in=newsfeeds).delete()
        return

    report = re.search(r"v2/reports/(\d+)", path)
    if report:
        newsfeeds = Newsfeed.objects.filter(
            object_type="Report", object_id=report.groups()[0]
        ).values_list("id", flat=True)
        UnseenNews.objects.filter(user_id=user_id, newsfeed_id__in=newsfeeds).delete()
        return

    clinicaltrial = re.search(r"v2/clinical-trials/(\d+)", path)
    if clinicaltrial:
        newsfeeds = Newsfeed.objects.filter(
            object_type="ClinicalTrial", object_id=report.groups()[0]
        ).values_list("id", flat=True)
        UnseenNews.objects.filter(user_id=user_id, newsfeed_id__in=newsfeeds).delete()
        return

    article = re.search(r"v2/articles/(\d+)", path)
    if article:
        newsfeeds = Newsfeed.objects.filter(
            object_type="Article", object_id=report.groups()[0]
        ).values_list("id", flat=True)
        UnseenNews.objects.filter(user_id=user_id, newsfeed_id__in=newsfeeds).delete()
        return

    event = re.search(r"v2/events/(\d+)", path)
    if event:
        newsfeeds = Newsfeed.objects.filter(
            object_type="Event", object_id=event.groups()[0]
        ).values_list("id", flat=True)
        UnseenNews.objects.filter(user_id=user_id, newsfeed_id__in=newsfeeds).delete()
        return

@app.task
def send_disease_oi_notification(email_list, type_, object_id):
    from django.contrib.auth.models import User
    from django.apps import apps

    model = apps.get_model('core', type_)
    object = model.objects.filter(id=object_id).first()

    try:
        for user in email_list:
            user_to_send= User.objects.filter(id=user).first()
            ctx_dict = {
                "type": str(object.__class__.__name__),
                "first_name": user_to_send.first_name,
                "disease": object.disease.name,
                "front_end_sub_domain": settings.FRONT_END_SUB_DOMAIN,
                "front_end_domain": settings.FRONT_END_DOMAIN,
                "end_of_url": f"explore/cases/{object.disease.id}/case-details/{object.id}",
                "report_id": object_id,
            }
            subject = render_to_string(
                    "new_post_for_disease_oi_subject.txt", ctx_dict
                )
            message_text = render_to_string(
                    "new_post_for_disease_oi_body.txt", ctx_dict
                )
                # Email subject *must not* contain newlines
            subject = "".join(subject.splitlines())
            send_mail(
                subject,
                message_text,
                "CURE ID <%s>" % settings.DEFAULT_FROM_EMAIL,
                [user_to_send.email],
                fail_silently=False,
            )
    except Exception as e:
        print(str(e))

@app.task
def send_comment_notification(comment, object):
    detail_type = ''
    from server.apps.core.models import CureReport, Report, Discussion, ClinicalTrial, Article, Event, Comment
    #get author first name from reverse relation
    user_info = CureReport.objects.filter(id=comment.object_id).values(first_name=F('author__profile__user__first_name'), email=F('author__profile__user__email')).first()
    if isinstance(object, Report):
        drugs = object.drugs.values_list("name", flat=True)
        tail = ""
        drug_string = ""

        if drugs.count() > 3:
            tail = "+ %s more," % (drugs.count() - 3)

        drugs = drugs[:3:]

        for drug in drugs:
            drug_string += "%s, " % drug

        ctx_dict = {
            "type": "Report",
            "first_name": user_info.get('first_name'),
            "drugs": drug_string,
            "tail": tail,
            "front_end_sub_domain": settings.FRONT_END_SUB_DOMAIN,
            "front_end_domain": settings.FRONT_END_DOMAIN,
            "end_of_url": f"explore/cases/{object.disease.id}/case-details/{object.id}",
        }
        subject = render_to_string("new_comment_on_report_subject.txt", ctx_dict)
        message_text = render_to_string("new_comment_on_report_body.txt", ctx_dict)
        # Email subject *must not* contain newlines
        subject = "".join(subject.splitlines())
        send_mail(
            subject,
            message_text,
            "CURE ID <%s>" % settings.DEFAULT_FROM_EMAIL,
            [user_info.get('email')],
            fail_silently=False,
        )
    else:
        if isinstance(object, ClinicalTrial):
            detail_type = 'clinical-trial-details'
        if isinstance(object, Discussion):
            detail_type = 'discussion-details'
        if isinstance(object, Article):
            if object.publication_type == 'news':
                detail_type = 'news-details'
            else:
                detail_type = 'journal-details'

        if isinstance(object, Event):
            detail_type = 'evemt-details'

        if isinstance(object, Comment):
            detail_type = 'comment-details'

        ctx_dict = {
                "tyoe": str(object.__class__.__name__),
                "first_name": object.author.first_name,
                "title": object.title,
                "front_end_sub_domain": settings.FRONT_END_SUB_DOMAIN,
                "front_end_domain": settings.FRONT_END_DOMAIN,
                "end_of_url": f"explore/discussions/{object.disease.id}/{detail_type}/{object.id}",
            }
        subject = render_to_string("new_comment_subject.txt", ctx_dict)
        message_text = render_to_string("new_comment_body.txt", ctx_dict)
        # Email subject *must not* contain newlines
        subject = "".join(subject.splitlines())
        send_mail(
            subject,
            message_text,
            "CURE ID <%s>" % settings.DEFAULT_FROM_EMAIL,
            [object.author.email],
            fail_silently=False,
        )

@app.task
def ctgov_updates():
    """Download updated ClinicalTrials from CT.gov"""
    from datetime import datetime, timedelta
    import pytz
    import requests
    from server.apps.core.models import ClinicalTrial
    from server.apps.core.signals import create_newsfeed
    from django.db.models.signals import post_save

    # Three days back, a bit of overlap shouldn't be a problem
    date = datetime.today() - timedelta(days=3)

    fields = [
        "NCTId",
        "BriefTitle",
        "Condition",
        "LeadSponsorName",
        "OverallStatus",
        "Phase",
        "EnrollmentCount",
        "StartDate",
        "LocationCountry",
        "InterventionName",
        "InterventionType",
        "StudyType",
    ]

    # get updates
    base_url = "https://www.clinicaltrials.gov/api/query/study_fields?expr=AREA%5BLastUpdatePostDate%5D+EXPAND%5BTerm%5D"
    base_url = f"{base_url}+RANGE%5B{date.month}%2F{date.day}%2F{date.year}%2C+MAX%5D"
    base_url = f"{base_url}&fields={'%2C'.join(fields)}&fmt=json"

    min_rnk = 1
    max_rnk = 1000
    continue_ = True
    data = []

    diseases = _diseases()
    possible_covid_names = ["covid-19"]
    if "covid-19" in diseases:
        covid_id = diseases["covid-19"]
        possible_covid_names = [i for i in diseases if diseases[i] == covid_id]

    while continue_:
        url = f"{base_url}&min_rnk={min_rnk}&max_rnk={max_rnk}"
        response = requests.get(url)
        results = response.json()["StudyFieldsResponse"]

        continue_ = results["NStudiesFound"] > results["MaxRank"]
        min_rnk += results["NStudiesReturned"]
        max_rnk += results["NStudiesReturned"]

        # Need this list inside _good_ct
        for ct in results["StudyFields"]:
            ct["extra_field"] = possible_covid_names

        data += list(filter(_good_ct, results["StudyFields"]))

    trial_ids = [i["NCTId"][0] for i in data]
    trials = ClinicalTrial.objects.filter(clinical_trials_gov_id__in=trial_ids)
    exist = {i.clinical_trials_gov_id: i for i in trials}

    post_save.disconnect(create_newsfeed, sender=ClinicalTrial)
    for ct in data:
        ct = _comb_clinicaltrial(ct)
        outcome = _identify_disease(ct, diseases)

        if not outcome[0]:
            continue

        ct.pop("disease")
        ct["disease_id"] = diseases[outcome[0]]
        # Matched_against field has max_length = 4096
        # and sometimes that value is longer
        ct["matched_against"] = outcome[1][:4096]

        if ct["clinical_trials_gov_id"] in exist:
            ct.pop("disease_id")
            ctid = ct.pop("clinical_trials_gov_id")
            different = False
            for key in ct:
                if ct[key] != getattr(exist[ctid], key):
                    setattr(exist[ctid], key, ct[key])
                    different = True
            if different:
                exist[ctid].save()
        else:
            ClinicalTrial.objects.create(**ct)
    post_save.connect(create_newsfeed, sender=ClinicalTrial)


def _good_ct(ct):
    """Do we want to insert that ClinicalTrial

    We only insert "Interventional" CTs
    Those that have "Drug" in treatments
    Or "Biological" any for Covid and w/o "vaccine/vaccination/etc" for other diseases

    """
    study_type = (
        "StudyType" not in ct
        or not len(ct["StudyType"])
        or ct["StudyType"][0] != "Interventional"
    )
    if study_type:
        return False

    has_drugs = "Drug" in ct["InterventionType"]
    other_intervention = False
    for ndx in range(len(ct["InterventionType"])):
        if "Other" in ct["InterventionType"]:
            if "drug" not in ct["InterventionName"][ndx].lower():
                other_intervention = True
                break
    if has_drugs or other_intervention:
        return True

    possible_covid_names = ct["extra_field"]
    all_conditions = ",".join([i.lower() for i in ct["Condition"]])
    covid19 = bool(len([i for i in possible_covid_names if i in all_conditions]))
    vaccine_not_found = "vaccin" not in ",".join(ct["InterventionName"]).lower()
    brieftitle = ct["BriefTitle"][0] if ct["BriefTitle"] else ""
    vaccine_not_found = vaccine_not_found and "vaccin" not in brieftitle.lower()
    biological = "Biological" in ct["InterventionType"] and (
        covid19 or vaccine_not_found
    )
    return biological


def _diseases():
    """get {Disease_Name: Disease_ID} dict."""

    from apps.api.models import Disease

    ct_to_cure = {
        "Abscess": "Abscess",
        "Acanthamoeba": "Acanthamoeba",
        "Acinteobacter": "Acinetobacter",
        "Actinomycosis": "Actinomycosis",
        "Adenovirus": "Adenovirus",
        "Aerococcus": "Aerococcus ",
        "African swine fever": "African Swine Fever ",
        "African trypanosomiasis": "African Trypanosomiasis",
        "Aggregatibacter": "Aggregatibacter",
        '"Amoebiasis': ' Intestinal",Amoebiasis (intestinal)',
        '"Keratitis': ' Acanthamoeba",Amoebic keratitis',
        "Anaplasmosis": "Anaplasmosis",
        "Angiostrongylosis": "Angiostrongyliasis",
        "Anisakiasis": "Anisakiasis",
        "Anthrax": "Anthrax",
        "Appendicitis": "Appendicitis",
        "Ascariasis": "Ascariasis",
        "Aspergillosis": "Aspergillosis",
        "B virus": "B virus",
        "Babesiosis": "Babesiosis",
        "Bacillary angiomatosis": "Bacillary angiomatosis",
        "Bacteremia": "Bacteremia",
        "Bacterial Vaginoses": "Bacterial vaginosis",
        "Bacteroides": "Bacteroides",
        "Balamuthia": "Balamuthia",
        "Balantidiasis": "Balantidiasis",
        "Barmah forest fever": "Barmah forest fever",
        "Bartonellosis": "Bartonellosis",
        "Basidiobolomycosis": "Basidiobolomycosis",
        "Baylisascariasis": "Baylisascaris",
        "Bejel": "Bejel",
        "Bifidobacterium ": "Bifidobacterium ",
        "BK virus": "BK virus",
        "Blastomycosis": "Blastomycosis",
        "Blastoschizomyces capitatus ": "Blastoschizomyces capitatus ",
        "Botulism": "Botulism",
        '"Bronchitis AND ""infection"" (in other terms)"': "Bronchitis",
        "Brucellosis": "Brucellosis",
        '"Burns AND ""infection"" (in other terms)"': "Burns",
        "Bursitis": "Bursitis",
        "Buruli ulcer": "Buruli ulcer",
        "California encephalitis": "California encephalitis",
        "Campylobacter Infections": "Campylobacter ",
        "Candida ": "Candida ",
        "Candida auris ": "Candida auris ",
        "Cardiobacterium": "Cardiobacterium",
        "Cat-scratch disease": "Cat scratch fever",
        "Cellulitis": "Cellulitis",
        "Central Nervous System Protozoal Infections": "Central Nervous System Protozoal Infections",
        "Cervicitis": "Cervicitis",
        "Chagas disease": "Chagas disease",
        "Chancroid": "Chancroid",
        "Chandipura virus": "Chandipura virus",
        "Chickenpox": "Chickenpox",
        "Chikungunya": "Chikungunya",
        "Chlamydia": "Chlamydia",
        '"""Chlamydia pneumoniae"" (in other terms)"': "Chlamydia pneumoniae",
        "Chlamydia trachomatis": "Chlamydia trachomatis",
        "Cholangitis": "Cholangitis",
        "Cholecystitis": "Cholecystitis",
        "Cholera": "Cholera",
        "Chromoblastomycosis": "Chromoblastomycosis",
        '"""Citrobacter"" (in other terms)"': "Citrobacter",
        "Clonorchis": "Clonorchiasis",
        "Clostridial Infections": "Clostridia other than C. difficile",
        "Clostridium difficile": "Clostridium difficile",
        "Clostridial necrotizing enteritis": "Clostridial necrotizing enteritis",
        "Coccidioidomycosis": "Coccidioidomycosis",
        '"Colitis AND ""infection"" (in other terms)"': "Colitis",
        "Colorado tick fever": "Colorado tick fever",
        "Conidiobolomycosis": "Conidiobolomycosis",
        '"Conjunctivitis AND ""infection"" (in other terms)"': "Conjunctivitis",
        "Corynebacterium NOT Diphtheria": "Corynebacterium ",
        "Covid19": "COVID-19",
        "Covid 19": "COVID-19",
        "SARS-CoV-2": "COVID-19",
        "Sars-CoV2": "COVID-19",
        "Sars-CoV 2": "COVID-19",
        "2019-nCoV": "COVID-19",
        "Corona virus": "COVID-19",
        "Coronavirus": "COVID-19",
        "Creutzfeldt-Jakob disease ": "Creutzfeldt-Jakob disease ",
        "Crimean-Congo Haemorrhagic Fever": "Crimean-Congo haemorrhagic fever (CCHF)",
        "Cryptococcosis": "Cryptococcosis",
        "Cryptosporidiosis": "Cryptosporidiosis",
        "Cyclosporiasis": "Cyclosporiasis",
        "Cysticercosis": "Cysticercosis",
        '"Cystitis AND ""infection"" (in other terms)"': "Cystitis",
        "Cystoisosporiasis OR Isosporiasis": "Cystoisosporiasis (Isosporiasis)",
        "Cytomegalovirus": "Cytomegalovirus",
        "Dengue": "Dengue",
        '"Dermatitis AND ""infection"" (in other terms)"': "Dermatitis",
        "Diabetic foot infections": "Diabetic foot infections",
        '"Diarrhea AND ""infection"" (in other terms)"': "Diarrhea (infectious)",
        "Diphtheria": "Diphtheria",
        "Dirofilariasis": "Dirofilariasis",
        '"Diverticulitis AND ""infection"" (in other terms)"': "Diverticulitis",
        "Dracunculiasis": "Dracunculiasis",
        "Eastern equine encephalitis (Eee)": "Eastern equine encephalitis",
        "Ebola virus disease": "Ebola virus disease",
        "Echinococcosis (Hydatid Disease)": "Echinococcosis",
        "Ehrlichiosis": "Ehrlichiosis",
        '"""Eikenella"" (in other terms)"': "Eikenella",
        "Empyema": "Empyema",
        '"Encephalitis AND ""infection"" (in other terms)"': "Encephalitis",
        '"Endocarditis AND ""infection"" (in other terms)"': "Endocarditis",
        '"Endometritis AND ""infection"" (in other terms)"': "Endometritis",
        '"Endomyometritis AND ""infection"" (in other terms)"': "Endomyometritis",
        '"Enteritis AND ""infection"" (in other terms)"': "Enteritis",
        "Enterobacter": "Enterobacter",
        "Enterobiasis": "Enterobiasis",
        "Enterococcal infection": "Enterococcal infection",
        "Enterotoxigenic Escherichia Coli Infection": "Enterotoxigenic E. coli (ETEC)",
        "Entomophthoramycosis": "Entomophthoramycosis",
        "Epidemic Pleurodynia": "Epidemic Pleurodynia",
        "Epidermophyton floccosum": "Epidermophyton floccosum",
        "Epididymitis": "Epididymitis",
        "Epstein-Barr Virus Infections": "Epstein-Barr virus",
        "Erisypelas": "Erysipelas",
        "Erysipeloid": "Erysipeloid",
        "Erythrasma": "Erythrasma",
        "Escherichia Coli Infections": "Escherichia coli",
        '"Esophagitis AND ""infection"" (in other terms)"': "Esophagitis",
        "Eubacterium NOT Bacterium": "Eubacterium ",
        '"Fasciitis AND ""infection"" (in other terms)"': "Fasciitis",
        "Fascioliasis": "Fascioliasis",
        "Fasciolopsiasis": "Fasciolopsiasis",
        "Febrile neutropenia": "Febrile neutropenia",
        "Fever of unknown origin": "Fever of unknown origin",
        "Furunculus": "Furunculous",
        "Fusarium infection": "Fusarium ",
        "Fusobacterium": "Fusobacterium",
        '"Gastroenteritis AND ""infection"" (in other terms)"': "Gastroenteritis",
        "Genital herpes": "Genital herpes",
        "Giardiasis": "Giardiasis",
        '"Gingivitis AND ""infection"" (in other terms)"': "Gingivitis",
        "Gonorrhea": "Gonorrhea",
        "Granuloma inguinale": "Granuloma inguinale",
        "Granulomatous amoebic encephalitis": "Granulomatous amoebic encephalitis",
        "Haemophilus infection": "Haemophilus",
        '"Hand': ' Foot and Mouth Disease",Hand-Foot-and-Mouth Disease',
        "Hantavirus infection": "Hantavirus",
        "Helicobacter pylori infection": "Helicobacter pylori",
        "Hendra virus": "Hendra virus",
        "Hepatitis": "Hepatitis",
        "Hepatitis A virus infection": "Hepatitis A ",
        "Hepatitis B virus infection": "Hepatitis B",
        "Hepatitis C virus infection": "Hepatitis C",
        "Hepatitis D virus infection": "Hepatitis D",
        "Hepatitis E virus infection": "Hepatitis E",
        "Herpangina": "Herpangina",
        "Herpes Labialis": "Herpes labialis (cold sores)",
        "Herpes simplex": "Herpes simplex",
        "Herpes zoster": "Herpes zoster",
        "Heterophyiasis": "Heterophyiasis",
        "Histoplasmosis": "Histoplasmosis",
        "HIV-1-Infection": "HIV-1",
        "HIV-2 infection": "HIV-2",
        "Hookworm infection": "Hookworm",
        "HPV Infection": "HPV",
        "Human T-Cell Lymphotropic Virus Type I Infection": "Human T-cell lymphotropic virus type I",
        "Impetigo": "Impetigo",
        "Infectious mononucleosis": "Infectious mononucleosis",
        "Influenza Viral Infections": "Influenza",
        "Intra-abdominal infections": "Intra-abdominal infections",
        "Jamestown Canyon River virus": "Jamestown Canyon River virus",
        "Japanese encephalitis": "Japanese encephalitis",
        "JC virus": "JC virus",
        "Kaposi's sarcoma": "Kaposi's sarcoma",
        "Keratitis": "Keratitis",
        "Kingella": "Kingella",
        "Klebsiella": "Klebsiella",
        "Kuru": "Kuru",
        "Kyansur Forest Disease ": "Kyansur Forest Disease ",
        "La Crosse encephalitis": "La Crosse encephalitis",
        "Laryngitis": "Laryngitis",
        "Lassa fever": "Lassa fever",
        "Legionella infection": "Legionella",
        "Leishmaniasis": "Leishmaniasis",
        "Leprosy": "Leprosy",
        "Leptospirosis": "Leptospirosis",
        "Listeriosis": "Listeriosis",
        "Loa Loa infection": "Loa Loa ",
        "Louping ill": "Louping ill",
        "Lyme disease": "Lyme disease",
        "Lymphangitis": "Lymphangitis",
        "Lymphatic filariasis": "Lymphatic filariasis",
        "Lymphocytic choriomeningitis": "Lymphocytic choriomeningitis",
        "Lymphogranuloma venereum": "Lymphogranuloma venereum",
        "Malaria": "Malaria",
        "Malassezia furfur infection": "Malassezia furfur",
        "Malassezia": "Malassezia furfur",
        "Mansonella Perstans Infections": "Mansonella ",
        "Marburg disease": "Marburg disease",
        "Mayaro": "Mayaro",
        "Measles": "Measles",
        "Mediterranean spotted fever": "Mediterranean spotted fever",
        "Melioidosis": "Melioidosis",
        "Meningitis": "Meningitis",
        "Meningococcal infections": "Meningococcal infection",
        "Meningoencephalitis": "Meningoencephalitis",
        "Microsporidia ": "Microsporidia ",
        "Microsporum": "Microsporum",
        "Middle East respiratory syndrome (MERS)": "Middle East respiratory syndrome (MERS)",
        "Monkeypox": "Monkeypox",
        "Moraxella ": "Moraxella ",
        "Morganella ": "Morganella ",
        "Mucormycosis": "Mucormycosis",
        "Mumps": "Mumps",
        "Murray Valley encephalitis": "Murray Valley encephalitis",
        "Mycetoma": "Mycetoma",
        "Mycoplasma": "Mycoplasma",
        "Myelitis": "Myelitis",
        "Myiasis": "Myiasis",
        "Myocarditis": "Myocarditis",
        "Naegleria": "Naegleria",
        "Gangrene": "Necrosis",
        "Infarction": "Necrosis",
        "Osteonecrosis": "Necrosis",
        "Necrosis": "Necrosis",
        "Nephritis": "Nephritis",
        "Neuritis": "Neuritis",
        "Neurocysticercosis": "Neurocysticercosis",
        "Nipah virus": "Nipah virus",
        "Nocardiosis": "Nocardiosis",

        "Nontuberculous Mycobacterial Pulmonary Infection": "Nontuberculous Mycobacterium",
        "Atypical Mycobacteria": "Nontuberculous Mycobacterium",
        "Atypical Mycobacterium Infection": "Nontuberculous Mycobacterium",
        "Atypical Mycobacterial Infection Non-Tuberculous Pneumonia": "Nontuberculous Mycobacterium",
        "Atypical Mycobacterial Infection of Lung": "Nontuberculous Mycobacterium",
        "Nontuberculous Mycobacterial Lung Disease": "Nontuberculous Mycobacterium",
        "Nontuberculous mycobacteria": "Nontuberculous Mycobacterium",
        "Non-Tuberculous Mycobacteria": "Nontuberculous Mycobacterium",
        "Mycolicibacterium obuense": "Nontuberculous Mycobacterium",
        "Mycolicibacterium gilvum": "Nontuberculous Mycobacterium",
        "Mycolicibacterium flavescens": "Nontuberculous Mycobacterium",
        "Mycolicibacterium duvalii": "Nontuberculous Mycobacterium",
        "Mycolicibacter terrae": "Nontuberculous Mycobacterium",
        "Mycobacterium, Atypical": "Nontuberculous Mycobacterium",
        "Mycobacterium terrae": "Nontuberculous Mycobacterium",
        "Mycobacterium szulgai": "Nontuberculous Mycobacterium",
        "Mycobacterium obuense": "Nontuberculous Mycobacterium",
        "Mycobacterium gordonae": "Nontuberculous Mycobacterium",
        "Mycobacterium gilvum": "Nontuberculous Mycobacterium",
        "Mycobacterium flavescens": "Nontuberculous Mycobacterium",
        "Mycobacterium duvalii": "Nontuberculous Mycobacterium",
        "Nontuberculous Mycobacterium": "Nontuberculous Mycobacterium",

        "Nontuberculous Mycobacterium Infection": "Nontuberculous mycobacteria",
        "Nontuberculous Mycobacterium": "Nontuberculous mycobacteria",
        "Salmonella Infection Non-Typhoid": "Non-Typhoidal salmonellosis",
        "Norovirus Infections": "Norovirus",
        "North Asian tick-borne rickettsiosis": "North Asian tick-borne rickettsiosis",
        "Norwalk Virus Infection": "Norwalk virus",
        "Oligella ": "Oligella ",
        "Omsk": "Omsk haemorrhagic fever ",
        "Onchocerciasis": "Onchocerciasis",
        "Onychomycosis": "Onychomycosis",
        "O'nyong'nyong": "O'nyong'nyong",
        "Opisthorchis": "Opisthorchiasis",
        "Orchitis": "Orchitis",
        "Orf": "Orf",
        "Oropouche": "Oropouche Fever",
        "Oroya": "Oroya fever",
        "Pott puffy tumor": "Osteomyelitis",
        "Petrositis": "Osteomyelitis",
        "Mastoiditis": "Osteomyelitis",
        '"Osteomyelitis AND ""infection"" (in other terms)"': "Osteomyelitis",
        '"Otitis AND ""infection"" (in other terms)"': "Otitis",
        '"Otitis externa AND ""infection"" (in other terms)"': "Otitis externa",
        '"Otitis media AND ""infection"" (in other terms)"': "Otitis media",
        "Paracoccidioidomycosis": "Paracoccidioidomycosis",
        "Paragonimiasis": "Paragonimiasis",
        "Parametritis": "Parametritis",
        "Parvovirus B19 Infection": "Parvovirus",
        "Pasteurella ": "Pasteurella ",
        "Pelvic inflammatory disease": "Pelvic inflammatory disease",
        "Peptococcus ": "Peptococcus ",
        "Peptostreptococcus ": "Peptostreptococcus ",
        '"Pericarditis AND ""infection"" (in other terms)"': "Pericarditis",
        '"Periodontitis AND ""infection"" (in other terms)"': "Periodontitis",
        "Peritonitis Infectious": "Peritonitis",
        "Pertussis": "Pertussis",
        "Phaeohyphomycosis": "Phaeohyphomycosis",
        '"Pharyngitis AND ""infection"" (in other terms)"': "Pharyngitis",
        '"Phlebitis AND ""infection"" (in other terms)"': "Phlebitis",
        "Phycomycosis": "Phycomycosis",
        "Pinta": "Pinta",
        "Plague": "Plague",
        "Pneumococcal infections": "Pneumococcal infection",
        "Pneumocystis jiroveci pneumonia": "Pneumocystis jiroveci pneumonia (PCP)",
        '"Pneumonia AND ""infection"" (in other terms)"': "Pneumonia",
        "Poliomyelitis": "Poliomyelitis",
        "Porphyromonas asaccharolytica": "Porphyromonas asaccharolytica",
        "Post-Operative Wound Infection": "Post-operative wound/surgical site infections",
        "Powassan": "Powassan virus ",
        "Prevotella": "Prevotella",
        "Primary amoebic meningoencephalitis": "Primary amoebic meningoencephalitis",
        '"Prostatitis AND ""infection"" (in other terms)"': "Prostatitis",
        "Proteus Infections": "Proteus",
        "Protothecosis": "Protothecosis",
        "Providencia ": "Providencia ",
        "Pseudomonas Infection": "Pseudomonas",
        "Psittacosis": "Psittacosis",
        '"Pyelitis AND ""infection"" (in other terms)"': "Pyelitis",
        '"Pyelonephritis AND ""infection"" (in other terms)"': "Pyelonephritis",
        "Pyoderma": "Pyoderma",
        "Q fever": "Q fever",
        "Queensland tick typhus": "Queensland tick typhus",
        "Rabies": "Rabies",
        "Rash of unknown origin": "Rash of unknown origin",
        "Rat-bite fever": "Rat-bite fever",
        '"Relapsing Fever': ' Tick-Borne",Relapsing fever',
        "Respiratory tract infections": "Respiratory tract infections",
        '"Retinitis AND ""infection"" (in other terms)"': "Retinitis",
        "Rheumatic fever": "Rheumatic fever",
        '"Rhinitis AND ""infection"" (in other terms)"': "Rhinitis",
        "Rickettsia parkeri": "Rickettsia parkeri",
        "Rickettsialpox": "Rickettsialpox",
        "Rift Valley fever": "Rift Valley fever",
        "Rocky Mountain spotted fever": "Rocky Mountain spotted fever",
        "Roseola infantum": "Roseola infantum",
        "Ross River virus ": "Ross River virus ",
        "Rotavirus": "Rotavirus",
        "Rubella": "Rubella",
        "Sandfly fever": "Sandfly fever",
        "Sarcocystis ": "Sarcocystis ",
        "Scabies": "Scabies",
        "Scarlet fever": "Scarlet fever",
        "Scedosporium Infection": "Scedosporium",
        "Schistosomiasis": "Schistosomiasis",
        "Scrub typhus": "Scrub typhus",
        "Semliki Forest virus ": "Semliki Forest virus ",
        '"Sepsis AND ""infection"" (in other terms)"': "Sepsis",
        "Septic Arthritis": "Septic Arthritis",
        "Septicemia": "Septicemia",
        "Serratia ": "Serratia ",
        "Severe acute respiratory syndrome (SARS)": "Severe acute respiratory syndrome (SARS)",
        "Shigellosis": "Shigellosis",
        "Sindbis ": "Sindbis ",
        '"Sinusitis AND ""infection"" (in other terms)"': "Sinusitis",
        "Skin and Subcutaneous Tissue Infection": "Skin and skin structure infections",
        "Smallpox": "Smallpox",
        "Sporotrichosis": "Sporotrichosis",
        "St. Louis encephalitis": "St. Louis encephalitis",
        "Staphylococcal Infections": "Staphylococcal infection",
        "Streptococcal infections": "Streptococcal infection",
        "Strongyloidiasis": "Strongyloidiasis",
        "Syphilis infection": "Syphilis",
        "Taeniasis Solium": "Taeniasis",
        "Talaromyces (formerly Penicillium) marneffei": "Talaromyces (formerly Penicillium) marneffei",
        "Tetanus": "Tetanus",
        "Tick bite fever": "Tick bite fever",
        "Tick-Borne Viral Encephalitis": "Tick-borne viral encephalitis",
        "Tinea": "Tinea",
        '"Tonsillitis AND ""infection"" (in other terms)"': "Tonsillitis",
        "Toxocariasis": "Toxocariasis",
        "Toxoplasmosis": "Toxoplasmosis",
        "Trachoma": "Trachoma",
        "Traveler's diarrhea": "Traveler's diarrhea",
        "Trench fever": "Trench fever",
        "Trichinosis": "Trichinosis",
        "Trichomoniasis": "Trichomoniasis",
        "Trichophyton": "Trichophyton",
        "Trichosporon ": "Trichosporon ",
        "Trichuriasis": "Trichuriasis",
        "Tuberculosis, Multidrug-Resistant": "Tuberculosis",
        "Tularemia": "Tularemia",
        "Tungiasis": "Tungiasis",
        "Typhlitis": "Typhlitis",
        "Typhoid fever": "Typhoid fever",
        "Typhus Fever Due to Rickettsia Tsutsugamushi": "Typhus fever",
        "Ureaplasma Infections": "Ureaplasma ",
        '"Urethritis AND ""infection"" (in other terms)"': "Urethritis",
        "Urinary tract infections": "Urinary tract infections",
        '"Uveitis AND ""infection"" (in other terms)"': "Uveitis",
        '"Vaccinia AND ""infection"" (in other terms)"': "Vaccinia",
        '"Vaginitis AND ""infection"" (in other terms)"': "Vaginitis",
        '"Vasculitis AND ""infection"" (in other terms)"': "Vasculitis",
        "Venezuelan equine encephalitis": "Venezuelan equine encephalitis",
        "Ventriculitis": "Ventriculitis",
        "Verruga peruana": "Verruga peruana",
        "Vibrio vulnificus ": "Vibrio vulnificus ",
        "Vincent's angina": "Vincent's angina",
        "West Nile Virus Infection": "West Nile ",
        "Western equine encephalitis": "Western equine encephalitis",
        "Whipples disease": "Whipples disease",
        "Wound infection": "Wound infections",
        "Yaws": "Yaws",
        "Yellow fever": "Yellow fever",
        "Yersiniosis": "Yersiniosis",
        "Zika virus infection": "Zika",
    }

    cure_diseases = Disease.objects.values("name", "id")
    diseases = {i["name"].lower(): i["id"] for i in cure_diseases}

    for ctgov_name in ct_to_cure:
        name = ct_to_cure[ctgov_name].lower()
        key = ctgov_name.lower()
        if key not in diseases and name in diseases:
            diseases[key] = diseases[name]

    diseases.pop("", None)

    # On 01.20.2022 Heather asked not to include Cancer
    # and End Stage Renal Disease CTs
    diseases.pop("cancer", None)
    diseases.pop("end stage renal disease", None)

    return diseases


def _comb_clinicaltrial(ct):
    """Change names from ones used in CTgov to the ones used in CURE"""
    new_ct = {
        "clinical_trials_gov_id": ct["NCTId"][0],
        "title": ct["BriefTitle"][0] if ct["BriefTitle"] else "",
        "disease": ct["Condition"],
        "sponsor": ct["LeadSponsorName"][0] if ct["LeadSponsorName"] else "",
        "status": ct["OverallStatus"][0] if ct["OverallStatus"] else "",
        "phase": "".join(ct["Phase"]),
        "participants": ct["EnrollmentCount"][0] if ct["EnrollmentCount"] else "",
        "start_year": ct["StartDate"][0][-4:] if ct["StartDate"] else "",
        "country": ct["LocationCountry"][0] if ct["LocationCountry"] else "",
    }

    zipped = [
        f"{i[0]}: {i[1]}"
        for i in list(zip(ct["InterventionType"], ct["InterventionName"]))
    ]
    new_ct["drugs_string"] = ",".join(zipped)
    return new_ct


def _identify_disease(ct, diseases):
    """Sometimes ct['condition'] be anything but disease name (but still contain the disease name),
    or ct['title']. Will identify those cases, as long as the mentioned disease name is not
    used together with not|no|without|excluded|excluding.

    Returns: [Disease_name found, Where matched]
            [None, None] if not found
    """

    EXCLUDES = ["no", "not", "without", "excluded", "excluding"]

    for condition in ct["disease"]:
        # Cure has a disease with this name or it's found in ctgov_to_cure
        condition = condition.lower().strip()
        if condition in diseases:
            return [condition, f"CURE: {ct['disease']}"]

        possible_diseases = list(
            filter(lambda x: re.search(r"\b{}\b".format(x), condition), diseases.keys())
        )
        if len(possible_diseases) > 0:
            for disease in possible_diseases:
                pattern = f'{"|".join(EXCLUDES)} {disease}'
                if re.search(pattern, condition):
                    continue
                return [disease, f"CONDITIONS: {ct['disease']}"]

    # The same for ct['title']
    title_lowercased = ct["title"].lower().strip()
    possible_diseases = list(
        filter(
            lambda x: re.search(r"\b{}\b".format(x), title_lowercased), diseases.keys()
        )
    )
    if len(possible_diseases) > 0:
        for disease in possible_diseases:
            pattern = f'{"|".join(EXCLUDES)} {disease}'
            if re.search(pattern, title_lowercased):
                continue

            return [disease, f"TITLE: {ct['title']}"]

    return [None, None]
