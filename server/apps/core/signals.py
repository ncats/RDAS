import email
from django.contrib.contenttypes.models import ContentType

from .constants import APPROVED
from server.apps.notifications.notifications import *

def create_newsfeed(sender, instance=None, created=False, **kwargs):
    from .models import CureReport, Discussion, ClinicalTrial, Article, Event, Comment
    from server.apps.api.v2.serializers.comment import CommentWSSerializer

    if instance is None:
        return

    action = 'created' if created else 'updated'
    if isinstance(instance, CureReport):
        deleted = instance.flagged or instance.status != APPROVED
    elif isinstance(instance, ClinicalTrial):
        deleted = instance.deleted or instance.status != APPROVED
    elif isinstance(instance, Discussion):
        deleted = instance.deleted or instance.flagged or instance.status != APPROVED
    elif isinstance(instance, Comment):
        deleted = instance.deleted or instance.flagged
        action = 'commented' if created else 'updated'

        serializer = CommentWSSerializer(instance)
        room_name = f"{instance.content_type.model}_{instance.object_id}"
        if not deleted:
            websocket_comment_message(serializer.data, action, room_name)
    elif isinstance(instance, Article) or isinstance(instance, Event):
        deleted = instance.status != APPROVED
    else:
        return

    __process_info(instance=instance, action=action, deleted=deleted)


def __process_info(instance, action='created', deleted=False):
    from .models import Newsfeed

    content_type = ContentType.objects.get_for_model(instance)
    newsfeed = Newsfeed.objects.filter(content_type=content_type, object_id=instance.id).first()

    old_id = instance.id

    if not deleted and not newsfeed:
        newsfeed = Newsfeed.objects.create(
                action='created',
                content_type=content_type,
                object_id=instance.id,
                pinned=False
            )
    elif deleted and newsfeed:
        newsfeed.delete()
    else:
        return

    websocket_newsfeed_message(newsfeed, old_id=old_id)


def websocket_newsfeed_message(instance, **kwargs):
    from asgiref.sync import async_to_sync
    from server.apps.api.v2.serializers.newsfeed import NewsfeedWSSerializer
    import channels.layers

    model = instance.content_type.model
    if model == 'curereport':
        model = 'report'
    room_name = f"newsfeed-{model}"

    if instance.id is None:
        data = {"message": "Newsfeed deleted", "id": kwargs["old_id"]}
    else:
        serializer = NewsfeedWSSerializer(instance)
        data = serializer.data

    channel_layer = channels.layers.get_channel_layer()
    async_to_sync(channel_layer.group_send)(room_name, {
        "type": "newsfeed_message",
        "message": data,
    })


def websocket_comment_message(data, action, room_name):
    from server.apps.api.v2.serializers.comment import CommentSerializer
    from asgiref.sync import async_to_sync
    import channels.layers

    channel_layer = channels.layers.get_channel_layer()
    async_to_sync(channel_layer.group_send)(room_name, {
        "type": "comment_message",
        "message": data,
    })


def push_notifications(sender, instance=None, created=False, **kwargs):
    from .models import CureReport, Discussion, ClinicalTrial, Article, Event, Comment
    data = {}
    if not created:
        if isinstance(instance, CureReport):
            displayable = instance.flagged or (instance.status == APPROVED and instance.__status != APPROVED)
            if not displayable:
                return
            drugnames = instance.report.regimens.all().values_list('drug__name', flat=True)
            drugs = ', '.join(drugnames)
            data = {
                "title": f"A report for {instance.report.disease.name}",
                "body": f"Treated with {drugs}, outcome {instance.report.outcome}\n\nSee details",
                "link": f"explore/cases/case-details/{instance.id}",
                "obj_type": "Report",
                "disease_id": instance.report.disease.id,
                "author": instance.author.id,
            }
            send_push_notification.delay(data)

        if isinstance(instance, Discussion):
            displayable = instance.deleted or instance.flagged or (instance.status == APPROVED and instance.__status != APPROVED)
            if not displayable:
                return

            data={
                "title": f"A discussion for {instance.disease.name}",
                "body": f"{instance.title}\n\nSee details",
                "link": f"explore/discussions/discussion-details/{instance.id}",
                "obj_type": "Discussion",
                "disease_id": instance.disease.id,
                "author": instance.author.id,
            }
            send_push_notification.delay(data)

        # if isinstance(instance, Comment):
        #     data={
        #         "title": "",
        #         "body": f"{instance.body}"

        #     }
        if isinstance(instance, ClinicalTrial):
            displayable = instance.deleted or (instance.status == APPROVED and instance.__status != APPROVED)
            if not displayable:
                return
            data={
                "title": f"A new clinical trial",
                "body": f"{instance.title}\n\nSee details",
                "link": f"explore/clinical-trials/clinical-trial-details/{instance.id}",
                "obj_type": "Clinical Trial",
                "author": instance.author.id,
            }
            send_push_notification.delay(data)

        if isinstance(instance, Article):
            displayable = instance.status == APPROVED and instance.__status != APPROVED
            if not displayable:
                return
            data={
                "title": f"A {instance.publication_type} article for {instance.disease.name}",
                "body": f"{instance.title}\n\nSee details",
                "link": f"explore/articles/article-details/{instance.id}",
                "obj_type": "Article",
                "publication_type": instance.publication_type,
                "disease_id": instance.disease.id,
                "author": instance.author.id,
            }
            send_push_notification.delay(data)

        if isinstance(instance, Event):
            displayable = instance.status == APPROVED and instance.__status != APPROVED
            if not displayable:
                return
            data={
                "title": f"A event for {instance.disease.name}",
                "body": f"{instance.title}\n\nSee details",
                "link": f"explore/events/event-details/{instance.id}",
                "obj_type": "Event",
                "disease_id": instance.disease.id,
                "author": instance.author.id,
            }
            send_push_notification.delay(data)
    #add comments


def send_notification_for_disease_oi(sender, instance=None, created=False, **kwargs):
    from server.apps.notifications.notifications import send_disease_oi_notification
    from .models import CureReport, Report, Discussion, ClinicalTrial, Article, Event, Comment

    # TODO: used to send notifications on content creation (<and created>)
    #   now we send it when status == APPROVED. The problem is when the approved
    #   content gets saved several times.
    if isinstance(instance, CureReport) and instance.status == APPROVED:
        email_list = instance.report.disease.profiles.filter(
            notifications__notification_case_favor = True
        ).values_list("user_id", flat=True)
        send_disease_oi_notification.delay(list(email_list), "Report", instance.id)
    #When saving CT no disease is sent can't send notification for disease interest
    # if isinstance(instance,ClinicalTrial):
    #     if instance and created:
    #         print('heere inct')
    #         email_list = instance.disease.profiles.filter(
    #             notifications__notification_clinical_trial_favor = True
    #         ).values_list("user_id", flat=True)
            # send_disease_oi_notification(email_list, ClinicalTrial, instance.id)
    if isinstance(instance, Discussion) and instance.status == APPROVED:
        email_list = instance.disease.profiles.filter(
            notifications__notification_post_favor = True
        ).values_list("user_id", flat=True)
        send_disease_oi_notification.delay(list(email_list), "Discussion", instance.id)
    if isinstance(instance, Article) and instance.status == APPROVED:
        if instance.publication_type == 'news':
            email_list = instance.disease.profiles.filter(
                notifications__notification_news_favor = True
            ).values_list("user_id", flat=True)
        if instance.publication_type == 'journal':
            email_list = instance.disease.profiles.filter(
                notifications__notification_journal_favor = True
            ).values_list("user_id", flat=True)
        send_disease_oi_notification.delay(list(email_list), "Article", instance.id)
    if isinstance(instance, Event) and instance.status == APPROVED:
        email_list = instance.disease.profiles.filter(
            notifications__notification_event_favor = True
        ).values_list("user_id", flat=True)
        send_disease_oi_notification.delay(list(email_list), "Event", instance.id)
    #add comments


def comment_on_object(sender, instance=None, created=False, **kwargs):
    from server.apps.core.models import CureReport, Report

    send_notification = False
    if instance and created:
        object = instance.content_type.get_object_for_this_type(id=instance.object_id)
        if isinstance(object, Report):
            send_notification = CureReport.objects.filter(id=instance.object_id).values('author__profile__notifications').first().get('author__profile__notifications').get('notification_comment_all')
        else:
            send_notification = object.author.profile.notifications.get('notification_comment_all')

        if instance.parent:
            if isinstance(object, Report):
                send_notification = CureReport.objects.filter(id=instance.object_id).values('author__profile__notifications').first().get('author__profile__notifications').get('notification_comment_reply')
            else:
                send_notification = object.author.profile.notifications.get('notification_comment_reply')

        if send_notification:
            send_comment_notification.delay(instance, object)

