from django import forms
from django.contrib.admin.widgets import FilteredSelectMultiple
from server.apps.core.models import Profile
from django.contrib.auth.decorators import login_required
from rest_framework.decorators import api_view, permission_classes
from server.apps.notifications.notifications import send_email_task
from django.shortcuts import render
from django.conf import settings
from firebase_admin import initialize_app, auth
from django.db.models import Value as V
from django.db.models.functions import Concat   

class PromotionEmailForm(forms.Form):
    choices = [
        (i.id, i.user.get_full_name())
        for i in Profile.objects.filter(status="ACTIVE").annotate(full_name=Concat('user__first_name', V(' '), 'user__last_name')).order_by(
            "full_name"
        ).only('id')
    ]
    choices.insert(0, (-999, " --- All --- "))
    users = forms.MultipleChoiceField(
        label="Accounts to send the email to",
        choices=choices,
        widget=FilteredSelectMultiple(verbose_name="Accounts", is_stacked=False,),
    )
    title = forms.CharField(label="Email subject", max_length=255)
    body = forms.CharField(label="Email body", widget=forms.Textarea)


@api_view(["GET", "POST"])
@login_required(login_url="/admin")
def promotion_email(request):
    if request.method == "POST":
        form = PromotionEmailForm(request.POST)

        if form.is_valid():
            users = form.cleaned_data["users"]
            if "-999" in users:
                users = Profile.objects.filter(status="ACTIVE")
            else:
                users = Profile.objects.filter(id__in=users)

            for profile in users:
                firebase_user=auth.get_user(profile.user.username)
                email = firebase_user.email
                send_email_task.delay(
                    form.cleaned_data["title"],
                    form.cleaned_data["body"],
                    None,
                    "CURE ID <%s>" % settings.DEFAULT_FROM_EMAIL,
                    [email],
                    None,
                    None,
                    None,
                    True,
                )

            # # Only send to additional emails if we are sending emails to all
            # if "-999" in form.cleaned_data["users"]:
            #     for pair in additional_emails:
            #         values = pair[0].rsplit(" ", 1)
            #         if len(values) == 1:
            #             name = ""
            #             surname = values[0]
            #         else:
            #             name = values[0]
            #             surname = values[1]

            #         if (
            #             Profile.objects.filter(
            #                 Q(
            #                     user__first_name__iexact=name,
            #                     user__last_name__iexact=surname,
            #                 )
            #                 | Q(user__email__iexact=pair[1])
            #             ).count() == 0
            #         ):
            #             send_email_task.delay(
            #                 form.cleaned_data["title"],
            #                 form.cleaned_data["body"],
            #                 None,
            #                 "CURE ID <%s>" % settings.DEFAULT_FROM_EMAIL,
            #                 [pair[1],],
            #                 None,
            #                 None,
            #                 None,
            #                 True,
            #             )

            form = PromotionEmailForm()
    else:
        form = PromotionEmailForm()

    return render(
        request, "promotion_email_form.html", {"form": form, "title": "Promotion Email"}
    )