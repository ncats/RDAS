from tokenize import Triple
from django import forms
from django.conf import settings
from django.contrib.auth import login
from django.contrib.auth.models import User
from django.shortcuts import redirect
from django.template.response import TemplateResponse

import pyrebase
from requests.exceptions import HTTPError
from rest_framework.authtoken.models import Token
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.status import HTTP_400_BAD_REQUEST, HTTP_200_OK, HTTP_405_METHOD_NOT_ALLOWED, HTTP_201_CREATED

from server.apps.api.v2.serializers.linked_account import LinkedAccountSerializer
from server.apps.core.models import LinkedAccount, Profile, CureReport, Comment, Discussion

firebase = pyrebase.initialize_app(settings.FIREBASE_CONFIG)
auth = firebase.auth()


class DjangoAdminLoginForm(forms.Form):
    username = forms.EmailField(label='Email address', required=True)
    password = forms.CharField(widget=forms.PasswordInput)

@api_view(['POST', 'GET'])
def admin_login(request):
    # Either a request coming from the frontend with [uid, providerid]
    #   or from Django admin page with [username, password]
    form = DjangoAdminLoginForm()
    if request.method == 'POST':
        uid = request.data.get('uid')
        provider = request.data.get('provider')
        token = request.data.get('token')

        # The situation when the frontend only sent us [uid] is not expected
        #   the below can only check one of the parameters
        if uid and provider and token:
            if not valid_firebase_login(uid, provider, token):
                return Response(
                    { "message": "Couldn't log you in. Try again." },
                    status=HTTP_400_BAD_REQUEST,
                )
            return return_user_data(uid, provider)
        else:
            form = DjangoAdminLoginForm(request.POST)
            if form.is_valid():
                try:
                    email = form.cleaned_data['username']
                    password = form.cleaned_data['password']
                    signin = auth.sign_in_with_email_and_password(email, password)
                    # TODO: should probably check whether the email is confirmed
                    #   and the account is not disabled in firebase
                    user = User.objects.get(username=signin['localId'], is_active=True)
                    login(request, user)
                    return redirect(request.GET.get('next', '/admin'))
                except (HTTPError, User.DoesNotExist):
                    form.add_error(None, "The email and password you entered didn't match our records. Please try again.")

    django_admin_login_template = TemplateResponse(
        request,
        "admin/v2_login.html",
        context={
            'form': form,
            'site_title': 'CURE ID'
        }
    )
    return django_admin_login_template.render()


def valid_firebase_login(uid, provider, id_token):
    try:
        account = auth.get_account_info(id_token)
        #{
        #  'kind': 'identitytoolkit#GetAccountInfoResponse',
        #  'users': [
        #           {
        #               'localId': 'EeJdROBx5vWTpKABqhdhOTvvtJL2',
        #               'email': 'testing@backend.com',
        #               'passwordHash': 'UkVEQUNURUQ=',
        #               'emailVerified': False,
        #               'passwordUpdatedAt': 1644332542573,
        #               'providerUserInfo': [
        #                   {
        #                       'providerId': 'password',
        #                       'federatedId': 'testing@backend.com',
        #                       'email': 'testing@backend.com',
        #                       'rawId': 'testing@backend.com'
        #                   }
        #               ],
        #               'validSince': '1644332542',
        #               'disabled': False,
        #               'lastLoginAt': '1644346518976',
        #               'createdAt': '1644332542573',
        #               'lastRefreshAt': '2022-02-08T18:55:18.976Z'
        #           }
        #   ]
        #}
        acc_uid, acc_provider = "", False
        if account['users'] and type(account['users']) == list:
            data = {}
            for user_data in account['users']:
                acc_uid = user_data.get('localId')
                provider_info = user_data.get('providerUserInfo')
                if provider_info and type(provider_info) == list:
                    for pinfo in provider_info:
                        if pinfo.get('providerId') == provider:
                            acc_provider = True
                            break
                data = user_data

            # TODO: should i add some sort of message?
            if (provider == 'password' and not data.get('emailVerified', True)) or data.get('disabled', False):
                return False

        return acc_uid == uid and acc_provider
    except (HTTPError, Exception) as e:
        return False


def return_user_data(uid, provider):
    """ Will create User, Profile, LinkedAccount, Token objects if necessary. """
    try:
        linked_accs = LinkedAccount.objects.filter(uid=uid)
        if not linked_accs:
            user = User.objects.create(username=uid)
            linked_accs = [ LinkedAccount.objects.create(uid=uid, user=user, provider=provider), ]
        else:
            user = User.objects.get(pk=linked_accs[0].user_id)

        linked_accs = LinkedAccountSerializer(linked_accs, many=True).data
        notification_defaults = {
            "notification_case_all": True,
            "notification_case_all_push": True,
            "notification_case_favor": False,
            "notification_case_favor_push": False,
            "notification_post_all": True,
            "notification_post_all_push": True,
            "notification_post_favor": False,
            "notification_post_favor_push": False,
            "notification_comment_all": True,
            "notification_comment_all_push": True,
            "notification_comment_favor": False,
            "notification_comment_favor_push": False,
            "notification_comment_reply": True,
            "notification_comment_reply_push": True,
            "notification_clinical_trial_all": True,
            "notification_clinical_trial_all_push": True,
            "notification_clinical_trial_favor": False,
            "notification_clinical_trial_favor_push": False,
            "notification_news_all": True,
            "notification_news_all_push": True,
            "notification_news_favor": False,
            "notification_news_favor_push": False,
            "notification_journal_all": True,
            "notification_journal_all_push": True,
            "notification_journal_favor": False,
            "notification_journal_favor_push": False,
            "notification_event_all": True,
            "notification_event_all_push": True,
            "notification_event_favor": False,
            "notification_event_favor_push": False,
            "email_period": 'daily',
            "notifications_do_not_disturb": False,
            # "quiet_time": False,
            # "quiet_time_end": "02:00",
            # "quiet_time_start": "11:00",
        }
        profile, _ = Profile.objects.get_or_create(user=user, defaults={"notifications": notification_defaults})
        token, _ = Token.objects.get_or_create(user=user)
    except Exception as e:
        return Response(
            { "message": "Couldn't log you in. Try again." },
            status=HTTP_400_BAD_REQUEST,
        )

    # TODO: probably need to login(username=user.username) here too
    return Response(
        {
            "data":{
                "token": token.key,
                "user_id": user.id,
                # TODO: a fool profile object would be more useful here
                "profile_id": profile.id,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                # TODO: no idea why would front end need this info.
                "linked_accounts": linked_accs
            }
        },
        status=HTTP_200_OK
    )


@api_view(['POST'])
def register(request):
    provider = request.data.get('provider')
    uid = request.data.pop('uid')
    token = request.data.pop('token')

    if uid or token:
        is_valid = valid_firebase_login(uid, provider, token)
        if is_valid:
            new_user = {
                "username": uid,
                "is_active": True,
            }
            # TODO: return_user_data might work here
            user = User.objects.create(**new_user)
            Profile.objects.create(user=user)
            token, created = Token.objects.get_or_create(user=user)
            return Response(
                {
                    'data': {
                        'token': token.key,
                        'user_id': user.id,
                        "username": user.username,
                    },
                    'message': 'Successful registration.'
                },
                status=HTTP_201_CREATED
            )
        else:
            return Response(
                { "message": "Couldn't log you in. Try again." },
                status=HTTP_400_BAD_REQUEST,
            )
    else:
        return Response(
            { "message": "All required fields not found in the request." },
            status=HTTP_400_BAD_REQUEST,
        )


@api_view(["POST"])
def link_accounts(request):
    # the account to link to
    user_id = request.data.get('id')
    # the account to be linked
    uid = request.data.pop('uid')
    provider = request.data.pop('provider')
    token = request.data.pop('token')

    if not uid or not user_id or not provider or not token:
        return Response(
            { "message": "All required fields not found in the request." },
            status=HTTP_400_BAD_REQUEST,
        )

    # User making the request can't link to other accounts
    # TODO: do I really need user_id then?
    # Check that the uid, protocol, token are valid
    if request.user.id != user_id or not valid_firebase_login(uid, provider, token):
        return Response(
            { "message": "Operation is not permitted." },
            status=HTTP_405_METHOD_NOT_ALLOWED
        )

    # TODO: What if uid,provider is already linked to another account? 
    # TODO: no try-excepts
    # TODO: Frontend probably should show message saying "All content from linked
    #   account will be moved (except Profile settings) and Profile deleted"
    switch_author(uid, provider, request.user.id)
    linked_accs = LinkedAccount.objects.filter(user_id=user_id).values()
    user = request.user
    return Response(
        {
            "data": {
                "user_id": user.id,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "linked_accounts": linked_accs,
            },
            "detail": "Account linked",
        },
        status=HTTP_200_OK
    )


def switch_author(uid, provider, to_user_id):
    link_acc, created = LinkedAccount.objects.get_or_create(
        uid=uid,
        provider=provider,
        user_id=to_user_id
    )
    if created:
        return

    current_user_id = link_acc.user_id
    # TODO: Notifications should *NOT* be sent 
    # TODO: How to make sure I add new models here (that have author field)?
    CureReport.objects.filter(author_id=current_user_id).update(author_id=to_user_id)
    # TODO: for Comment and Discussion the string representation of author might
    #   change depending on first_name and last_name values
    Comment.objects.filter(author_id=current_user_id).update(author_id=to_user_id)
    Discussion.objects.filter(author_id=current_user_id).update(author_id=to_user_id)
    # TODO: not moving any profile settings from this one to the new one?
    # TODO: really delete or just set status=BANNED
    #Profile.objects.get(user_id=current_user_id).delete()
    # TODO: should I delete it?
    #User.objects.filter(id=current_user_id).update(is_active=False)


@api_view(["POST"])
def unlink_accounts(request):
    # TODO: Why would anybody want to do that?
    # since I move all the content to the linked account, and delete Profile and User
    # I don't understand what I can do here

    # the account to be linked
    uid = request.data.pop('uid')
    provider = request.data.pop('provider')
    if not uid or not provider:
        return Response(
            { "message": "All required fields not found in the request." },
            status=HTTP_400_BAD_REQUEST,
        )

    LinkedAccount.objects.filter(uid=uid, provider=provider, user=request.user).delete()

    user = request.user
    linked_accs = LinkedAccount.objects.filter(user_id=user.id).values()
    return Response(
        {
            "data": {
                "user_id": user.id,
                "username": user.username,
                "first_name": user.first_name,
                "last_name": user.last_name,
                "linked_accounts": linked_accs,
            },
            "detail": "Account unlinked",
        },
        status=HTTP_200_OK
    )


@api_view(["POST"])
def logout(request):
    # TODO: check that it really logs out. Dominic's code had
    #   request.user.auth_token.delete()
    django_logout(request)
    return Response({ "data": {}, }, status=HTTP_200_OK)


# @api_view(["POST"])
# @permission_classes((IsAuthenticated,))
# def fcm_token(request):
#     data = request.data.copy()
#     try:
#         token = Token.objects.get(key=data["token"])
#         reg_id = data["registration_id"]
#         FCMDevice.objects.filter(user_id=token.user_id).delete()
#         FCMDevice.objects.get_or_create(
#             registration_id=reg_id,
#             user_id=token.user_id
#         )
#     except:
#         return JsonResponse(
#             status=HTTP_400_BAD_REQUEST,
#             data={"error": "Provided data is not valid."}
#         )
#     return JsonResponse(status=HTTP_200_OK, data={'result': 'Done'})
