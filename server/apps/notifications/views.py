from fcm_django.models import FCMDevice
from rest_framework.authtoken.models import Token
from django.conf import settings
from django.http import JsonResponse
from rest_framework.status import HTTP_201_CREATED, HTTP_200_OK, HTTP_400_BAD_REQUEST
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import api_view, permission_classes
from server.apps.notifications.notifications import clear_notifications_task
from server.apps.core.models import UnseenNews
from django.contrib.auth.models import User
from django.shortcuts import redirect


@api_view(["POST"])
@permission_classes((IsAuthenticated,))
def fcm_token(request):
    data = request.data.copy()
    try:
        token = Token.objects.get(key=data["token"])
        reg_id = data["registration_id"]
        FCMDevice.objects.filter(user_id=token.user_id).delete()
        FCMDevice.objects.get_or_create(
            registration_id=reg_id,
            user_id=token.user_id
        )
    except:
        return JsonResponse(
            status=HTTP_400_BAD_REQUEST,
            data={"error": "Provided data is not valid."}
        )
    return JsonResponse(status=HTTP_200_OK, data={'result': 'Done'})


@api_view(['GET'])
@permission_classes((IsAuthenticated,))
def unseen_notifications(request):
    user_id = request.user.id
    unseennews = UnseenNews.objects.filter(user_id=user_id).count()
    return JsonResponse(status=HTTP_200_OK, data={'unseen_notifications': unseennews})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def clear_notifications(request):
    user_id = request.user.id
    page = request.data.get("page", "")
    # No need to check object_id for it being an int,
    # if it's not in clear_notifications_task nothing bad will happen
    object_id = request.data.get("id", "")
    path = ""

    if page == "newsfeed":
        path = "v2/newsfeed"
    elif page == "discussion" and object_id:
        path = f"v2/discussions/{object_id}"
    elif page == "report" and object_id:
        path = f"v2/reports/{object_id}"
    elif page == "articles" and object_id:
        path = f"v2/articles/{object_id}"
    elif page == "events" and object_id:
        path = f"v2/events/{object_id}"
    elif page == "clinical-trials" and object_id:
        path = f"v2/clinical-trials/{object_id}"

    if path:
        clear_notifications_task(user_id, path)
        unseen = UnseenNews.objects.filter(user_id=user_id).count()
        return JsonResponse(status=HTTP_200_OK, data={"unseen_notifications": unseen})

    return JsonResponse(status=HTTP_400_BAD_REQUEST, data={})


@api_view(["GET"])
def daily_digest_unsubscribe(request):
    salt = request.GET.get("salt", None)
    email = request.GET.get("email", None)

    if salt and email:
        try:
            id_ = int(salt)
        except:
            id_ = 27
        user_id = (id_ - 27) / 2
        user = User.objects.filter(id=user_id).first()
        print(user)
        email_digest_fields = [
            "notification_case_all",
            "notification_case_favor",
            "notification_post_all",
            "notification_post_favor",
            "notification_comment_all",
            "notification_comment_favor",
            "notification_comment_reply",
            "notification_news_all",
            "notification_news_favor",
            "notification_journal_all",
            "notification_journal_favor",
            "notification_event_all",
            "notification_event_favor"
        ]
        if user:
            for field in email_digest_fields:
                user.profile.notifications[field]=False
                user.profile.save()

    redirect_to = f"https://{settings.FRONT_END_SUB_DOMAIN}{settings.FRONT_END_DOMAIN}#/newsfeed"
    return redirect(redirect_to)