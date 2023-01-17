"""
    Before: copy the template into PROD:~/code/server/apps/notifications/templates/<name>.html
            copy this script to PROD:/tmp

            !!! Update notifications/templates/monthly_newsletter_subject.txt with current month

    Run:    PYTHONSTARTUP=/tmp/script.py DJANGO_SETTINGS_MODULE=server.settings.prod ./manage.py shell

        On local add GOOGLE_APPLICATION_CREDENTIALS=/data/ncats-cure-id-firebase-adminsdk-2m98k-0eac32f489.json

            > main()

    In case SUBJECT is does not change in the test email kill the celery workers:
        # ps auxww | grep celery | grep -v "grep" | grep -v "beat" | awk '{print $2}' | xargs kill -HUP
        # su cureuser -c "COMMANDS FROM entrypoint.sh TO START CELERY IF NECESSARY"
"""

import firebase_admin
from firebase_admin import auth
from django.contrib.auth.models import User
from server.apps.core.models import *
from server.apps.notifications.notifications import send_email_task


def get_all_email_addresses(testrun: str) -> list[str]:
    """
        gets email addresses of the *ACTIVE (in CUREID)* users from Firebase.
    """
    addresses = []

    if testrun != 'sendall':
        addresses = [
            'shanmardev@hotmail.com',
        ]
        return addresses

    # This function usually requires page#, but we only have 600 users, 
    #   so all should be included in the only request
    users = auth.list_users()

    if users:
        users = users.iterate_all()
        for user in users:
            email = user.email
            uid = user.uid
            try:
                cureid_user = User.objects.get(username=uid, is_active=True)
                addresses.append(email)
            except User.DoesNotExist:
                pass

    return addresses

def set_the_task(addresses: list[str], template: str) -> None:
    # [
    #   subject_file_name: str     - filename inside notifications/templates/ folder. 
    #                                Create it and set to "August Newsletter from CURE ID"
    #
    #   text_body_file_name: str   - filename inside notifications/templates/ folder. Empty.
    #   
    #   {}                         - empty dict, used to supply values to template variables
    #
    #   'CURE ID <noreplyprojectcure@gmail.com>'        - the sender value
    #
    #   [email]                    - a list of emails to send the newsletter to, will have just one email per user
    #
    #   None
    #
    #   None
    #
    #   html_template: str         - filename inside notifications/templates folder
    # ]

    struct = [
        'monthly_newsletter_subject.txt', 'monthly_newsletter_body.txt', {},
        'CURE ID <noreplyprojectcure@gmail.com>', [], None, None, template
    ]

    for email in addresses:
        struct[4] = [email,]
        send_email_task.delay(*struct)


def main() -> None:
    updated = input(' Did you update the current month name in <notifications/templates/monthly_newsletter_subject.txt>? (yes/no) ')
    if updated != 'yes':
        print(' Canceling execution ... ')
        return

    template = input(' What is the base filename of this month\'s email template? ')

    testrun = input(' Type [sendall] to send email to all CUREID users, otherwise the email will be sent to test users only ')
    addresses = get_all_email_addresses(testrun)
    print(f' Found {len(addresses)} email addresses ..')
    print(' Emailing ..')
    set_the_task(addresses, template)


print("\n\n EXECUTE 'main()' TO SEND MONTHLY EMAILS \n\n")
