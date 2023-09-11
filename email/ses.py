import django
from django.core.mail import send_mail
from django.template.loader import render_to_string
import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
django.setup()

#TEST SEND EMAIL

send_mail(
    'Test Email',
    'this is a test email',
    'ncatsrdas@mail.nih.gov',
    ['RECIPIENT_EMAIL']
)
