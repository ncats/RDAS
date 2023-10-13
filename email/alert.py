#import django
#from django.core.mail import send_mail
#from django.template.loader import render_to_string
import os
import boto3
from botocore.exceptions import ClientError
#os.environ['DJANGO_SETTINGS_MODULE'] = 'settings'
#django.setup()
'''
def clinical_msg (data): # dict
    msg_plain = render_to_string('templates/clinical_notif.txt', data)
    msg_html = render_to_string('templates/clinical_notif.html', data)

    send_mail(
        'Test Email',
        msg_plain,
        'from-email <notif@nih.gov>',
        ['RECIPIENT'],
        html_message=msg_html
    )

def pubmed_msg (data): # dict
    msg_plain = render_to_string('templates/pubmed_notif.txt', data)
    msg_html = render_to_string('templates/pubmed_notif.html', data)

    send_mail(
        'Test Email',
        msg_plain,
        'from-email <notif@nih.gov>',
        ['RECIPIENT'],
        html_message=msg_html
    )

def grant_msg (data): # dict
    msg_plain = render_to_string('templates/grant_notif.txt', data)
    msg_html = render_to_string('templates/grant_notif.html', data)

    send_mail(
        'Test Email',
        msg_plain,
        'from-email <notif@nih.gov>',
        ['RECIPIENT'],
        html_message=msg_html
    )
    
def test_msg (sub, msg, recip):
    send_mail(
        f'{sub}',
        f'{msg}',
        'ncatsrdas@mail.nih.gov',
        [f'{recip}']
    )
'''

def get_secret():

    secret_name = "/smtp/credentials"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']
    print(secret)

    # Your code goes here.

get_secret()
#test_msg('test','test','devon.leadman@nih.gov')
