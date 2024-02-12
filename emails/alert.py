import boto3
from botocore.exceptions import ClientError

sender_email = 'ncatsrdas@mail.nih.gov'

def setup_email_client():
    client = boto3.client(
        service_name='ses',
        region_name='us-east-1'
    )
    return client

def send_email(sub,msg,recip,html=None,client=setup_email_client()):
    if not client:
        return
    
    response = client.send_email(
        Source=sender_email,
        Destination={
            'ToAddresses': [
                f'{recip}',
            ],
        },
        Message={
            'Subject': {
                'Data': f'{sub}',
            },
            'Body': {
                'Text': {
                    'Data': f'{msg}',
                },
                'Html': {
                    'Data': f'{html}'
                },
            }
        }
    )

def send_raw_email(sub,msg,recip,client=None):
    if not client:
        return

    client = session.client(
        service_name='ses',
        region_name='us-east-1'
    )
    
    response = client.send_raw_email(
        Source='ncatsrdas@mail.nih.gov',
        Destinations=[
            f'{recip}',
        ],
        RawMessage={
            'Data': f'To:{recip}\nFrom:ncatsrdas@mail.nih.gov\nSubject:{sub}\nMIME-Version: 1.0\nContent-type: Multipart/Mixed; boundary="NextPart"\n\n--NextPart\nContent-Type: text/plain\n\n{msg}\n\n'
        },
    )

