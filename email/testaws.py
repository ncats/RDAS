import boto3
from botocore.exceptions import ClientError

def send_raw_email(sub,msg.recip):
    client = session.client(
        service_name='ses',
        region_name='us-east-1'
    )
    ses_client = boto3.client("ses", region_name="us-east-1")
    CHARSET = "UTF-8"

    response = client.send_raw_email(
        Source='ncatsrdas@mail.nih.gov',
        Destinations=[
            f'{recip}',
        ],
        RawMessage={
            'Data': f'To:{recip}\nFrom:ncatsrdas@mail.nih.gov\nSubject:{sub}\nMIME-Version: 1.0\nContent-type: Multipart/Mixed; boundary="NextPart"\n\n--NextPart\nContent-Type: text/plain\n\n{msg}\n\n'
        },
    )

send_raw_email('Test Email', 'This is a test email with formatting', 'devon.leadman@axleinfo.com')
