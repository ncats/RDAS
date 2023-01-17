
from rest_framework.decorators import api_view
from rest_framework.response import Response
from server.apps.core.models import AttachedImage
from server.apps.core.constants import *
from django.contrib.auth.decorators import login_required
from django.conf import settings
import boto3
from urllib.parse import urlparse, unquote
from botocore.exceptions import ClientError

PUBLIC_URL = "https://c-path-cureid-ncats-public.s3.us-west-2.amazonaws.com/"

@api_view(['POST'])
def approve_disapprove_image(request):
    try:
        id = request.POST.get('value').split('-')[1]
        decision = request.POST.get('value').split('-')[0]
        image = AttachedImage.objects.get(id=id)

        #check if the image is from cpath s3 bucket
        if 'c-path-cureid-ncats' in image.real_name:
            key = urlparse(image.real_name).path.lstrip('/')
            key = unquote(key)
            original = {
                "Bucket": settings.AWS_S3_BUCKET_PRIVATE,
                "Key": key.lstrip('/')
            }

            s3 = boto3.resource('s3',
                aws_access_key_id=settings.AWS_S3_ACCESS_KEY_ID, 
                aws_secret_access_key=settings.AWS_S3_SECRET_ACCESS_KEY )
            bucket = s3.Bucket(settings.AWS_S3_BUCKET_PRIVATE)
            dest_bucket = s3.Bucket(settings.AWS_S3_BUCKET_PUBLIC)
        #image from unfurlfed article url
        else:
            key = image.real_name

        # try:
        #     objs = list(bucket.objects.filter(Prefix=key))
        # except Exception as e:
        #     print(str(e))
        # if len(objs) > 0 and objs[0].key == key:
        try:
            if decision == 'approve':
                if 'c-path-cureid-ncats' in image.real_name:
                    print('moving bucket')
                    new_url = f"{PUBLIC_URL}{key}"
                    image.url = new_url
                    #Copy image into public bucket
                    try:
                        dest_bucket.copy(original, key)
                    except ClientError as e:
                        print(str(e))
                else:
                    image.url = image.real_name
            else:
                s3.Object(settings.AWS_S3_BUCKET_PRIVATE, key).delete()
        except Exception as e:
            print(str(e))
        image.reviewer_id = request.user.id
        image.reviewed = True
        image.save()
        # else:
        #     return Response({
        #         "data": "Image does not exist"
        #     },status=400)



        return Response({
            "data":'Image moved to public bucket'
        })
    except Exception as e:
        return Response({
            "data":str(e)
        })

def create_presigned_url(object_name):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3',
                            aws_access_key_id=settings.AWS_S3_ACCESS_KEY_ID, 
                            aws_secret_access_key=settings.AWS_S3_SECRET_ACCESS_KEY,
                            region_name='us-west-2'
                            )
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': settings.AWS_S3_BUCKET_PRIVATE,
                                                            'Key': unquote(object_name)},
                                                    ExpiresIn=3600)

    except Exception as e:
        print(str(e))
        return None

    # The response contains the presigned URL
    return response