"""
Django settings for server project.

For more information on this file, see
https://docs.djangoproject.com/en/2.2/topics/settings/

For the full list of settings and their config, see
https://docs.djangoproject.com/en/2.2/ref/settings/
"""
from django.utils.translation import gettext_lazy as _
from pathlib import Path

import os

# Load .env file
from dotenv import load_dotenv
load_dotenv()

DEBUG = False

BASE_DIR = Path(__file__).parent.parent.parent
# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/2.2/howto/deployment/checklist/

SECRET_KEY = "CHANGE-ME"

#Variables for creating urls for object urls
API_SUB_DOMAIN = ''
API_DOMAIN = '.ncats.io/v2/'
FRONT_END_SUB_DOMAIN = 'cure'
FRONT_END_DOMAIN = '.ncats.io/'
IMAGE_PARTIAL_URL = 'https://s3-ap-south-1.amazonaws.com/projectcureassets/disease_images/'
DEFAULT_FROM_EMAIL = 'noreplyprojectcure@gmail.com'

# S3 Buckets
ATTACHED_IMAGE_S3_CONTENT_BUCKET = "https://curetestassets.s3.us-west-2.amazonaws.com/comment/"

# Application definition:

INSTALLED_APPS = (
  'rest_framework',
  'rest_framework.authtoken',
  'django_elasticsearch_dsl',
  'django_filters',

  # Your apps go here:
  'server.apps.core.apps.CoreConfig',
  'server.apps.api.apps.ApiConfig',
  'server.apps.wsockets.apps.WsocketsConfig',
  'server.apps.notifications.apps.NotificationsConfig',
  'server.apps.search.apps.SearchConfig',

  # Default django apps:
  'django.contrib.auth',
  'django.contrib.contenttypes',
  'django.contrib.sessions',
  'django.contrib.messages',
  'django.contrib.staticfiles',

  # django-admin:
  'django.contrib.admin',
  'django.contrib.admindocs',

  # channels
  'channels',
  'fcm_django'
)


MIDDLEWARE = (
    # Content Security Policy:
    'csp.middleware.CSPMiddleware',

    # Django:
    'django.middleware.security.SecurityMiddleware',
    # django-permissions-policy
    'django_permissions_policy.PermissionsPolicyMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',

    # CUREID Maintenance Middleware
    'server.apps.api.middleware.CUREMaintenanceMiddleware',
)

ROOT_URLCONF = 'server.urls'

WSGI_APPLICATION = 'server.wsgi.application'
ASGI_APPLICATION = 'server.asgi.application'


# Database
# https://docs.djangoproject.com/en/2.2/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'NAME': os.environ.get('PROJECT_CURE_DB_NAME', 'default'),
        'USER': os.environ.get('PROJECT_CURE_DB_USER', 'default'),
        'PASSWORD': os.environ.get('PROJECT_CURE_DB_PASSWORD', 'default'),
        'HOST': os.environ.get('PROJECT_CURE_DB_HOST', 'localhost'),
        'PORT': os.environ.get('PROJECT_CURE_DB_PORT', 5432),
        'CONN_MAX_AGE': os.environ.get('CONN_MAX_AGE', 60),
        'OPTIONS': {
            'connect_timeout': 10,
        },
    }
}


# Internationalization
# https://docs.djangoproject.com/en/2.2/topics/i18n/

LANGUAGE_CODE = 'en-us'

USE_I18N = True
USE_L10N = True

LANGUAGES = (
    ('en', _('English')),
)

LOCALE_PATHS = (
    'locale/',
)

USE_TZ = True
TIME_ZONE = 'UTC'


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/2.2/howto/static-files/

STATIC_URL = '/static/'

STATICFILES_FINDERS = (
    'django.contrib.staticfiles.finders.FileSystemFinder',
    'django.contrib.staticfiles.finders.AppDirectoriesFinder',
)

STATIC_ROOT = './data/static'

MEDIA_ROOT = 'https://ncats-test.s3.amazonaws.com/comment'
# Templates
# https://docs.djangoproject.com/en/2.2/ref/templates/api

TEMPLATES = [{
    'APP_DIRS': True,
    'BACKEND': 'django.template.backends.django.DjangoTemplates',
    'DIRS': [
        # Contains plain text templates, like `robots.txt`:
        BASE_DIR.joinpath('server', 'templates'),
    ],
    'OPTIONS': {
        'context_processors': [
            # Default template context processors:
            'django.contrib.auth.context_processors.auth',
            'django.template.context_processors.debug',
            'django.template.context_processors.i18n',
            'django.template.context_processors.media',
            'django.contrib.messages.context_processors.messages',
            'django.template.context_processors.request',
        ],
    }
}]


# Django authentication system
# https://docs.djangoproject.com/en/2.2/topics/auth/

AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',
)

PASSWORD_HASHERS = [
    'django.contrib.auth.hashers.BCryptSHA256PasswordHasher',
    'django.contrib.auth.hashers.BCryptPasswordHasher',
    'django.contrib.auth.hashers.PBKDF2PasswordHasher',
    'django.contrib.auth.hashers.PBKDF2SHA1PasswordHasher',
    'django.contrib.auth.hashers.Argon2PasswordHasher',
]


# Django Rest Framework
REST_FRAMEWORK = {
  'DEFAULT_AUTHENTICATION_CLASSES': (
    'rest_framework.authentication.TokenAuthentication',
    'rest_framework.authentication.SessionAuthentication',
  ),
  'DEFAULT_FILTER_BACKENDS': (
    'django_filters.rest_framework.DjangoFilterBackend',
  ),
}


# Security
# https://docs.djangoproject.com/en/2.2/topics/security/

SESSION_COOKIE_HTTPONLY = True
CSRF_COOKIE_HTTPONLY = True
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_BROWSER_XSS_FILTER = True

X_FRAME_OPTIONS = 'DENY'

# https://github.com/adamchainz/django-permissions-policy#setting
PERMISSIONS_POLICY = {
    "accelerometer": [],
    "ambient-light-sensor": [],
    "autoplay": [],
    "camera": [],
    "display-capture": [],
    "document-domain": [],
    "encrypted-media": [],
    "fullscreen": [],
    "geolocation": [],
    "gyroscope": [],
    #"interest-cohort": [],
    "magnetometer": [],
    "microphone": [],
    "midi": [],
    "payment": [],
    "usb": [],
}  # noqa: WPS234


# Timeouts
# https://docs.djangoproject.com/en/2.2/ref/settings/#std:setting-EMAIL_TIMEOUT

EMAIL_TIMEOUT = 5

"""
This file contains a definition for Content-Security-Policy headers.

Read more about it:
https://developer.mozilla.org/ru/docs/Web/HTTP/Headers/Content-Security-Policy

We are using `django-csp` to provide these headers.
Docs: https://github.com/mozilla/django-csp
"""
CSP_DEFAULT_SRC = ("'none'",)
CSP_SCRIPT_SRC = ("'self'", 'code.jquery.com/', 'cdn.jsdelivr.net','sha256-zV+3y0p9TuvwIf1CNhhwjUj33Tdq0ATjhXPGiGFF38c=')
CSP_IMG_SRC = ("'self'", 'ncats-test.s3.amazonaws.com', 'https://c-path-cureid-ncats-secure.s3.amazonaws.com', 'https://c-path-cureid-ncats-public.s3.amazonaws.com')
CSP_FONT_SRC = ("'self'",)
CSP_STYLE_SRC = ("'self'", 'cdn.jsdelivr.net/')
CSP_SCRIPT_SRC_ELEM = ("'self'", 'https://code.jquery.com/', 'https://cdn.jsdelivr.net', "'unsafe-inline'")
CSP_INCLUDE_NONCE_IN = ("script-src","img-src" )
CSP_CONNECT_SRC = ("'self'",)

CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': 'redis://127.0.0.1:6379',
    },
}

# Redis
BROKER_URL = 'redis://localhost:6379'
CELERY_BROKER_URL = 'redis://localhost:6379'
CELERY_RESULT_BACKEND = 'redis://localhost:6379'
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TIMEZONE = 'UTC'

CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            "hosts": [('127.0.0.1', 6379)],
        },
    },
}


# FIREBASE



#PYREBASE
FIREBASE_CONFIG = {
  "apiKey": os.environ.get('FIREBASE_API_KEY'),
  "authDomain": os.environ.get('FIREBASE_AUTH_DOMAIN'),
  "databaseURL": os.environ.get('FIREBASE_DB_URL'),
  "storageBucket": os.environ.get('FIREBASE_STORAGE_BUCKET'),
}

#FIREBASE ADMIN ADK
FIREBASE_PRIVATE_KEY_FILE = os.path.realpath(f"{BASE_DIR}/data/ci-firebase-private.key")
if os.path.isfile(FIREBASE_PRIVATE_KEY_FILE):
    with open(FIREBASE_PRIVATE_KEY_FILE, "r") as f:
        os.environ.setdefault("FIREBASE_PRIVATE_KEY", f.read())

FIREBASE_CREDENTIALS = {
  "type": "service_account",
  "project_id": "ncats-cure-id",
  "private_key_id": os.environ.get('FIREBASE_PRIVATE_KEY_ID'),
  "private_key": os.environ.get('FIREBASE_PRIVATE_KEY'),
  "client_email": os.environ.get('FIREBASE_CLIENT_EMAIL'),
  "client_id": os.environ.get('FIREBASE_CLIENT_ID'),
  "auth_uri": os.environ.get('FIREBASE_AUTH_URI'),
  "token_uri": os.environ.get('FIREBASE_TOKEN_URI'),
  "auth_provider_x509_cert_url": os.environ.get('FIREBASE_AUTH_CERT_URL'),
  "client_x509_cert_url": os.environ.get('FIREBASE_CLIENT_CERT_URL')
}

FCM_DJANGO_SETTINGS = {
     # default: _('FCM Django')
    "APP_VERBOSE_NAME": "['Cure ID']",
     # true if you want to have only one active device per registered user at a time
     # default: False
    "ONE_DEVICE_PER_USER": False,
     # devices to which notifications cannot be sent,
     # are deleted upon receiving error response from FCM
     # default: False
    "DELETE_INACTIVE_DEVICES": False,
    # Transform create of an existing Device (based on registration id) into
                # an update. See the section
    # "Update of device with duplicate registration ID" for more details.
    "UPDATE_ON_DUPLICATE_REG_ID": False,
}



EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'

#Host for sending e-mail.
EMAIL_HOST = 'email-smtp.us-east-1.amazonaws.com'
EMAIL_PORT = 587
EMAIL_HOST_USER = os.environ.get('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = os.environ.get('EMAIL_HOST_PASSWORD')
EMAIL_USE_TLS = True


AWS_S3_ACCESS_KEY_ID = os.environ.get('AWS_S3_ACCESS_KEY_ID')
AWS_S3_SECRET_ACCESS_KEY = os.environ.get('AWS_S3_SECRET_ACCESS_KEY')
AWS_S3_BUCKET_PRIVATE = os.environ.get('AWS_S3_BUCKET_PRIVATE')
AWS_S3_BUCKET_PUBLIC = os.environ.get('AWS_S3_BUCKET_PUBLIC')
