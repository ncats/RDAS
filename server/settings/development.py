from .base import *

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = [
    'localhost',
    '0.0.0.0',  # noqa: S104
    '127.0.0.1',
    '[::1]',
    'cure-api-ci.ncats.io',
    '10.9.3.145',
    'cure-api.test.ncats.io',
    '10.9.61.100',
    'cure-api.ncats.io',
]

API_SUB_DOMAIN = ""
API_DOMAIN = "localhost:8000/v2/"


# Django debug toolbar:
# https://django-debug-toolbar.readthedocs.io

INSTALLED_APPS += (
    # Better debug:
    'debug_toolbar',

    'corsheaders',
)

MIDDLEWARE += (
    'corsheaders.middleware.CorsMiddleware',

    'debug_toolbar.middleware.DebugToolbarMiddleware',
    # https://github.com/bradmontgomery/django-querycount
    # Prints how many queries were executed, useful for the APIs.
    'querycount.middleware.QueryCountMiddleware',
)

CORS_ALLOW_ALL_ORIGINS = True
CORS_ALLOWED_ORIGINS = [
    "http://localhost:4200",
    "https://cure-ci.ncats.io",
    "https://cure.test.ncats.io",
    # might look into these to allow mobile
    "http://localhost",
    "capacitor://localhost",
    "ionic://localhost",
]


def _custom_show_toolbar(request):
    """Only show the debug toolbar to users with the superuser flag."""
    return DEBUG and request.user.is_superuser


DEBUG_TOOLBAR_CONFIG = {
    'SHOW_TOOLBAR_CALLBACK':
        'server.settings.development._custom_show_toolbar',
}

# This will make debug toolbar to work with django-csp,
# since `ddt` loads some scripts from `ajax.googleapis.com`:
CSP_SCRIPT_SRC = ("'self'", 'ajax.googleapis.com')
CSP_IMG_SRC = ('*')
CSP_CONNECT_SRC = ("'self'",)

EMAIL_HOST = 'localhost'
EMAIL_PORT = 1025
EMAIL_USE_TLS = False
EMAIL_BACKEND = 'django.core.mail.backends.filebased.EmailBackend'
EMAIL_FILE_PATH = '/tmp/app-messages' # change this to a proper location

from elasticsearch import RequestsHttpConnection
ELASTICSEARCH_DSL={
    'default': {
        'hosts': 'localhost:9200',
        'use_ssl': True,
        'verify_certs': True,
        'http_auth': ('elastic', os.environ.get('PROJECT_CURE_ES_USER_PASSWORD', '')),
        'ca_certs': './data/http_ca.crt',
        'connection_class': RequestsHttpConnection
    },
}

AWS_S3_ACCESS_KEY_ID = os.environ.get('AWS_S3_ACCESS_KEY_ID')
AWS_S3_SECRET_ACCESS_KEY = os.environ.get('AWS_S3_SECRET_ACCESS_KEY')
AWS_S3_BUCKET_PRIVATE = os.environ.get('AWS_S3_BUCKET_PRIVATE')
AWS_S3_BUCKET_PUBLIC = os.environ.get('AWS_S3_BUCKET_PUBLIC')
