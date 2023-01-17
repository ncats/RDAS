from .base import *

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

ALLOWED_HOSTS = [
    'localhost',
    '0.0.0.0',  # noqa: S104
    '127.0.0.1',
    '[::1]',
    'cure-api.test.ncats.io',
    '10.9.61.100',
]

API_SUB_DOMAIN = "cure-api.test"

INSTALLED_APPS += (
    'corsheaders',
)

MIDDLEWARE += (
    'corsheaders.middleware.CorsMiddleware',
)

CORS_ALLOW_ALL_ORIGINS = True

# This will make debug toolbar to work with django-csp,
# since `ddt` loads some scripts from `ajax.googleapis.com`:
CSP_SCRIPT_SRC = ("'self'", 'ajax.googleapis.com')
CSP_IMG_SRC = ('*')
CSP_CONNECT_SRC = ("'self'",)

# We need this so DRF generates next/previous links with https
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
SECURE_SSL_REDIRECT = True
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True

ELASTICSEARCH_DSL={
    'default': {
        'hosts': 'cure-es.test.ncats.io:9200',
    },
}

AWS_S3_ACCESS_KEY_ID = os.environ.get('AWS_S3_ACCESS_KEY_ID')
AWS_S3_SECRET_ACCESS_KEY = os.environ.get('AWS_S3_SECRET_ACCESS_KEY')
AWS_S3_BUCKET_PRIVATE = os.environ.get('AWS_S3_BUCKET_PRIVATE')
AWS_S3_BUCKET_PUBLIC = os.environ.get('AWS_S3_BUCKET_PUBLIC')