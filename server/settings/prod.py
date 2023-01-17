from .base import *

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

CSRF_TRUSTED_ORIGINS=['https://*.ncats.io']

ALLOWED_HOSTS = [
    'localhost',
    '0.0.0.0',  # noqa: S104
    '127.0.0.1',
    '[::1]',
    'cure-api-prod.ncats.io',
    'cure-api.ncats.io',
    '10.10.3.42',
]

API_SUB_DOMAIN = "cure-api"


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
        'hosts': 'cure-es.ncats.io:9200',
    },
}

AWS_S3_ACCESS_KEY_ID = os.environ.get('AWS_S3_ACCESS_KEY_ID')
AWS_S3_SECRET_ACCESS_KEY = os.environ.get('AWS_S3_SECRET_ACCESS_KEY')
AWS_S3_BUCKET_PRIVATE = os.environ.get('AWS_S3_BUCKET_PRIVATE')
AWS_S3_BUCKET_PUBLIC = os.environ.get('AWS_S3_BUCKET_PUBLIC')

EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'

#Host for sending e-mail.
EMAIL_HOST = 'email-smtp.us-east-1.amazonaws.com'
EMAIL_PORT = 587
EMAIL_HOST_USER = os.environ.get('EMAIL_HOST_USER')
EMAIL_HOST_PASSWORD = os.environ.get('EMAIL_HOST_PASSWORD')
EMAIL_USE_TLS = True