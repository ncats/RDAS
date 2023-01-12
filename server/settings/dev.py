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
]

API_SUB_DOMAIN = "cure-api-ci"


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
CSP_IMG_SRC = ("'self'", 'data:')
CSP_CONNECT_SRC = ("'self'",)

# We need this so DRF generates next/previous links with https
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
SECURE_SSL_REDIRECT = True
SESSION_COOKIE_SECURE = True
CSRF_COOKIE_SECURE = True

ELASTICSEARCH_DSL={
    'default': {
        'hosts': 'cure-es-ci.ncats.io:9200',
    },
}

