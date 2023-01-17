import os

from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application

import server.apps.wsockets.routing


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'server.settings.base')

application = ProtocolTypeRouter({
  'http': get_asgi_application(),
  'websocket': AuthMiddlewareStack(
        URLRouter(
            server.apps.wsockets.routing.websocket_urlpatterns
        )
    ),
})
