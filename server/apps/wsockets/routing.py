from django.urls import re_path

from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/test/(?P<test>\w+)/$', consumers.TestConsumer.as_asgi()),
    re_path(r'ws/newsfeed/(?P<content_type>\w+)/$', consumers.NewsfeedConsumer.as_asgi()),
    re_path(r'ws/(?P<type>\w+)-comments/(?P<object_id>\w+)/$', consumers.CommentsConsumer.as_asgi()),
]
