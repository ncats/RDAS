import json

from asgiref.sync import async_to_sync
from channels.generic.websocket import WebsocketConsumer


class CommentsConsumer(WebsocketConsumer):
    def connect(self):
        type_ = self.scope['url_route']['kwargs']['type']
        if type_ not in ['report', 'discussion']:
            self.close()

        object_id = self.scope['url_route']['kwargs']['object_id']
        self.room_name = type_
        self.room_group_name = f"{type_}_{object_id}"

        self.accept()

        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name,
        )

    def disconnect(self, close_code):
        async_to_sync(self.channel_layer.group_discard)(
            self.room_group_name,
            self.channel_name,
        )

    def receive(self, text_data=None, bytes_data=None):
        text_data_json = json.loads(text_data)
        message = text_data_json["message"]

        async_to_sync(self.channel_layer.group_send)(
            self.room_group_name,
            {
                "type": "comment_message",
                "message": message,
            }
        )

    def comment_message(self, event):
        self.send(text_data=json.dumps(event))



class NewsfeedConsumer(WebsocketConsumer):
    def connect(self):
        content_type = self.scope['url_route']['kwargs']['content_type']
        if content_type not in ['report', 'discussion', 'clinicaltrial', 'article', 'event']:
            self.close()

        self.room_name = f"newsfeed-{content_type}"
        self.room_group_name = self.room_name

        self.accept()

        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name,
        )

    def disconnect(self, close_code):
        async_to_sync(self.channel_layer.group_discard)(
            self.room_group_name,
            self.channel_name,
        )

    def receive(self, text_data=None, bytes_data=None):
        text_data_json = json.loads(text_data)
        message = text_data_json["message"]

        async_to_sync(self.channel_layer.group_send)(
            self.room_group_name,
            {
                "type": "newsfeed_message",
                "message": message,
            }
        )

    def newsfeed_message(self, event):
        self.send(text_data=json.dumps(event))


class TestConsumer(WebsocketConsumer):

    def connect(self):
        self.room_name = self.scope['url_route']['kwargs']['test']
        self.room_group_name = f'test_{self.room_name}'

        # connection has to be accepted
        self.accept()

        # join the room group
        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name,
        )
        print(f"Room Group Name: {self.room_group_name}")
        print(f"Channel Name: {self.channel_name}")

    def disconnect(self, close_code):
        async_to_sync(self.channel_layer.group_discard)(
            self.room_group_name,
            self.channel_name,
        )

    def receive(self, text_data=None, bytes_data=None):
        text_data_json = json.loads(text_data)
        message = text_data_json['message']

        # send chat message event to the room
        async_to_sync(self.channel_layer.group_send)(
            self.room_group_name,
            {
                'type': 'test_message',
                'message': message,
            }
        )

    def test_message(self, event):
        self.send(text_data=json.dumps(event))
