from rest_framework import serializers
from server.apps.core.models import LinkedAccount


class LinkedAccountSerializer(serializers.ModelSerializer):
    class Meta:
        model = LinkedAccount
        exclude = ('user',)
