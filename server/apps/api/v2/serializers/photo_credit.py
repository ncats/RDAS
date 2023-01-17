from rest_framework import serializers
from server.apps.core.models import PhotoCredit
from server.apps.api.v2.serializers.disease import MinimalDiseaseSerializer


class PhotoCreditSerializer(serializers.ModelSerializer):
    disease = MinimalDiseaseSerializer()
    class Meta:
        model = PhotoCredit
        fields = '__all__'
