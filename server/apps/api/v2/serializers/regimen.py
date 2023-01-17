from rest_framework import serializers
from server.apps.core.models import Regimen
from server.apps.api.v2.serializers.drug import DrugSerializer


class RegimenSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    drug = DrugSerializer()
    use_drug = serializers.SerializerMethodField()

    def get_use_drug(self, regimen):
        result = []
        if regimen.use_drug:
            result = [{"value": ud} for ud in regimen.use_drug]
        return result

    class Meta:
        model = Regimen
        fields = '__all__'
