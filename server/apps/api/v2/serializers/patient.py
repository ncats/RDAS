from multiprocessing.sharedctypes import Value
from rest_framework import serializers
from server.apps.core.models import Patient,Comorbidity,Neonate,Pregnancy



class ComorbiditySerializer(serializers.ModelSerializer):
    # value = serializers.CharField()
    class Meta:
        model = Comorbidity
        fields=(
            'value',
        )


class NeonateSerializer(serializers.ModelSerializer):

     class Meta:
         model = Neonate
         fields = '__all__'

class PregnancySerializer(serializers.ModelSerializer):
    neonates = NeonateSerializer(many=True, default=[])
    class Meta:
        model = Pregnancy
        fields = '__all__'

class PatientSerializer(serializers.ModelSerializer):
    comorbidities = ComorbiditySerializer(many=True, default=[], source='comorbidity')
    pregnancy = PregnancySerializer()
    races = serializers.SerializerMethodField()


    def get_races(self, patient):
        result = []
        if patient.race:
            result = [{"value": r } for r in patient.race]
        return result

    class Meta:
        model = Patient
        fields = '__all__'

        depth = 1
