from rest_framework import serializers
from server.apps.core.models import Organism

class OrganismSerializer(serializers.ModelSerializer):
    class Meta:
        model = Organism
        fields = [
            'id','name'
        ]

    
