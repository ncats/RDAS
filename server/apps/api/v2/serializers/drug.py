from server.apps.core.models import Drug, fix_title, Disease
from rest_framework import serializers
from django.conf import settings


class DrugSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    name = serializers.CharField(read_only=True, max_length=100)
    url = serializers.SerializerMethodField()
    rxnorm_id = serializers.IntegerField(read_only=True)
    fda_approved = serializers.BooleanField(read_only=True)
    total = serializers.IntegerField(read_only=True)

    class Meta:
        model = Drug
        fields = (
            'id',
            'name',
            'url',
            'rxnorm_id',
            'fda_approved',
            'total'
        )

    def get_url(self, drug):
        drug_id = ''
        if isinstance(drug, Drug):
            drug_id = drug.id
        # /drugs?disease=<disease_id> returns a list
        # not a Drug object
        elif isinstance(drug, dict):
            drug_id = drug['id']
        url = "%s%sdrugs/%s" % (
            settings.API_SUB_DOMAIN,
            settings.API_DOMAIN,
            drug_id
        )
        return url


class WritableDrugSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(read_only=True)
    name = serializers.CharField(max_length=100, required=False)
    rxNorm_id = serializers.IntegerField(allow_null=True, required=False)
    notes = serializers.CharField(allow_null=True, allow_blank=True, required=False)

    class Meta:
        model = Drug
        fields = (
            'id',
            'name',
            'rxNorm_id',
            'notes'
        )

    def validate(self, attrs):
        drug_name = fix_title(attrs.pop('name'))
        # TODO: change the below to
        #  attrs.update({"name": drug_name})
        #  return? super().validate(attrs)
        drug, created = Drug.objects.get_or_create(name=drug_name)
        if attrs:
            for key in attrs:
                setattr(drug, key, attrs[key])
            drug.save()
        return drug


class MinimalDrugSerializer(serializers.ModelSerializer):
    class Meta:
        model = Drug
        fields = (
            'id',
            'name'
        )

    def validate(self, *args, **kwargs):
        super().validate(*args, **kwargs)
        drug = Drug.objects.filter(name=args[0]['name']).first()
        if not drug:
            raise serializers.ValidationError(f'No drug named [{args[0]["name"]}] found.')
        return drug
