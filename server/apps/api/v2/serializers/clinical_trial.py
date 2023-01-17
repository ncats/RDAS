from django.contrib.contenttypes.models import ContentType
from rest_framework import serializers

from server.apps.api.v2.serializers.comment import MinimalCommentSerializer
from server.apps.api.v2.serializers.disease import MinimalDiseaseSerializer,WritableDiseaseSerializer
from server.apps.api.v2.serializers.image import AttachedImageSerializer
from server.apps.api.v2.serializers.profile import MinimalUserSerializer
from server.apps.core.models import ClinicalTrial, AttachedImage, Disease, Comment, User


class ClinicalTrialSerializer(serializers.ModelSerializer):
    disease = MinimalDiseaseSerializer()
    comment_count = serializers.SerializerMethodField()
    author = MinimalUserSerializer()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()
    drugs = serializers.SerializerMethodField()

    class Meta:
        model = ClinicalTrial
        fields = '__all__'

    def get_comment_authors(self, clinicaltrial):
        content_type = ContentType.objects.get_for_model(clinicaltrial)
        authors_list = Comment.objects.filter(content_type_id=content_type.id, object_id=clinicaltrial.id).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, clinicaltrial):
        content_type = ContentType.objects.get_for_model(clinicaltrial)
        count = Comment.objects.filter(content_type=content_type, object_id=clinicaltrial.id).count()
        return count

    def get_comment_latest(self, clinicaltrial):
        content_type = ContentType.objects.get_for_model(clinicaltrial)
        try:
            latest = Comment.objects.filter(content_type_id=content_type.id, object_id=clinicaltrial.id).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []
        return serializer.data

    def get_drugs(self, clinicaltrial):
        """ 1. Remove "Drug: " from the value(s)
            2. Concatenate them with "; "
            3. Don't return "Other: xxxx", "Device: xxxx", "Procedure: xxxx", "Genetic: xxxx"
        """
        drugs_string = ""
        prepend = ""
        remove = ['Other: ', 'Device: ', 'Procedure: ', 'Genetic: ']

        if not clinicaltrial.drugs:
            return ""

        for value in clinicaltrial.drugs:
            if list(filter(lambda n: value.startswith(n), remove)):
                continue
            drugs_string = f"{drugs_string}{prepend}{value.replace('Drug: ', '')}"
            prepend = "; "
        return drugs_string

class ClinicalTrialDataVisiualizationSerializer(serializers.ModelSerializer,):
    disease = MinimalDiseaseSerializer()

    class Meta:
        model = ClinicalTrial
        exclude = [
            'drugs',
            'title',
            'clinical_trials_gov_id',
            'deleted',
            'sponsor',
            'created',
            'updated'
        ]


class MinimalClinicalTrialSerializer(serializers.ModelSerializer):
    class Meta:
        model = ClinicalTrial
        fields = (
            'id',
            'title'
        )


class WritableClinicalTrialSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer(required=False)
    images = serializers.SerializerMethodField(required=False)
    author = MinimalUserSerializer(required=False)

    class Meta:
        model = ClinicalTrial
        exclude = [
            'created',
            'updated'
        ]

    def get_images(self, ct_object):
        content_type = ContentType.objects.get_for_model(ct_object)
        images = AttachedImage.objects.filter(content_type_id=content_type, object_id=ct_object.id, reviewed=True)
        serializer = AttachedImageSerializer(images, many=True)
        return serializer.data

    def create(self, validated_data):
        author = self.context["request"].user
        new_ct = ClinicalTrial.objects.create(**validated_data, author=author)
        return new_ct

    def update(self, instance, validated_data):
        try:
            disease = validated_data.get('disease', instance.disease)
            updated_disease, created = Disease.objects.get_or_create(name=disease.get('name'))
            instance.report.disease = updated_disease
        except:
            pass

        instance.clinical_trials_gov_id = validated_data.get('clinical_trial_govs_id', instance.clinical_trials_gov_id)
        instance.title = validated_data.get('title', instance.title)
        instance.ct_status = validated_data.get('ct_status', instance.ct_status)
        instance.phase = validated_data.get('phase', instance.phase)
        instance.participants = validated_data.get('participants', instance.participants)
        instance.interventions = validated_data.get('interventions', instance.interventions)
        instance.locations = validated_data.get('locations', instance.locations)
        instance.start_year = validated_data.get('start_year', instance.start_year)
        instance.country = validated_data.get('country', instance.country)
        instance.drugs = validated_data.get('drugs', instance.drugs)
        instance.sponsor = validated_data.get('sponsor', instance.sponsor)
        instance.save()
        return super().update(instance, validated_data)


class ClinicalTrialNewsfeedSerializer(serializers.ModelSerializer):
    disease = MinimalDiseaseSerializer()
    comment_count = serializers.SerializerMethodField()
    author = MinimalUserSerializer()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()

    class Meta:
        model = ClinicalTrial
        fields = '__all__'

    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, clinicaltrial):
        content_type = ContentType.objects.get_for_model(clinicaltrial)
        count = Comment.objects.filter(content_type=content_type, object_id=clinicaltrial.id).count()
        return count

    def get_comment_latest(self,discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        try:
            latest = Comment.objects.filter(content_type_id=content_type.id, object_id=discussion.id).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []

        return serializer.data
