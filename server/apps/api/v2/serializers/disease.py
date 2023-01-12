from rest_framework import serializers
from django.conf import settings
from server.apps.core.models import (
    Disease,
    Discussion,
    CureReport,
    Report,
    Drug,
    Regimen,
    fix_title
)
from server.apps.core.models import Disease
from server.apps.core.constants import APPROVED


class FieldsMixin(object):
    def get_report_count(self, disease):
        query = CureReport.objects.filter(report__disease=disease, status=APPROVED)
        return query.count()

    def get_drug_count(self, disease):
        query = CureReport.objects.filter(report__disease=disease, status=APPROVED).values_list('report_id', flat=True)
        count = Regimen.objects.filter(report__in=query).values('drug_id').distinct().count()
        return count

    def get_discussion_count(self, disease):
        return disease.discussions.filter(deleted=False, flagged=False).count()

    def get_trial_count(self, disease):
        return disease.clinical_trials.filter(deleted=False).count()

    def get_event_count(self,disease):
        return disease.events.count()

    def get_article_count(self,disease):
        return disease.articles.count()

    def get_image_url(self, disease):
        image_name = disease.image_name
        if not image_name:
            image_name = ""
        url = "%s%s" % (
            settings.IMAGE_PARTIAL_URL,
            image_name
        )
        return url

    def get_url(self, disease):
        url = "%s%sdiseases/%s" % (
            settings.API_SUB_DOMAIN,
            settings.API_DOMAIN,
            disease.id
        )
        return url


class WritableDiseaseSerializer(serializers.ModelSerializer):
    class Meta:
        model = Disease
        fields = (
            'id',
            'name'
        )
        extra_kwargs = {
            'name': {
                'validators': [],
            }
        }

    def validate(self, *args, **kwargs):
        super().validate(*args, **kwargs)
        disease = Disease.objects.filter(name__icontains=args[0]['name'].capitalize()).first()
        if not disease:
            disease = Disease.objects.create(name=args[0]['name'])
        return disease

class DiseaseSerializer(serializers.ModelSerializer, FieldsMixin):
    name =serializers.CharField(read_only=True)
    url = serializers.SerializerMethodField()
    discussion_count = serializers.SerializerMethodField()
    trial_count = serializers.SerializerMethodField()
    event_count = serializers.SerializerMethodField()
    report_count = serializers.SerializerMethodField()
    article_count = serializers.SerializerMethodField()
    image_url = serializers.SerializerMethodField()

    class Meta:
        model = Disease
        fields = ['id', 'name', 'url', 'discussion_count',
                  'report_count', 'trial_count', 'event_count',
                  'article_count', 'image_url',]


class MinimalDiseaseSerializer(serializers.ModelSerializer, FieldsMixin):
    image_url = serializers.SerializerMethodField()

    class Meta:
        model = Disease
        fields = (
            'id',
            'name',
            'image_url',
        )
