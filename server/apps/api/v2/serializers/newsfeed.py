from rest_framework import serializers

from server.apps.core.constants import APPROVED
from server.apps.core.models import Comment, CureReport, Newsfeed, Report, Discussion, ClinicalTrial, Article, Event
from .article import ArticleNewsfeedSerializer
from .clinical_trial import ClinicalTrialNewsfeedSerializer
from .comment import CommentNewsfeedSerializer
from .discussion import DiscussionNewsfeedSerializer
from .event import EventNewsfeedSerializer
from .report import ReportNewsfeedSerializer


class NewsfeedWSSerializer(serializers.ModelSerializer):
    content_type = serializers.SerializerMethodField()
    object_data = serializers.SerializerMethodField()

    class Meta:
        model = Newsfeed
        fields=['content_type', 'object_data']

    def get_content_type(self, instance):
        return instance.content_type.model

    def get_object_data(self, instance):
        if instance.content_type.model == "comment":
            comment = Comment.objects.filter(pk=instance.object_id, flagged=False, deleted=False).first()
            if comment:
                serializer = CommentNewsfeedSerializer(comment)
                return serializer.data
        if instance.content_type.model == "curereport":
            curereport = CureReport.objects.filter(pk=instance.object_id, flagged=False, status=APPROVED).first()
            if curereport:
                serializer = ReportNewsfeedSerializer(curereport.report)
                return serializer.data
        if instance.content_type.model == "discussion":
            discussion = Discussion.objects.filter(pk=instance.object_id, flagged=False, deleted=False).first()
            if discussion:
                serializer = DiscussionNewsfeedSerializer(discussion)
                return serializer.data
        if instance.content_type.model == "clinicaltrial":
            clinical_trial = ClinicalTrial.objects.filter(pk=instance.object_id, deleted=False).first()
            if clinical_trial:
                serializer = ClinicalTrialNewsfeedSerializer(clinical_trial)
                return serializer.data
        if instance.content_type.model == "event":
            event = Event.objects.filter(pk=instance.object_id).first()
            if event:
                serializer = EventNewsfeedSerializer(event)
                return serializer.data
        if instance.content_type.model == "article":
            article = Article.objects.filter(pk=instance.object_id).first()
            if article:
                serializer = ArticleNewsfeedSerializer(article)
                return serializer.data
        return ""
