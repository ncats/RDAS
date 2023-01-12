from django.contrib.contenttypes.models import ContentType
from rest_framework import serializers

from server.apps.api.v2.serializers.comment import MinimalCommentSerializer
from server.apps.api.v2.serializers.disease import WritableDiseaseSerializer
from server.apps.api.v2.serializers.profile import MinimalUserSerializer
from server.apps.core.models import Event,Comment, User


class EventSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer(required=False)
    author = MinimalUserSerializer()
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()

    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, event):
        content_type = ContentType.objects.get_for_model(event)
        count = Comment.objects.filter(content_type_id=content_type.id, object_id=event.id).count()

        return count

    def get_comment_latest(self,event):
        content_type = ContentType.objects.get_for_model(event)
        try:
            latest = Comment.objects.filter(content_type_id=content_type.id, object_id=event.id).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []

        return serializer.data

    class Meta:
        model = Event
        fields = '__all__'

class WritableEventSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer(required=False)
    author = MinimalUserSerializer(required=False)

    class Meta:
        model = Event
        fields = '__all__'

    def create(self,validated_data):
        author = self.context["request"].user
        disease = validated_data.pop('disease',None)
        new_event = Event.objects.create(disease=disease, author=author, **validated_data)
        new_event.save()
        return new_event

    def update(self, instance, validated_data):
        instance.disease = validated_data.get('disease', instance.disease)
        instance.title = validated_data.get('title', instance.title)
        instance.event_description = validated_data.get('event_description', instance.event_description)
        instance.event_sponsor = validated_data.get('event_sponsor', instance.event_sponsor)
        instance.url = validated_data.get('url', instance.url)
        instance.location = validated_data.get('location', instance.location)
        instance.contact = validated_data.get('contact', instance.contact)
        instance.event_start = validated_data.get('event_start', instance.event_start)
        instance.event_start_time = validated_data.get('event_start_time', instance.event_start_time)
        instance.event_end = validated_data.get('event_end',instance.event_end)
        instance.event_end_time = validated_data.get('event_end_time', instance.event_end_time)

        instance.save()
        return instance


class EventNewsfeedSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer(required=False)
    author = MinimalUserSerializer()
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()

    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, event):
        content_type = ContentType.objects.get_for_model(event)
        count = Comment.objects.filter(content_type_id=content_type.id, object_id=event.id).count()
        return count

    def get_comment_latest(self,event):
        content_type = ContentType.objects.get_for_model(event)
        try:
            latest = Comment.objects.filter(content_type_id=content_type.id, object_id=event.id).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []
        return serializer.data

    class Meta:
        model = Event
        fields = '__all__'
