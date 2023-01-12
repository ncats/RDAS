from rest_framework import serializers
from django.contrib.contenttypes.models import ContentType

from server.apps.api.v2.serializers.comment import MinimalCommentSerializer
from server.apps.api.v2.serializers.disease import WritableDiseaseSerializer, MinimalDiseaseSerializer
from server.apps.api.v2.serializers.image import AttachedImageSerializer
from server.apps.api.v2.serializers.profile import DiscussionUserSerializer
from server.apps.core.models import Discussion, Disease, AttachedImage, User, Comment


class DiscussionSerializer(serializers.ModelSerializer):
    disease = MinimalDiseaseSerializer()
    author = DiscussionUserSerializer()
    attached_images = serializers.SerializerMethodField()
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()

    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(
                content_type_id=content_type.id, object_id=article.id,
                deleted=False, flagged=False, anonymous=False,
            ).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = DiscussionUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, discussion):
        return discussion.comments.filter(deleted=False, flagged=False).count()

    def get_comment_latest(self, discussion):
        comments = discussion.comments.filter(deleted=False, flagged=False)
        if comments:
            latest = comments.latest('created')
            serializer = MinimalCommentSerializer(latest)
            return serializer.data
        return []

    def get_attached_images(self, discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        images = AttachedImage.objects.filter(content_type_id=content_type.id, object_id=discussion.id)
        serializer = AttachedImageSerializer(images, many=True)
        return serializer.data

    class Meta:
        model = Discussion
        fields = '__all__'

    def to_representation(self, instance):
        data = super().to_representation(instance)
        if instance.anonymous:
            author = data.pop('author', None)
            author.update({'id': None})
            author.update({'first_name': 'Anonymous'})
            author.update({'last_name': ''})
            data.update({'author': author})
        return data


class WritableDiscussionSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer()
    author = DiscussionUserSerializer(required=False)
    attached_images = serializers.SerializerMethodField()
    comment_count = serializers.SerializerMethodField()

    class Meta:
        model = Discussion
        fields ='__all__'

    def get_comment_count(self, discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        count = AttachedImage.objects.filter(
                content_type_id=content_type.id, object_id=discussion.id,
            ).count()
        return count

    def get_attached_images(self, discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        images = AttachedImage.objects.filter(content_type_id=content_type.id, object_id=discussion.id)
        serializer = AttachedImageSerializer(images, many=True)
        return serializer.data

    def create(self, validated_data):
        author = self.context["request"].user
        disease = validated_data.pop('disease', None)
        images = validated_data.pop('attached_images', None)
        new_discussion = Discussion.objects.create(disease=disease, **validated_data, author=author)

        if images:
            try:
                for image in images:
                    # TODO: image['url'] is the full S3 address or just the filename?
                    #   It is expected to save just the filepath in AttachedImage
                    # TODO: if we have 3 images it will make 3 queries to DB.
                    if not AttachedImage.objects.filter(real_name=image['url']).exists():
                        new_image=AttachedImage(content_object=new_discussion, real_name=image["url"], caption=image.get('caption', None))
                        new_image.save()
            except Exception as e:
                pass

        return new_discussion

    def update(self, instance, validated_data):
        instance.disease = validated_data.get('disease', instance.disease)
        instance.body = validated_data.get('body',instance.body )
        instance.title = validated_data.get('title',instance.title)
        instance.anonymous = validated_data.get('anonymous', instance.anonymous)

        images = validated_data.pop('attached_images', [])
        current_images = [image.get('id', None) for image in images]
        images_in_db = [im for im in instance.attached_images.all()]
        images_to_delete = [i for i in images_in_db if i.id not in current_images]
        instance.attached_images.remove(*images_to_delete)

        if images:
            for image in images:
                # TODO: does it even make sense to update filename in AttachedImage?
                if 'id' in image or AttachedImage.objects.filter(real_name=image['url']).exists():
                    image_to_update = AttachedImage.objects.get(id=image['id'])
                    image_to_update.caption = image['caption']
                    image_to_update.save()
                else:
                    new_image=AttachedImage(content_object=instance, real_name=image.get('url'), caption=image.get('caption',None))
                    new_image.save()

        instance.save()
        return instance

    def to_internal_value(self, data):
        internal_value = super().to_internal_value(data)
        if 'attached_images' in data:
            internal_value.update({
                "attached_images": data.get("attached_images")
            })
        return internal_value

    def to_representation(self, instance):
        data = super().to_representation(instance)
        if instance.anonymous:
            author = data.pop('author', None)
            author.update({'id': None})
            author.update({'first_name': 'Anonymous'})
            author.update({'last_name': ''})
            data.update({'author': author})
        return data


class DiscussionNewsfeedSerializer(serializers.ModelSerializer):
    disease = MinimalDiseaseSerializer()
    author = DiscussionUserSerializer()
    attached_images = serializers.SerializerMethodField()
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()

    # TODO: don't forget that comments/discussions can be anonymous
    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(
                content_type_id=content_type.id, object_id=article.id,
                deleted=False, flagged=False, anonymous=False,
            ).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = DiscussionUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, discussion):
        return discussion.comments.filter(deleted=False, flagged=False).count()

    def get_comment_latest(self, discussion):
        comments = discussion.comments.filter(deleted=False, flagged=False)
        if comments:
            latest = comments.latest('created')
            serializer = MinimalCommentSerializer(latest)
            return serializer.data
        return []

    # TODO: these should be inside the comment_authors structure
    def get_attached_images(self, discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        images = AttachedImage.objects.filter(content_type_id=content_type.id, object_id=discussion.id)
        serializer = AttachedImageSerializer(images, many=True)
        return serializer.data

    class Meta:
        model = Discussion
        fields = '__all__'

    def to_representation(self, instance):
        data = super().to_representation(instance)
        if instance.anonymous:
            author = data.pop('author', None)
            author.update({'id': None})
            author.update({'first_name': 'Anonymous'})
            author.update({'last_name': ''})
            data.update({'author': author})
        return data
