from django.contrib.contenttypes.models import ContentType
from rest_framework import serializers

from server.apps.api.v2.serializers.attached_image import AttachedImageSerializer
from server.apps.api.v2.serializers.profile import UserSerializer, CommentUserSerializer
from server.apps.core.models import Comment, AttachedImage, Profile

import os


class CommentSerializer(serializers.ModelSerializer):
    author = CommentUserSerializer(read_only=True)
    content_type = serializers.SerializerMethodField()
    children = serializers.SerializerMethodField()
    body = serializers.SerializerMethodField()
    attached_images = serializers.SerializerMethodField()
    comment_likes = serializers.SerializerMethodField()

    class Meta:
        model = Comment
        fields = '__all__'

    def get_content_type(self, comment):
       return comment.content_type.model

    def get_children(self, comment):
        result = []
        for child in getattr(comment, "children", []):
            result.append(CommentSerializer(child).data)
        return result

    def get_body(self, comment):
        if comment.deleted:
            return "Comment deleted"
        return comment.body

    def get_attached_images(self, comment):
        images = comment.attached_images.all()
        serializer = AttachedImageSerializer(images, many=True)
        return serializer.data

    def get_comment_likes(self,comment):
        count = Profile.objects.filter(liked_comments__contains = [comment.id]).count()
        profiles = Profile.objects.filter().values_list('liked_comments', flat=True)
        return count

class MinimalCommentSerializer(serializers.ModelSerializer):
    children = serializers.SerializerMethodField()

    def get_children(self, comment):
        result = []
        for child in getattr(comment, "children", []):
            result.append(CommentSerializer(child).data)
        return result

    class Meta:
        model = Comment
        fields = ('body','children' )


class WritableCommentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Comment
        fields = '__all__'

    def to_internal_value(self, data):
        author = data.pop("author", {})
        author_id = author.get("id", None)
        data["author"] = author if not author_id else author_id

        content_type = data.get("content_type", None)
        if content_type:
            try:
                model = ContentType.objects.get(app_label='core', model=content_type)
                data["content_type"] = model.id
            except Exception as e:
                pass

        images = data.get("attached_images", None)
        pk = data.get("id", None)
        if (images or images == []) and pk:
            self._update_attached_images(pk, images)

        return super().to_internal_value(data)

    def _update_attached_images(self, pk, images):
        try:
            instance = Comment.objects.get(pk=pk)
        except:
            return

        saved_attached_images = instance.attached_images.all()
        # There can be image items with "id": None or w/o "id" field
        new_images = [image for image in images if not image.get('id', False)]
        existent_images = {image['id']:image.get('caption', '') for image in images if image.get('id', False)}
        for image in saved_attached_images:
            if image.id not in existent_images:
                image.delete()
            elif image.caption != existent_images[image.id]:
                image.caption = existent_images[image.id]
                image.save()
        for image in new_images:
            url = image.get('url', '')
            if not url:
                continue
            caption = image.get('caption', '')
            AttachedImage.objects.create(content_object=instance, real_name=url, caption=caption)


class CommentWSSerializer(serializers.ModelSerializer):
    author = serializers.SerializerMethodField()
    content_type = serializers.SerializerMethodField()

    class Meta:
        model = Comment
        fields = ['id', 'author', 'parent_id', 'object_id', 'content_type', 'body']

    def get_author(self, obj):
        profile_image = ""
        if obj.author.profile.profile_image.count() > 0:
            profile_image = obj.author.profile.profile_image.all()[0].url
        author = {
            "author_name": f"{obj.author.first_name} {obj.author.last_name}",
            "first_name": obj.author.first_name,
            "last_name": obj.author.last_name,
            "author_image": profile_image,
        }
        if obj.anonymous:
            author["author_name"] = "Anonymous"
            author["first_name"] = ""
            author["last_name"] = ""
            author["author_image"] = "should-return-filler-image-here.jpg"
        return author

    def get_content_type(self, obj):
        return obj.content_type.model


class CommentNewsfeedSerializer(serializers.ModelSerializer):
    author = CommentUserSerializer()
    content_type = serializers.SerializerMethodField()
    children = serializers.SerializerMethodField()
    body = serializers.SerializerMethodField()
    attached_images = serializers.SerializerMethodField()
    comment_likes = serializers.SerializerMethodField()

    class Meta:
        model = Comment
        fields = '__all__'

    def get_content_type(self, comment):
       return comment.content_type.model

    def get_children(self, comment):
        result = []
        for child in getattr(comment, "children", []):
            result.append(CommentSerializer(child).data)
        return result

    def get_body(self, comment):
        if comment.deleted:
            return "Comment deleted"
        return comment.body

    def get_attached_images(self, comment):
        images = comment.attached_images.all()
        serializer = AttachedImageSerializer(images, many=True)
        return serializer.data

    def get_comment_likes(self,comment):
        count = Profile.objects.filter(liked_comments__contains = [comment.id]).count()
        return count


