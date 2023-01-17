from django.contrib.contenttypes.models import ContentType
from rest_framework.permissions import SAFE_METHODS
from rest_framework.response import Response
from rest_framework.renderers import BrowsableAPIRenderer
from rest_framework.viewsets import ModelViewSet
from rest_framework.decorators import action
from django.conf import settings

from server.apps.api.v2.pagination import LimitOffsetPagination
from server.apps.api.v2.permissions import BaseCUREIDAccessPermission
from server.apps.api.v2.renderers import CustomRenderer
from server.apps.api.v2.serializers.comment import CommentSerializer, WritableCommentSerializer
from server.apps.core.models import Comment, AttachedImage, Profile

import os


class CommentViewSet(ModelViewSet):
    serializer_class = CommentSerializer
    queryset = Comment.objects.all()
    pagination_class = LimitOffsetPagination
    permission_classes = [BaseCUREIDAccessPermission,]
    renderer_classes = [CustomRenderer, BrowsableAPIRenderer]

    def get_queryset(self):
        type = self.request.query_params.get("content_type", "").lower()
        id = self.request.query_params.get("id", "")
        if type and id:
            content_type = ContentType.objects.filter(app_label='core', model=type).first()
            if not content_type:
                return Comment.objects.none()
            comments = self.queryset.filter(content_type_id=content_type.id, object_id=id).order_by('created', 'parent')
            return self._organize_comments(comments)
        else:
            return self.queryset

    def get_serializer_class(self):
        if self.request.method in SAFE_METHODS:
            return self.serializer_class
        else:
            return WritableCommentSerializer

    def _organize_comments(self, comments):
        root_comments = []
        all_comments = {}
        data = list(comments)
        for comment in data:
            comment.children = []
            all_comments[comment.id] = comment
            if not comment.parent:
                root_comments.append(comment)
            else:
                all_comments[comment.parent_id].children.append(comment)

        cleaned_comments = self._remove_childless_comments(root_comments)
        return cleaned_comments

    def _remove_childless_comments(self, comments):
        """ Removes Deleted or Flagged comments (from the arg list)
            that don't have other comments pointing to them.
        """
        for item in comments:
            if item.children:
                item.children = self._remove_childless_comments(item.children)
            if item.deleted and not item.children:
                comments.remove(item)
        return comments

    def get_parent_id(self,comment):
        if comment.parent:
            return self.get_parent_id(comment.parent)
        else:
            return comment

    def create(self, request):
        try:
            user  = request.user
            model_name = request.data.get('content_type')
            object_id = request.data.get('object_id')
            parent_id = request.data.get('parent', None)
            my_model = ContentType.objects.get(app_label='core', model=model_name)
            comment_body = request.data.get('body')
            new_comment = Comment(
                    body=comment_body,
                    author_id=user.id,
                    content_type_id=my_model.id,
                    object_id=object_id,
                    parent_id=parent_id,
            )
            new_comment.save()

            images = request.data.get('attached_images', [])
            for image in images:
                url = os.path.basename(image['url'])
                caption = image.get('caption', '')
                new_image=AttachedImage(content_object=new_comment, real_name=url, caption=caption)
                new_image.save()

            comments = Comment.objects.filter(content_type_id=my_model.id, object_id=object_id).order_by('created', 'parent')
            organized_comments = self._organize_comments(comments)

            serializer = CommentSerializer(organized_comments, many=True)

            return Response({
                    "data": serializer.data
                },
                status=200)
        except Exception as e:
            return Response({
                "Error: ": str(e)
            })

    def destroy(self, request, *args, **kwargs):
        try:
            super().destroy(request, *args, **kwargs)
            obj = Comment.objects.get(pk=kwargs['pk'])
            comments = Comment.objects.filter(content_type=obj.content_type, object_id=obj.object_id).order_by('created', 'parent')
            organized_comments = self._organize_comments(comments)
            serializer = CommentSerializer(organized_comments, many=True)

            return Response({
                    "data": serializer.data,
                },
                status=200
            )
        except Exception as e:
            print(str(e))

    def update(self, request, *args, **kwargs):
        super().update(request, *args, **kwargs)
        obj = Comment.objects.get(pk=kwargs['pk'])
        comments = Comment.objects.filter(content_type=obj.content_type, object_id=obj.object_id).order_by('created', 'parent')
        organized_comments = self._organize_comments(comments)
        serializer = CommentSerializer(organized_comments, many=True)

        return Response({
                "data": serializer.data,
            },
            status=200
        )


    #should create a common function for like and unlike
    @action(methods=['post'], detail=True, url_path="like", url_name="like_comment")
    def like_comment(self, request, pk=None):
        try:
            data = self.like_unlike_comment(request)
            return Response({
                "liked_comments": data
            },
            status=200)
        except Exception as e:
            return Response({
                "message": str(e)
            },
            status=200)

    @action(methods=['post'], detail=True, url_path="unlike", url_name="like_comment")
    def unlike_comment(self, request, pk=None):
        try:
            data = self.like_unlike_comment(request)
            return Response({
                "liked_comments": data
            },status=200)
        except Exception as e:
            return Response({
                "message": str(e)
            },
            status=200)

    def like_unlike_comment(self, request):
        args = request.path.split('/')
        user = request.user
        mode = args[4]
        id = args[3]

        comment_id = int(id)
        comment = self.queryset.filter(id=comment_id).first()

        if comment:
            if mode == 'unlike':
                user.profile.liked_comments.remove(comment_id)
            if mode == 'like':
                user.profile.liked_comments.append(comment_id)
            user.profile.save()

        liked_comments = Comment.objects.filter(id__in=user.profile.liked_comments).select_related('parent')
        comments = Comment.objects.filter(content_type=comment.content_type, object_id=comment.object_id).order_by('created', 'parent')

        all_organized = self._organize_comments(comments)

        serializer = CommentSerializer(all_organized, many=True)
        return serializer.data

    def _get_comment_tree(self, all_comments, parent_comments):
        return list(set(parent_comments).intersection(all_comments))
