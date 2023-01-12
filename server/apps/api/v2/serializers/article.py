from django.contrib.contenttypes.models import ContentType
from rest_framework import serializers

from server.apps.api.v2.serializers.comment import MinimalCommentSerializer
from server.apps.api.v2.serializers.disease import WritableDiseaseSerializer
from server.apps.api.v2.serializers.image import AttachedImageSerializer
from server.apps.api.v2.serializers.profile import MinimalUserSerializer, FullNameSerializer
from server.apps.core.models import Article, Disease, AttachedImage, Comment, User


def pretty_published_authors(authors: str) -> str:
    if not authors:
        return ''

    if ',' in authors and ';' in authors:
        return authors

    authors = authors.replace('\r\n', ', ')
    authors = authors.replace(';', ', ')
    return authors


class ArticleSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer()
    author = MinimalUserSerializer()
    attached_images = serializers.SerializerMethodField(required=False)
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()
    published_authors = serializers.SerializerMethodField()

    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).values_list('author')
        authors = User.objects.filter(id__in = authors_list)
        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, article):
        content_type = ContentType.objects.get_for_model(article)
        count = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).count()
        return count

    def get_comment_latest(self,article):
        content_type = ContentType.objects.get_for_model(article)
        try:
            latest = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []
        return serializer.data

    def get_attached_images(self, ct_object):
        content_type = ContentType.objects.get_for_model(ct_object)
        attached_images = AttachedImage.objects.filter(content_type_id=content_type, object_id=ct_object.id)
        serializer = AttachedImageSerializer(attached_images, many=True)
        return serializer.data

    def get_name(self, article):
        return article.publication_name if article.publication_name else ""

    def get_published_authors(self, article):
        return pretty_published_authors(article.published_authors)

    class Meta:
        model = Article
        exclude = ['publication_name',]


class WritableArticleSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer(required=False)
    attached_images = serializers.SerializerMethodField(required=False)
    author = MinimalUserSerializer(required=False)
    pubmed_id = serializers.SerializerMethodField()
    comment_count = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()
    published_authors = serializers.SerializerMethodField()


    class Meta:
        model = Article
        fields= '__all__'

    def get_pubmed_id(self, article):
        if not article.pubmed_id:
            return str("")
        return article.pubmed_id

    def get_comment_count(self, discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        count = Comment.objects.filter(content_type_id=content_type.id, object_id=discussion.id).count()
        return count

    def get_name(self, article):
        return article.publication_name if article.publication_name else ""

    def get_attached_images(self, ct_object):
        content_type = ContentType.objects.get_for_model(ct_object)
        attached_images = AttachedImage.objects.filter(content_type_id=content_type, object_id=ct_object.id)
        serializer = AttachedImageSerializer(attached_images, many=True)
        return serializer.data

    def get_published_authors(self, article):
        return pretty_published_authors(article.published_authors)

    def create(self, validated_data):
        author = self.context["request"].user
        disease = validated_data.pop('disease',None)
        images = validated_data.pop('attached_images', None)
        new_article = Article.objects.create(disease=disease, **validated_data, author=author)
        if images:
            for image in images:
                if not AttachedImage.objects.filter(real_name=image['url']).exists():
                    new_image=AttachedImage(content_object=new_article, real_name=image["url"], caption=image.get('caption', None))
                    new_image.save()
        return new_article

    def update(self, instance, validated_data):
        instance.disease = validated_data.get('disease', instance.disease)
        instance.publication_type = validated_data.get('publication_type', instance.publication_type)
        instance.publication_name = validated_data.get('publication_name', instance.publication_name)
        instance.published_authors = validated_data.get('published_authors',instance.published_authors )
        instance.title = validated_data.get('title',instance.title)
        instance.abstract= validated_data.get('abstract',instance.abstract)
        instance.article_url = validated_data.get('article_url',instance.article_url)
        images = validated_data.pop('attached_images', [])

        content_type = ContentType.objects.get_for_model(instance)
        current_images = [image.get('id', None) for image in images]
        images_in_db = [im for im in instance.attached_images.all()]
        images_to_delete = [i for i in images_in_db if i.id not in current_images]
        instance.attached_images.remove(*images_to_delete)

        if images:
            for image in images:
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
        if 'pubmed_id' in data:
            pmid = data.get('pubmed_id', None)
            if not pmid:
                data.update({
                    "pubmed_id": 0
                })
        internal_value = super(WritableArticleSerializer, self).to_internal_value(data)
        if 'attached_images' in data:
            images = data.get("attached_images")
            internal_value.update({
                "attached_images": images
            })
        if 'name' in data:
            publication_name = data.pop('name')
            internal_value.update({
                "publication_name": publication_name,
            })

        return internal_value


class ArticleNewsfeedSerializer(serializers.ModelSerializer):
    disease = WritableDiseaseSerializer()
    author = MinimalUserSerializer()
    attached_images = serializers.SerializerMethodField(required=False)
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()
    name = serializers.SerializerMethodField()
    published_authors = serializers.SerializerMethodField()

    def get_name(self, article):
        return article.publication_name if article.publication_name else ""

    def get_published_authors(self, article):
        return pretty_published_authors(article.published_authors)

    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, article):
        content_type = ContentType.objects.get_for_model(article)
        count = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).count()

        return count

    def get_comment_latest(self,article):
        content_type = ContentType.objects.get_for_model(article)
        try:
            latest = Comment.objects.filter(content_type_id=content_type.id, object_id=article.id).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []

        return serializer.data

    def get_attached_images(self, ct_object):
        content_type = ContentType.objects.get_for_model(ct_object)
        attached_images = AttachedImage.objects.filter(content_type_id=content_type, object_id=ct_object.id)
        serializer = AttachedImageSerializer(attached_images, many=True)
        return serializer.data

    class Meta:
        model = Article
        fields = '__all__'
