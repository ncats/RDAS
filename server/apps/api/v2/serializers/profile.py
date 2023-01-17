import profile
from django.contrib.auth.models import User
from rest_framework import serializers

from server.apps.api.v2.serializers.disease import MinimalDiseaseSerializer, WritableDiseaseSerializer
from server.apps.api.v2.serializers.drug import MinimalDrugSerializer
from server.apps.core.models import LinkedAccount, Profile, Drug, AttachedImage
from django.contrib.contenttypes.models import ContentType
from server.apps.core.constants import ATTACHED_IMAGE_FILLER

class LinkedAccountsSerializer(serializers.ModelSerializer):

    class Meta:
        model = LinkedAccount
        fields = (
            "uid",
            "user_id",
            "provider"
        )


class CommentUserSerializer(serializers.ModelSerializer):
    profile_image = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ['id', 'first_name', 'last_name', 'profile_image']

    def get_profile_image(self, profile):
        content_type = ContentType.objects.get_for_model(profile)
        profile_image = AttachedImage.objects.filter(content_type_id=content_type, object_id=profile.id).first()
        if profile_image:
            if profile_image.url == ATTACHED_IMAGE_FILLER:
                return f"assets/{profile_image.url}"
            else:
                return profile_image.url
        else:
            return ''

class UserSerializer(serializers.ModelSerializer):
    username = serializers.CharField(read_only=True)
    linked_accounts = LinkedAccountsSerializer(many=True)

    class Meta:
        model = User
        fields = (
            'id',
            'username',
            'email',
            'first_name',
            'last_name',
            'linked_accounts',
            'is_staff',
            'is_superuser'
        )


class MinimalUserSerializer(serializers.ModelSerializer):
    qualification = serializers.CharField(source='profile.qualification')
    first_name = serializers.SerializerMethodField()
    last_name = serializers.SerializerMethodField()

    def get_first_name(self,user):
        if user.is_superuser:
            return "CURE"
        else:
            return user.first_name

    def get_last_name(self,user):
        if  user.is_superuser:
            return "Admin"
        else:
            return user.last_name


    class Meta:
        model = User
        fields = (
            'id',
            'first_name',
            'last_name',
            'is_superuser',
            'is_staff',
            'qualification'
        )

class DiscussionUserSerializer(serializers.ModelSerializer):
    qualification = serializers.CharField(source='profile.qualification')

    class Meta:
        model = User
        fields = (
            'id',
            'first_name',
            'last_name',
            'qualification'
        )

class EditableUserSerializer(serializers.ModelSerializer):
    linked_accounts = LinkedAccountsSerializer(many=True, required=False)

    class Meta:
        model = User
        fields = (
            "id",
            "first_name",
            "last_name",
            "linked_accounts",
        )
        read_only_fields = (
            'username',
            'password'
        )


class FullNameSerializer(serializers.ModelSerializer):
    full_name = serializers.CharField()
    # Not removing full_name in case somebody else uses it
    name = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = (
            'id',
            'full_name',
            'name',
        )

    def get_name(self, user):
        return user.full_name if user.full_name else ""


class ProfileSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True)
    favorited_drugs = MinimalDrugSerializer(many=True)
    favorited_diseases = MinimalDiseaseSerializer(many=True)
    profile_image = serializers.SerializerMethodField(required=False)



    class Meta:
        model = Profile
        exclude  = [
            'created',
            'updated',
            'status',
            'terms_and_conditions',
        ]

    def get_profile_image(self, profile):
        content_type = ContentType.objects.get_for_model(profile)
        profile_image = AttachedImage.objects.filter(content_type_id=content_type, object_id=profile.id).first()
        if profile_image:
            if profile_image.url == ATTACHED_IMAGE_FILLER:
                return f"assets/{profile_image.url}"
            else:
                return profile_image.url
        else:
            return ''

class EditableProfileSerializer(serializers.ModelSerializer):
    user = EditableUserSerializer()
    profile_image = serializers.SerializerMethodField(required=False)


    class Meta:
        model = Profile
        exclude  = [
            'created',
            'updated',
            'status',
            'terms_and_conditions',
        ]

    def get_profile_image(self, profile):
        content_type = ContentType.objects.get_for_model(profile)
        profile_image = AttachedImage.objects.filter(content_type_id=content_type, object_id=profile.id).first()

        if profile_image:
            if profile_image.url == ATTACHED_IMAGE_FILLER:
                return f"assets/{profile_image.url}"
            else:
                return profile_image.url
        else:
            return ''

    def to_internal_value(self, data):
        # TODO: doesn't allow updating drugs or diseases; it's possible to send
        #   ids with wrong names, all the code cares about is <id>
        for field in ["favorited_drugs", "favorited_diseases"]:
            items = data.pop(field, [])
            data[field] = []
            for item in items:
                data[field].append(item['id'])
        internal_value = super().to_internal_value(data)

        if 'profile_image' in  data:
            image = data.get("profile_image")
            internal_value.update({
                "profile_image": image
            })
        return internal_value

    def to_representation(self, instance):
        return {"data": ProfileSerializer().to_representation(instance)}

    def _process_favorited_items(self, instance, validated_data):
        fields = [
                "favorited_drugs", "favorited_diseases", "favorited_discussions",
                "favorited_events", "favorited_clinical_trials", "favorited_reports",
                "favorited_articles"
        ]
        for field in fields:
            if field not in validated_data:
                continue
            items = validated_data.pop(field, [])
            request_items = [i.id for i in items]
            currently_in_db = [i.id for i in getattr(instance, field).all()]
            missing = [i for i in currently_in_db if i not in request_items]
            getattr(instance, field).remove(*missing)
            new = [i for i in request_items if i not in currently_in_db]
            getattr(instance, field).add(*new)

    def update(self, instance, validated_data):
        # - user ---------------------------------------------------------
        user = validated_data.pop('user', None)
        if user:
            update = False
            for field in ["first_name", "last_name"]:
                value = user.get(field, "").strip()
                if value and value != getattr(instance.user, field):
                    print(value)
                    setattr(instance.user, field, value)
                    update = True
            if update:
                instance.user.save()


        self._process_favorited_items(instance, validated_data)
        profile_image = validated_data.pop('profile_image', None)
        if profile_image:
            my_model = ContentType.objects.get(app_label='core', model='profile')
            current_image = AttachedImage.objects.filter(content_type_id=my_model.id, object_id=instance.id).first()
            if current_image:
                if not (current_image.real_name == profile_image or current_image.url == profile_image):
                    current_image.delete()
                    new_image=AttachedImage(content_type_id=my_model.id, object_id=instance.id, real_name=profile_image)
                    new_image.save()
            else:
                new_image=AttachedImage(content_type_id=my_model.id, object_id=instance.id, real_name=profile_image)
                new_image.save()

        return super().update(instance, validated_data)
