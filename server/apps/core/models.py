from datetime import datetime
from django.contrib.auth.models import User
from django.contrib.contenttypes.fields import GenericForeignKey, GenericRelation
from django.contrib.contenttypes.models import ContentType
from django.contrib.postgres.fields import ArrayField
from django.core.exceptions import ValidationError
from django.db import models
from django.db.models import JSONField
from django.db.models.functions import Lower
from django.db.models.signals import post_save
# from server.apps.core.admin import JournalArticle,NewsArticle
import os

from .constants import *
from .constants_country import COUNTRIES
from .constants_language import LANGUAGES
from .signals import create_newsfeed, push_notifications, send_notification_for_disease_oi, comment_on_object


class CureUser(User):
    class Meta:
        proxy = True
        ordering = [Lower("first_name"), Lower("last_name")]

    def __str__(self):
        return f"{self.first_name} {self.last_name}"


class CreatedUpdatedModel(models.Model):
    """
        Abstract class with created and updated fields.
    """
    created = models.DateTimeField(
		auto_now_add=True,
		help_text="Datetime when the object was created."
	)
    updated = models.DateTimeField(
		auto_now=True,
		help_text="Datetime when the object was last updated."
	)

    class Meta:
        abstract = True


class BasePostModel(CreatedUpdatedModel):
    author = models.ForeignKey(
        CureUser,
        on_delete=models.PROTECT,
        blank=False,
        null=True,
		help_text="User account that created this content."
    )
    body = models.TextField(
		help_text="Text content of the object."
	)
    anonymous = models.BooleanField(
		default=False,
		help_text="Does the author wish to remain anonymous."
	)
    # deleted will be modified by content author.
    deleted = models.BooleanField(
		default=False,
		help_text="Deleted content will not be displayed."
	)
    # flagged will be modified by CUREID editors.
    flagged = models.BooleanField(
		default=False,
		help_text="Flagged content will be hidden until reviewed by CUREID editors."
	)

    class Meta:
        abstract=True


class SaveOldStatus(models.Model):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        setattr(self, '__status', self.status)

    class Meta:
        abstract = True


def fix_title(name):
	"""
		Make sure the first letter is capitalized.
	"""

	if name[0].islower():
		name = f"{name[0].upper()}{name[1:]}"
	return name


def shorten_text(text, length=30):
    """
        Show up to [length] first characters of the text.
        Shorten to the last space before lenght, add '...'
    """
    if not text:
        return ""

    if len(text) > length:
        text = text[:length]
        last_space_ndx = text.rindex(" ") if " " in text else (length - 3)
        return f"{text[:last_space_ndx]}..."
    return text


class CustomQuerySet(models.query.QuerySet):
    def delete(self):
        # TODO: this doesn't trigger signals => ES_dsl doesn't work
        #   have to either change it to use save() or call the signals myself
        self.update(deleted=True)


class NoDeleteManager(models.Manager):
    def get_queryset(self):
        return CustomQuerySet(self.model, using=self._db)


# -------------------------------------------------------------------------------
# Sort classes alphabetically
class Article(CreatedUpdatedModel, SaveOldStatus):
    author = models.ForeignKey(
        CureUser, related_name="articles", on_delete=models.PROTECT, blank=False, null=True
    )
    disease = models.ForeignKey(
        "Disease",
        related_name="articles",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )
    title = models.CharField(
        max_length=1024,
        blank=True,
        null=True,
        verbose_name="Article Title",
        help_text="Article title, up to 255 characters."
    )
    published = models.BooleanField(
        default=False,
        blank=True,
        null=True,
        help_text="Was this article published?"
    )
    publication_name = models.CharField(
        max_length=256,
        blank=True,
        null=True,
        help_text="Journal name the article was published in."
    )
    pubmed_id = models.IntegerField(
        blank=True,
        null=True,
        help_text="PubMed ID of the article."
    )
    doi = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        help_text="DOI code of the article."
    )
    article_url = models.URLField(
        max_length=1096,
        blank=True,
        null=True,
        help_text=""
    )
    pub_year = models.IntegerField(
        blank=True,
        null=True,
        help_text="The article's publication year."
    )
    published_authors = models.TextField(
        max_length=1024,
        blank=True,
        null=True,
        help_text="A list of authors' names. Up to 256 characters."
    )
    publication_type = models.CharField(
        max_length=16,
        choices = ((NEWS, NEWS), (JOURNAL, JOURNAL)),
    )
    article_author_email = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="A list of the article's authors' emails. Up to 256 characters."
    )
    abstract = models.TextField(
        blank=True,
        null=True,
        help_text="An abstract of the article."
    )
    article_type = models.CharField(
        max_length=8,
        blank=True,
        null=True,
        choices=(
            (ORIGINAL, ORIGINAL),
            (REVIEW, REVIEW),
        ),
        help_text="Article type."
    )
    study_type = models.CharField(
        max_length=32,
        blank=True,
        null=True,
        choices=(
            (CASE_REPORT, CASE_REPORT),
            (CASE_SERIES, CASE_SERIES),
            (OBSERVATIONAL_STUDY, OBSERVATIONAL_STUDY),
            (CLINICAL_TRIAL, CLINICAL_TRIAL),
            (OTHER, OTHER),
        ),
        default=CASE_REPORT,
        help_text="Article study type."
    )
    number_of_patients = models.IntegerField(
        blank=True,
        null=True,
        help_text=""
    )
    article_language = models.CharField(
        max_length=2,
        blank=True,
        null=True,
        choices=LANGUAGES
    )
    full_text_available = models.BooleanField(
        blank=True,
        null=True,
        default=False,
        help_text="Is Full Text Available? (Yes or No)."
    )
    # TODO: this is unnecessary here
    attached_images = GenericRelation(
        'AttachedImage',
        related_query_name='images',
        help_text="",
    )
    status = models.CharField(
        max_length=32,
        choices=(
            # Article will be displayed
            (APPROVED, APPROVED),
            # Article needs review
            (SUBMITTED, SUBMITTED),
            # Article has been saved for more edit later before submitting to review
            (SAVED, SAVED),
            # Article was "flagged" by CUREID staff
            (REJECTED, REJECTED),
            # Article was "deleted" by the author or CUREID staff
            (DELETED, DELETED),
        ),
        default=SUBMITTED,
    )

    anonymous = models.BooleanField(default=False)

post_save.connect(create_newsfeed, sender=Article)
post_save.connect(push_notifications, sender=Article)


class AttachedImage(CreatedUpdatedModel):
    # type = ContentType.objects.get_for_model(report)
    # comments = Comment.objects.filter(content_type=type, object_id=report.id)
    content_type = models.ForeignKey(
        ContentType,
        on_delete=models.CASCADE,
        related_name="+",
        null=True,
    )
    object_id = models.IntegerField()
    content_object = GenericForeignKey('content_type', 'object_id')
    url = models.CharField(
        max_length=256,
        null=False,
        blank=False,
        default = ATTACHED_IMAGE_FILLER,
        help_text="The filename that will be displayed. Filler until the file is approved."
    )
    real_name = models.CharField(
        max_length=256,
        null=False,
        blank=False,
        help_text="The filename of the image in the S3."
    )
    reviewer = models.ForeignKey(
        CureUser,
        null=True,
        blank=True,
        on_delete=models.PROTECT,
		help_text="User account that reviewed the image."
    )
    reviewed = models.BooleanField(
        default=False,
        help_text='If the image had already been reviewed'
    )
    caption = models.TextField(
        blank=True,
        null=True,
        help_text='Caption for the image'
    )


class ClinicalTrial(CreatedUpdatedModel, SaveOldStatus):
    author = models.ForeignKey(
        CureUser,
        related_name="clinicaltrials",
        on_delete=models.PROTECT,
        blank=False,
        null=True
    )
    title = models.CharField(
        max_length=4096,
        blank=False,
        help_text="ClinicalTrial title, up to 4096 characters long.",
        null=True
    )
    disease = models.ForeignKey(
        "Disease",
        related_name="clinical_trials",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        help_text=""
    )
    drugs = ArrayField(
        models.CharField(max_length=255),
        max_length=50,
        blank=True,
        null=True,
        help_text="A list of Drugs used. Up to 50 different drugnames, each not longer than 255 characters. Comma separated."
    )
    sponsor = models.CharField(
        max_length=8192,
        blank=True,
        null=True,
        help_text="The CT sponsor name, not longer than 8192 characters."
    )
    interventions = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Interventions, up to 255 characters long."
    )
    ct_status = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Status, up to 255 characters long."
    )
    phase = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Phase, up to 255 characters long."
    )
    locations = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Locations, up to 255 characters long."
    )
    participants = models.PositiveIntegerField(
        blank=True,
        null=True,
        help_text="Number of participants in the CT.",
    )
    start_year = models.PositiveIntegerField(
        null=True,
        blank=True
    )
    # TODO: check all values can be inserted
    country = models.CharField(
        max_length=44,
        blank=True,
        null=True,
        choices=COUNTRIES,
        help_text="Which country the CT was executed in.",
    )
    # TODO: all ct_ids in PROD are 11 char long, ask Heather whether there can
    #   be other ones
    # TODO: can there be several CTs with the same CT_gov_id?
    clinical_trials_gov_id = models.CharField(max_length=15)
    # TODO: what's the difference between "Banned", "Deleted", "Flagged", etc in different models?
    deleted = models.BooleanField(default=False)
    # If the CT was created by quering ClinicalTrials.gov's updates,
    #  matched_against will hold the value of "Conditions". This
    #  will allow Django admins to easier catch mistakes.
    matched_against = models.CharField(
        max_length=4096,
        blank=True,
        null=True,
        default="",
        help_text="Value in [Conditions] or [Title] matched with the disease."
    )
    # TODO: needs to be deleted .. after we have tests in place
    attached_images = GenericRelation(
        AttachedImage,
        related_query_name='images',
        help_text="",
    )
    status = models.CharField(
        max_length=32,
        choices=(
            # CT will be displayed
            (APPROVED, APPROVED),
            # CT needs review
            (SUBMITTED, SUBMITTED),
            # Doesn't really make sense with CTs
            (SAVED, SAVED),
            # CT was "flagged" by CUREID staff; not much sense currently
            (REJECTED, REJECTED),
            # CT was deleted by the author or CUREID staff
            (DELETED, DELETED),
        ),
        default=SUBMITTED,
    )

    class Meta:
        unique_together = ("disease", "clinical_trials_gov_id")

    def __str__(self):
        return shorten_text(self.title, 100)

# TODO: currently there's no code to send notifications for CT
#post_save.connect(send_notification_for_disease_oi, sender=ClinicalTrial)
post_save.connect(create_newsfeed, sender=ClinicalTrial)
post_save.connect(push_notifications, sender=ClinicalTrial)


# TODO: check performance with comments fetching, creating, etc
# TODO: how to fetch comments in the correct order in one DB query?
class Comment(BasePostModel):
    # type = ContentType.objects.get_for_model(report)
    # comments = Comment.objects.filter(content_type=type, object_id=report.id)
    content_type = models.ForeignKey(
        ContentType,
        on_delete=models.CASCADE,
        related_name="comments",
        null=True,
    )
    object_id = models.IntegerField()
    # TODO: test whether this object values update when the object this points
    #   to is updated
    content_object = GenericForeignKey('content_type', 'object_id')
    parent = models.ForeignKey(
        "self",
        blank=True,
        null=True,
        on_delete=models.CASCADE,
        help_text="This will point to another comment which the current one is commenting on."
    )
    attached_images = GenericRelation(
        AttachedImage,
        related_query_name='images',
        help_text="",
    )

    objects = NoDeleteManager()

    def __str__(self):
        return f"{shorten_text(self.body)} by {self.author.first_name} {self.author.last_name}"

    def is_top_comment(self):
        """ One that doesn't have a parent. """
        return not self.parent

    def save(self, *args, **kwargs):
        if self.content_object.__class__ == Discussion:
            if self.author == self.content_object.author and self.content_object.anonymous:
                self.anonymous = True
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self.deleted = True
        super().save(*args, **kwargs)

post_save.connect(create_newsfeed, sender=Comment)
post_save.connect(comment_on_object, sender=Comment)


class Comorbidity(models.Model):
    label = models.CharField(
        max_length=64
    )
    value = models.CharField(
        max_length=64
    )


    class Meta:
        verbose_name_plural = 'Comorbidities'

    def __str__(self):
        return self.value


class Discussion(BasePostModel, SaveOldStatus):
    title = models.CharField(max_length=1024)
    disease = models.ForeignKey(
        "Disease",
        related_name="discussions",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )
    # TODO: this is unnecessary too
    attached_images = GenericRelation(
        AttachedImage,
        related_query_name='images',
        help_text="",
    )
    # TODO: is this field necessary? I doubt if comments can point to anything.
    comments = GenericRelation(
        Comment,
        related_query_name='comments',
        help_text="",
    )
    status = models.CharField(
        max_length=32,
        choices=(
            # Discussion will be displayed
            (APPROVED, APPROVED),
            # Discussion needs review
            (SUBMITTED, SUBMITTED),
            # Doesn't really make sense with Discussions
            (SAVED, SAVED),
            # Discussion was "flagged" by CUREID staff
            (REJECTED, REJECTED),
            # Discussion was deleted by the author or CUREID staff
            (DELETED, DELETED),
        ),
        default=SUBMITTED,
    )

    def __str__(self):
        return f"{shorten_text(self.title)} by {self.author.first_name} {self.author.last_name}"

    def save(self, *args, **kwargs):
        self.title = fix_title(self.title)
        super().save(*args, **kwargs)

#post_save.connect(send_notification_for_disease_oi, sender=Discussion)
post_save.connect(create_newsfeed, sender=Discussion)
post_save.connect(push_notifications, sender=Discussion)


class Disease(models.Model):
    name = models.CharField(max_length=128, unique=True)
    image_name = models.CharField(max_length=128, blank=True, null=True)
	# TODO: !! changed name from MedDRA
	# TODO: is this name of the disease in MedDRA.org????
    # TODO: must it be unique?
    meddra = models.CharField(
        max_length=128,
        default="NONE",
        help_text=""
    )
	# TODO: looks like an old version of organism
    transmitted_by = models.CharField(max_length=512, blank=True)
    # TODO: this used to be a manytomany relationship, ask dominic why he changed it
    synonyms = ArrayField(
		models.CharField(max_length=64),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Up to 16 other names of this disease. Comma separated.",
	)
    syndromes = ArrayField(
		models.CharField(max_length=64),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Up to 16 syndromes of this disease. Comma separated.",
	)
    # TODO: what's the difference between it and synonyms?
    linkterms = ArrayField(
		models.CharField(max_length=64),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Up to 16 linkterms of this disease. Comma separated.",
	)
    # TODO: field treatments is blank for all diseases in PROD, skipping this field
    fda_approved_drugs = models.ManyToManyField(
        "Drug",
        blank=True,
        related_name="fda_approved_for_disease",
        help_text="Drugs that FDA approved to be used with this disease."
    )
    organisms = models.ManyToManyField(
        "Organism",
        blank=True,
        related_name="disease",
        help_text="Organisms that are believed to cause the Disease",
    )

    class Meta:
        ordering = ("name",)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        self.name = fix_title(self.name)
        super().save(*args, **kwargs)


class Drug(models.Model):
    name = models.CharField(
        max_length=100,
        unique=True,
        help_text=''
    )
    # TODO: !! changed from rxNorm_id
    rxnorm_id = models.IntegerField(
        blank=True,
        null=True,
        unique=True,
        help_text=''
    )
    is_tuberculosis_resistant = models.BooleanField(
        default=False, blank=True, null=True
    )

    class Meta:
        ordering = ("name",)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        self.name = fix_title(self.name)
        super().save(*args, **kwargs)


class Event(CreatedUpdatedModel, SaveOldStatus):
    author = models.ForeignKey(
        CureUser, related_name="events", on_delete=models.PROTECT, blank=False, null=True
    )
    title = models.CharField(
		max_length=1024,
		blank=False,
        null=True,
		help_text=""
	)
    disease = models.ForeignKey(
        Disease,
		related_name="events",
		on_delete=models.PROTECT,
        null=True,
		help_text=""
    )
    contact = models.CharField(
		max_length=128,
		null=True,
		blank=True,
		help_text=""
	)
    event_description = models.TextField(
		null=True,
		blank=True,
		help_text=""
	)
    event_sponsor = models.CharField(
		max_length=256,
		null=True,
		blank=True,
		help_text=""
	)
    location = models.CharField(
        max_length=128,
		null=True,
		blank=True,
		help_text=""
    )
    event_start = models.CharField(
        max_length=32,
        null=True,
        blank=True
    )
    event_start_time = models.CharField(
        max_length=32,
        null=True,
        blank=True
    )
    event_end = models.CharField(
        max_length=32,
        null=True,
        blank=True
    )
    event_end_time = models.CharField(
        max_length=32,
        null=True,
        blank=True
    )
    url = models.URLField(max_length=1024, blank=True, null=True)
    status = models.CharField(
        max_length=32,
        choices=(
            # Event will be displayed
            (APPROVED, APPROVED),
            # Event needs review
            (SUBMITTED, SUBMITTED),
            # Event has been saved for more edit later before submitting to review
            (SAVED, SAVED),
            # Event was "flagged" by CUREID staff
            (REJECTED, REJECTED),
            # Event was "deleted" by the author or CUREID staff
            (DELETED, DELETED),
        ),
        default=SUBMITTED,
    )

    def __str__(self):
        return self.title

#post_save.connect(send_notification_for_disease_oi, sender=Event)
post_save.connect(create_newsfeed, sender=Event)
post_save.connect(push_notifications, sender=Event)


# TODO: I think this should be connected to the Report,
#   though not all Reports will be for pregnant patients
# TODO: What are the possible values for each field
class Neonate(CreatedUpdatedModel):
    pregnancy = models.ForeignKey(
        "Pregnancy",
        related_name="neonates",
        on_delete=models.CASCADE,
        help_text="",
    )
    diagnosed_with_disease = models.CharField(
        max_length=16,
        blank=True,
        null=True,
        choices=(
            (YES, YES),
            (NO, NO),
            (NOT_TESTED, NOT_TESTED)
        ),
    )
    abnormalities_or_defects = models.TextField(
        blank=True,
        null=True,
        help_text=""
    )
    other_outcome_details = models.TextField(
        blank=True,
        null=True,
        help_text=""
    )


class Newsfeed(CreatedUpdatedModel):
    action = models.CharField(
        max_length=25,
        blank=False,
        default="created",
        help_text="Can be 'created' or 'commented on'."
    )
    content_type = models.ForeignKey(
        ContentType,
        on_delete=models.CASCADE,
        related_name="+",
        null=True,
    )
    object_id = models.IntegerField()
    content_object = GenericForeignKey('content_type', 'object_id')
    pinned = models.BooleanField(default=False)


class LinkedAccount(models.Model):
    """ Firebase account details linked to CUREID user account.
        It is possible that users login into CUREID using different accounts
        through Firebase service. All those user accounts can be linked to
        the same CUREID user account.
    """
    # TODO: what happens to existing profiles, existent content, etc?
    user = models.ForeignKey(
        User,
        on_delete=models.PROTECT,
        related_name="linked_accounts",
        help_text="CUREID User account linked to this Firebase account."
    )
    uid = models.CharField(
        max_length=32,
        null=False,
        help_text="User ID used in Firebase."
    )
    provider = models.CharField(
        max_length=16,
        null=False,
        help_text="Name of the service used to log in."
    )

    def __str__(self):
        return f"{self.provider}: {self.user.first_name} {self.user.last_name} - {self.uid}"

class Organism(models.Model):

    name = models.CharField(max_length=64, unique=True)

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        self.name = fix_title(self.name)
        super().save(*args, **kwargs)

class Patient(CreatedUpdatedModel):
    # TODO: not useful field, cause if the same Patient is used in different Reports/Articles
    #   several years appart this field will not be true.
    age = models.IntegerField(
        blank=True,
        null=True,
        help_text="The exact age of the patient at the time of the Report."
    )
    age_group = models.CharField(
        max_length=11,
        blank=True,
        choices=(
            (Q6_C1, Q6_C1),
            (Q6_C2, Q6_C2),
            (Q6_C3, Q6_C3),
            (Q6_C4, Q6_C4),
            (Q6_C5, Q6_C5),
            (Q6_C6, Q6_C6),
            (Q6_C7, Q6_C7),
            (Q6_C8, Q6_C8),
            (Q6_C9, Q6_C9),
            (Q6_C10, Q6_C10),
            (Q6_C11, Q6_C11),
            (Q6_C12, Q6_C12),
            (Q6_C13, Q6_C13),
        ),
        null=True,
        help_text="",
    )
    ethnicity = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        # TODO: why only these two options?
        choices=(
            (HISPANIC,      HISPANIC),
            (NON_HISPANIC,  NON_HISPANIC),
            (UNKNOWN,       UNKNOWN),
            (NA,            NA)
        ),
        help_text=""
    )
    sex = models.CharField(
        max_length=64,
        blank=True,
        choices=(
            (MALE, MALE),
            (FEMALE, FEMALE),
            (INTERSEX, INTERSEX),
            (OTHER, OTHER),
            (NOT_SPECIFIED, NOT_SPECIFIED),
        ),
        null=True,
        help_text="",
    )

    race = ArrayField(
		models.CharField(max_length=75),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Race(s) of the patient",
	)

# TODO: I need to know what values this field will hold
    comorbidity = models.ManyToManyField(
        "Comorbidity",
        blank=True,
        related_name="comorbidities",
        help_text=""
    )

    pregnant = models.BooleanField(
        default=False,
        help_text="Was the patient pregnant at the time of test."
    )
    pregnancy = models.OneToOneField(
        "Pregnancy",
        blank=True,
        null=True,
        related_name="+",
        on_delete=models.PROTECT,
        help_text=""
    )
    other_coinfections = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        help_text="Up to 255 characters long."
    )

    def __str__(self):
        return f"Age group={self.age_group}, sex={self.sex}, pregnant={self.pregnant}"


class PhotoCredit(models.Model):
    disease = models.OneToOneField(
        Disease,
        related_name="+",
        on_delete=models.CASCADE,
        unique=True,
        help_text=""
    )
    title = models.CharField(
        max_length=1024,
        help_text="Title of the PhotoCredit. Up to 1024 characters long."
    )
    author = models.CharField(
        max_length=1024,
        help_text="Author of the photo."
    )
    link = models.CharField(
        max_length=1024,
        help_text=""
    )

    def __str__(self):
        return f"{self.title} by {self.author} (for {self.disease.name})"


# TODO: WHAT ARE THE POSSIBLE VALUES FOR EACH FIELD
class Pregnancy(CreatedUpdatedModel):
    treatment_gestational_age = models.CharField(
        max_length=16,
        blank=True,
        null=True,
        choices=GESTATIONAL_AGE_WEEKS,
        help_text="The gestational age in week at the time of the treatment."
    )
    delivery_gestational_age = models.CharField(
        max_length=16,
        blank=True,
        null=True,
        choices=GESTATIONAL_AGE_WEEKS,
        help_text=""
    )
    outcome = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        help_text=""
    )

    class Meta:
        verbose_name_plural = 'Pregnancies'

    def __str__(self):
        return f"{self.outcome}"


class Profile(CreatedUpdatedModel):
    user = models.OneToOneField(
        User,
        related_name='profile',
        on_delete=models.CASCADE,
        help_text='',
    )
    profile_image = GenericRelation(
        AttachedImage,
        related_query_name='profile_image',
        help_text="",
    )

    title = models.CharField(
    	max_length=4,
    	blank=True,
    	choices=(
        	(MR,  MR),
        	(MRS, MRS),
        	(MS,  MS),
        	(DR,  DR)
    	),
    	help_text='',
    )
    qualification = models.CharField(
        max_length=30,
        choices=(
            (RESEARCHER,                    RESEARCHER),
            (MICROBIOLOGIST,                MICROBIOLOGIST),
            (PHARMACOLOGIST,                PHARMACOLOGIST),
            (IMMUNOLOGIST,                  IMMUNOLOGIST),
            (SOCIAL_WORKER,                 SOCIAL_WORKER),
            (EPIDEMIOLOGIST,                EPIDEMIOLOGIST),
            (HEALTH_POLICY_SPECIALIST,      HEALTH_POLICY_SPECIALIST),
            (REGULATOR,                     REGULATOR),
            (LAWYER,                        LAWYER),
            (INFORMATICIST_DATA_SCIENTIST,  INFORMATICIST_DATA_SCIENTIST),
            (MEDICAL_DOCTOR,                MEDICAL_DOCTOR),
            (NURSE_PRACTITIONER,            NURSE_PRACTITIONER),
            (PHYSICIAN_ASSISTANT,           PHYSICIAN_ASSISTANT),
            (NURSE,                         NURSE),
            (COMMUNITY_HEALTH_WORKER,       COMMUNITY_HEALTH_WORKER),
            (PHARMACIST,                    PHARMACIST),
            (PUBLIC_HEALTH_SPECIALIST,      PUBLIC_HEALTH_SPECIALIST),
            (OTHER_HEALTHCARE_PROFESSIONAL, OTHER_HEALTHCARE_PROFESSIONAL),
            (PATIENT,                       PATIENT),
            (PARENT,                        PARENT),
            (CAREGIVER,                     CAREGIVER),
            (COMMUNITY_MEMBER,              COMMUNITY_MEMBER),
            (ADVOCATE,                      ADVOCATE),
            (CURE_ADMIN,                    CURE_ADMIN)
        ),
        blank=False,
        default=OTHER_HEALTHCARE_PROFESSIONAL,
        help_text=''
    )
    institution = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text=''
    )
    specialty = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text=''
    )
    terms_and_conditions = models.BooleanField(
        default=False,
        help_text='Has the account accepted the CUREID terms and conditions?',
    )
    # TODO: check all values can be inserted
    country = models.CharField(
        max_length=44,
        blank=True,
        choices=COUNTRIES,
        help_text='Which country does the user live in.',
    )
    status = models.CharField(
        max_length=20,
        choices=(
            (ACTIVE, ACTIVE),
            (PAUSED, PAUSED),
            (BANNED, BANNED)
        ),
        default=ACTIVE,
        help_text='This can be used to disable the user.',
    )
    # TODO: check values to/from DRF serializers
    # TODO: settings need to be reviewed, many renamed, some removed
    #
    # profile.notifications = {"email_notifications": True, "email_period": "weekly"}
    #
    # ex.:
    #  {
    #    // should email notifications be sent to the user
    #    "email_notifications": true/false,
    #    // how often to send email digests
    #    "email_period": "weekly/daily",
    #    // should push notifications be sent to the user
    #    "push_notifications": true/false,
    #    // does the user want to have "quiet time"
    #    "quiet_time": true/false,
    #    "quiet_time_start": "10:30 AM",
    #    "quiet_time_end": "12:30 PM",
    #    // should notifications about comments to your case be sent to you
    #    "comment_on_your_case": true/false,
    #    // should notifications about comments to your discussion be sent to you
    #    "replied_to_your_discussion_post": true/false,
    #    // should notifications about your case being approved be sent to you
    #    "approved_publish_case": true/false,
    #    // should not-s about a report posted for the disease you're interested in be sent to you
    #    "case_posted_you_are_interested_in": true/false,
    #    // should reminders to finish a report you've started be sent to you
    #    "finish_case_report_reminder": true/false,
    #    // should the email digests include news about all diseases or only the ones you're interested in
    #    "get_notifications_for_all_diseases": true/false,
    #    // stop all notifications
    #    "notifications_do_not_disturb": true/false,
    #    // receive email notifications about all reports
    #    "notification_case_all": true/false|false,
    #    // receive push notifications about all reports
    #    "notification_case_all_push": true/false|false,
    #    //TODO: the 2 case_favor don't make much sense
    #    // receive email notifications about favorited reports
    #    "notification_case_favor": true/false|false,
    #    "notification_case_favor_push": true/false|false,
    #    // reveive email/push notifications about all discussions
    #    "notification_post_all": true/false|false,
    #    "notification_post_all_push": true/false|false,
    #    //TODO: these make more sense, but then having comments settings are not required. or these two.
    #    // receive email/push notifications about favorited discussions
    #    "notification_post_favor": true/false|false,
    #    "notificatoin_post_favor_push": true/false|false,
    #    "notification_comment_all": true/false|false,
    #    "notification_comment_all_push": true/false|false,
    #    "notification_comment_favor": true/false|false,
    #    "notification_comment_favor_push": true/false|false,
    #  }
    notifications = JSONField(
        null=True,
        help_text="JSON array of notification settings."
    )
    # TODO: !! changed name from 'drugs_interest'
    favorited_drugs = models.ManyToManyField(
        Drug,
        blank=True,
        related_name="profiles",
        help_text="List of drugs the user is interested in.",
    )
    # TODO: !! changed name from 'diseases_interest'
    favorited_diseases = models.ManyToManyField(
        Disease,
        blank=True,
        related_name="profiles",
        help_text="List of diseases the user is interested in.",
    )
    favorited_discussions = models.ManyToManyField(
        Discussion,
        blank=True,
        related_name="profiles",
        help_text='List of discussions the user is interested in.',
    )
    favorited_reports = models.ManyToManyField(
        "Report",
        blank=True,
        related_name="profiles",
        help_text="List of reports the user is interested in.",
    )
    favorited_clinical_trials = models.ManyToManyField(
        "ClinicalTrial",
        blank=True,
        related_name="profiles",
        help_text="List of ClinicalTrials the user is interested in.",
    )
    favorited_articles = models.ManyToManyField(
        Article,
        blank=True,
        related_name="profiles",
        help_text="List of Articles the user is interested in.",
    )
    favorited_events = models.ManyToManyField(
        Event,
        blank=True,
        related_name="profiles",
        help_text="List of Events the user is interested in.",
    )

    #storing comment ids only used Generic Relation, add() worked but remove() didn't work
    liked_comments = ArrayField(
		models.PositiveIntegerField(null=True, blank=True),
        null=True,
        blank=True,
        default=list,
	)
    # TODO: do we need middle_name?

    def __str__(self):
        return f"self.user.username ({self.user.first_name} {self.user.last_name})"

class Regimen(CreatedUpdatedModel):
    drug = models.ForeignKey(
        Drug,
        related_name="+",
        on_delete=models.PROTECT,
        help_text=""
    )
    report = models.ForeignKey(
        "Report",
        related_name="regimens",
        on_delete=models.PROTECT,
        help_text=""
    )
    dose = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text=""
    )
    frequency = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text=""
    )
    route = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text=""
    )
    # TODO: including Duration inside Regimen, cause they are 1-to-1 anyway
    # TODO: start_date and end_date are not displayed to CUREID users,
    #   do we need them?
    start_date = models.DateField(
        blank=True,
        null=True,
        help_text="Date when the regimen started. Can be blank/unknown."
    )
    end_date = models.DateField(
        blank=True,
        null=True,
        help_text="Date when the regimen ended. Can be blank/unknown."
    )
    duration = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Free text for how long the regimen was administered. Can be blank."
    )
    # TODO: since we don't show start/end_date field "dates_unknown" will ALWAYS be True
    #   not including it here.

    # TODO: currently `severity` is in Report, why did Dominic include it in Regimen?
    severity = models.CharField(
        max_length=17,
        blank=True,
        null=True,
        choices=(
            (OUTPATIENT,        OUTPATIENT),
            (INPATIENT,         INPATIENT),
            (ICU_CRITICAL_CARE, ICU_CRITICAL_CARE),
        ),
        verbose_name="Treatment Setting (per regimen)",
        help_text=""
    )
    severity_detail = models.CharField(
        max_length=4096,
        blank=True,
        null=True,
        verbose_name="Treatment Setting Details",
        help_text=""
    )
    use_drug = ArrayField(
		models.CharField(max_length=128),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Use drug",
	)
    # TODO: "comments" is empty for all regimen in prod, not including ito

    def __str__(self):
        return f"{self.id}: {self.drug}"


class CureReport(CreatedUpdatedModel, SaveOldStatus):
    author = models.ForeignKey(
        #"auth.User", related_name="cure_reports", on_delete=models.PROTECT, blank=False, null=False
        CureUser, related_name="cure_reports", on_delete=models.PROTECT, blank=False, null=False
    )

    report = models.OneToOneField(
        "Report",
        related_name="reports", blank=False, null=False, on_delete=models.CASCADE)

    anonymous = models.BooleanField(default=False)
    flagged= models.BooleanField(default=False)
    reminder = models.BooleanField(default=False)
    when_reminder = models.CharField(
        max_length=16,
        blank=True,
        choices=(
            (NO_REMINDER, NO_REMINDER),
            (ONE_WEEK, ONE_WEEK),
            (TWO_WEEKS, TWO_WEEKS),
            (THREE_WEEKS, THREE_WEEKS),
            (ONE_MONTH, ONE_MONTH),
            (THREE_MONTHS, THREE_MONTHS),
            (SIX_MONTHS, SIX_MONTHS),
            (ONE_YEAR, ONE_YEAR),
        )
    )

    status = models.CharField(
        max_length=32,
        choices=(
            (SUBMITTED, SUBMITTED),
            (SAVED, SAVED),
            (APPROVED, APPROVED),
            (REJECTED, REJECTED),
            (DELETED, DELETED),
        ),
        default=SAVED,
    )

    report_type = models.CharField(
        max_length=16,
        default='',
        null=True,
        blank=True
    )

    is_author = models.BooleanField(default=False)

# post_save.connect(send_notification_for_disease_oi, sender=CureReport)
post_save.connect(create_newsfeed, sender=CureReport)
post_save.connect(push_notifications, sender=CureReport)


class Report(models.Model):
    article = models.OneToOneField(
        Article,
        # Can there be many reports from one article? Then why is this OneToOne?
        related_name="report_articles",
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        help_text="",
    )
    patient = models.OneToOneField(
        Patient,
        # TODO: Makes no sense, since it's One2One
        related_name="+",
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        help_text=""
    )
    # other Report fields
    disease = models.ForeignKey(
        Disease,
        # TODO: do we use this one anywhere?
        related_name="reports",
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        help_text="What disease did your patient have?"
    )
    cross_linked_diseases = models.ManyToManyField(
        Disease,
        related_name="cross_linked_reports",
        blank=True,
        null=True,
        help_text="",
    )
    drugs = models.ManyToManyField(
       Drug,
       related_name="reports",
       through="core.Regimen",
       blank=True,
       help_text="What drug(s) did you use in a new way?"
    )
    organisms = models.ManyToManyField(
        Organism,
        related_name="reports_organisms",
        blank=True,
        help_text=""
    )
    outcome = models.CharField(
        max_length=128,
        blank=True,
        choices=(
            (PATIENT_WAS_CURED,             PATIENT_WAS_CURED),
            (PATIENT_IMPROVED,              PATIENT_IMPROVED),
            (PATIENT_CONDITION_UNCHANGED,   PATIENT_CONDITION_UNCHANGED),
            (PATIENT_DETERIORATED,          PATIENT_DETERIORATED),
            (PATIENT_DIED,                  PATIENT_DIED),
            (TREATMENT_TERMINATED,          TREATMENT_TERMINATED),
            (UNKNOWN_OUTCOME,               UNKNOWN_OUTCOME),
        ),
        help_text="What was the patient's outcome?"
    )
    surgery = models.CharField(
        max_length=16,
        blank=True,
        choices=(
            (YES,           YES),
            (NO,            NO),
            (IDK_UNKNOWN,   IDK_UNKNOWN)
        ),
        help_text="Did the patient have surgery, if applicable?"
    )
    country_contracted = models.CharField(
        max_length=44,
        blank=True,
        null=True,
        choices=COUNTRIES,
        help_text="Country disease was contracted in."
    )
    country_treated = models.CharField(
        max_length=44,
        blank=True,
        null=True,
        choices=COUNTRIES,
        help_text="Country disease was treated in."
    )
    began_treatment_year = models.CharField(
        max_length=20,
        blank=True,
        help_text="Patient Began Treatment with this regimen in (year)."
    )
    site_of_disease = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        verbose_name="Site of Infection",
        help_text="Site of infection."
    )
    site_of_tuberculosis_infection = ArrayField(
		models.CharField(max_length=64),
		max_length=4,
		blank=True,
		null=True,
		default=list,
		help_text="site of infection",
	)
    clinical_syndrome = models.CharField(
        max_length=255,
        blank=True,
        help_text=""
    )
    unusual = models.TextField(max_length=4096, blank=True)
    when_outcome = models.CharField(
            max_length=128,
            blank=True,
            choices=((AFTER, AFTER), (AT_COMPLETED, AT_COMPLETED), (WHILE, WHILE)),
        )
    how_outcome = ArrayField(
		models.CharField(max_length=64),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Diagnosis",
	)
    relapse = models.CharField(
        max_length=16,
        blank=True,
        null=True,
        choices=((YES, YES), (NO, NO), (IDK_UNKNOWN, IDK_UNKNOWN)),
    )
    adverse_events = models.TextField(max_length=4096, blank=True)
    have_adverse_events = models.BooleanField(
        verbose_name="Did the patient experience any adverse events?", default=False
    )
    resistant_drugs = models.ManyToManyField(
        "Drug", blank=True, related_name="report_resistant_drugs"
    )
    previous_drugs = models.ManyToManyField(
        "Drug",
        blank=True,
        related_name="report_previous_drugs"
    )
    how_diagnosis = ArrayField(
		models.CharField(max_length=1096),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Diagnosis",
	)
    why_new_way = ArrayField(
		models.CharField(max_length=1096),
		max_length=16,
		blank=True,
		null=True,
		default=list,
		help_text="Treatment challenges",
	)
    adverse_events_outcome = models.CharField(
        max_length=64,
		blank=True,
		null=True,
		help_text="Adverse events outcome"
    )

    sample = ArrayField(
        models.CharField(max_length=128),
        max_length=16,
        blank=True,
        null=True,
        default=list,
        help_text='Sample information'
    )

    extrapulmonary_site = ArrayField(
		models.CharField(max_length=32),
		max_length=8,
		blank=True,
		null=True,
		default=list,
		help_text="Extra pulmonary site",
	)
    additional_info = models.TextField(
        blank=True,
        null=True
    )
    outcome_followup = models.CharField(
        blank=True,
        max_length=64,
        null="True",
        help_text="Outcome timing"
    )

    extra_fields = JSONField(
        blank=True,
        null=True,
        help_text="JSON array for any additional report fields.",
        default=dict
    )

    attached_images = GenericRelation(
        AttachedImage,
        related_query_name='images',
        help_text="",
    )

    @property
    def percentage_completed(self):
        fields = Report._meta.fields
        values = [getattr(self, field.name, None) for field in fields]
        completed = sum(x not in [None, ''] for x in values)
        return round(((completed/len(fields)) * 100))


class UnseenNews(models.Model):
    user = models.ForeignKey(
        "auth.User", related_name="unseennews", on_delete=models.PROTECT
    )
    newsfeed = models.ForeignKey(
        "Newsfeed", related_name="+", on_delete=models.CASCADE,
    )
    pushnotification = models.BooleanField(default=False)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.newsfeed.object_type}[{self.newsfeed.object_id}] for {self.user}"


class UserProposedArticle(models.Model):
    pubmed_id = models.IntegerField(
        blank=True,
        null=True,
        help_text="PubMed ID of the article."
    )
    article_url = models.URLField(
        max_length=1096,
        blank=True,
        null=True,
        help_text=""
    )
    author = models.ForeignKey(
        #"auth.User", related_name="cure_reports", on_delete=models.PROTECT, blank=False, null=False
        CureUser,
        related_name="proposed_articles",
        on_delete=models.PROTECT,
        blank=False,
        null=False
    )
    status = models.CharField(
        max_length=32,
        choices=(
            # Article needs review
            (SUBMITTED, SUBMITTED),
            # Article was "flagged" by CUREID staff or found not applicable
            (REJECTED, REJECTED),
        ),
        default=SUBMITTED,
    )


    @property
    def needs_review(self):
        if self.status == REJECTED:
            return False

        if self.pubmed_id and Article.objects.filter(pubmed_id=self.pubmed_id):
            return False

        if self.article_url and Article.objects.filter(article_url=self.article_url):
            return False

        return True


    def save(self, *args, **kwargs):
        if self.pubmed_id and len(f"{self.pubmed_id}") > 10:
            raise ValidationError('The pubmed_id value is too big.', code='invalid')

        if not self.pubmed_id or (type(self.pubmed_id) == str and not self.pubmed_id.isnumeric()):
            self.pubmed_id = None

        if not self.pubmed_id and not self.article_url:
            raise ValidationError('Both pubmed_id and article_url cannot be blank', code='invalid')

        similar = 0
        if self.pubmed_id:
            numbers = UserProposedArticle.objects.filter(pubmed_id=self.pubmed_id).count()
            if self.id:
                numbers -= 1
            similar += numbers + Article.objects.filter(pubmed_id=self.pubmed_id).count()
        if self.article_url:
            numbers = UserProposedArticle.objects.filter(article_url=self.article_url).count()
            if self.id:
                numbers -= 1
            similar += numbers + Article.objects.filter(article_url=self.article_url).count()
        if similar:
            # No idea what message to show them here
            raise ValidationError('Thank you for the suggestion. ', code='invalid')

        super().save(*args, **kwargs)
