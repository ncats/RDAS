from django.conf import settings
from django_elasticsearch_dsl import Document, fields, Keyword
from django_elasticsearch_dsl.registries import registry
from django.contrib.contenttypes.models import ContentType
from django.contrib.auth.models import User

from server.apps.api.models import LogRequest
from server.apps.core.constants import APPROVED, PATIENT_WAS_CURED, PATIENT_IMPROVED, PATIENT_CONDITION_UNCHANGED, PATIENT_DETERIORATED, UNKNOWN_OUTCOME, TREATMENT_TERMINATED, PATIENT_DIED
from server.apps.core.models import (
    Article,
    Event,
    Drug,
    Disease,
    ClinicalTrial,
    Discussion,
    Comment,
    Report,
    CureReport,
    Profile,
)

#@registry.register_document
class EventDocument(Document):
    disease = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'name': fields.TextField(),
            'url': fields.TextField(),
        }
    )
    author = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'first_name': fields.TextField(),
            'last_name': fields.TextField(),
            'qualification': fields.TextField(),
        }
    )
    comment_count = fields.IntegerField()
    comment_latest = fields.ObjectField(
        properties={
            'body': fields.TextField(),
        }
    )
    comment_authors = fields.NestedField(
        properties={
            "name": fields.TextField(),
        }
    )

    def prepare_author(self, instance):
        data = {
            "id": instance.author.id,
            "first_name": instance.author.first_name,
            "last_name": instance.author.last_name,
            "qualification": instance.author.profile.qualification,
        }
        return data

    def prepare_comment_count(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        comment_count = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).count()
        return comment_count

    def prepare_comment_latest(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        latest_comment = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).order_by('updated', 'created').last()
        data = {
            "body": "",
        }
        if latest_comment:
            data["body"] = latest_comment.body
        return data

    def prepare_comment_authors(self, instance):
        data = []
        content_type = ContentType.objects.get_for_model(instance)
        comments = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False)
        for comment in comments:
            author = f"{comment.author.first_name} {comment.author.last_name}"
            data.append({"name": author})
        return data

    class Index:
        name = 'events'

    class Django:
        model = Event
        fields = [ "title", "contact", "event_description", "event_sponsor",
                "location", "event_start", "event_start_time", "event_end",
                "event_end_time", "url", "status" ]
        related_models = [ Disease, User, Profile, Comment ]

    def get_queryset(self):
        qs = super().get_queryset().filter(status=APPROVED).select_related('author', 'disease')
        return qs

    def get_instances_from_related(self, related_instance):
        if isinstance(related_instance, Disease):
            return related_instance.events.filter(status=APPROVED)
        #elif isinstance(related_instance, User):
        #    return related_instance.events.filter(status=APPROVED)
        elif isinstance(related_instance, Profile):
            return related_instance.user.events.filter(status=APPROVED)
        elif isinstance(related_instance, Comment):
            return Event.objects.filter(status=APPROVED, id=related_instance.object_id)


#@registry.register_document
class ArticleDocument(Document):
    disease = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'name': fields.TextField(),
            'url': fields.TextField(),
        }
    )
    author = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'first_name': fields.TextField(),
            'last_name': fields.TextField(),
            'qualification': fields.TextField(),
        }
    )
    comment_count = fields.IntegerField()
    comment_latest = fields.ObjectField(
        properties={
            'body': fields.TextField(),
        }
    )
    comment_authors = fields.NestedField(
        properties={
            "name": fields.TextField(),
        }
    )
    name = fields.TextField()

    def prepare_name(self, instance):
        return instance.publication_name

    def prepare_author(self, instance):
        data = {
            "id": instance.author.id,
            "first_name": instance.author.first_name,
            "last_name": instance.author.last_name,
            "qualification": instance.author.profile.qualification,
        }
        return data

    def prepare_comment_count(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        comment_count = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).count()
        return comment_count

    def prepare_comment_latest(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        latest_comment = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).order_by('updated', 'created').last()
        data = {
            "body": "",
        }
        if latest_comment:
            data["body"] = latest_comment.body
        return data

    def prepare_comment_authors(self, instance):
        data = []
        content_type = ContentType.objects.get_for_model(instance)
        comments = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False)
        for comment in comments:
            author = f"{comment.author.first_name} {comment.author.last_name}"
            data.append({"name": author})
        return data

    class Index:
        name = 'articles'

    class Django:
        model = Article
        fields = [ "title", "published", "pubmed_id", "doi", "article_url",
                "pub_year", "published_authors", "publication_type", "article_author_email",
                "abstract", "article_type", "study_type", "number_of_patients",
                "article_language", "full_text_available", "status"]
        related_models = [ Disease, User, Profile, Comment ]

    def get_queryset(self):
        qs = super().get_queryset().filter(status=APPROVED).select_related('author', 'disease')
        return qs

    def get_instances_from_related(self, related_instance):
        if isinstance(related_instance, Disease):
            return related_instance.articles.filter(status=APPROVED)
        #elif isinstance(related_instance, User):
        #    return related_instance.articles.filter(status=APPROVED)
        elif isinstance(related_instance, Profile):
            return related_instance.user.articles.filter(status=APPROVED)
        elif isinstance(related_instance, Comment):
            return Article.objects.filter(status=APPROVED, id=related_instance.object_id)


#@registry.register_document
class DrugDocument(Document):
    url = fields.TextField()

    def prepare_url(self, instance):
        url = "%s%sdrugs/%s" % (
            settings.API_SUB_DOMAIN,
            settings.API_DOMAIN,
            instance.id
        )
        return url

    class Index:
        name = 'drugs'

    class Django:
        model = Drug
        fields = [ 'name', 'rxnorm_id', 'is_tuberculosis_resistant' ]


@registry.register_document
class DiseaseDocument(Document):
    synonyms = fields.NestedField(properties={"key": fields.TextField()})
    url = fields.TextField()
    discussion_count = fields.IntegerField()
    report_count = fields.IntegerField()
    trial_count = fields.IntegerField()
    event_count = fields.IntegerField()
    article_count = fields.IntegerField()
    image_url = fields.TextField()
    times_viewed = fields.IntegerField()
    cross_linked_diseases = fields.NestedField(
        properties={
            "key": fields.TextField(),
            "report_id": fields.IntegerField()
        }
    )

    def prepare_synonyms(self, instance):
        if not instance.synonyms:
            return []
        # query = Q("match", drugs__key="Otilia")
        # cts = ClinicalTrialDocument.search()
        # search = cts.filter("nested", path="drugs", query=query)
        return [{"key": syn} for syn in instance.synonyms]

    def prepare_cross_linked_diseases(self, instance):
        items = CureReport.objects.filter(
            flagged=False,
            report__disease=instance,
            status=APPROVED
        ).exclude(report__cross_linked_diseases=None
        ).values_list('report__id', 'report__cross_linked_diseases__name')
        return [{"key": i[1], "report_id": i[0]} for i in items]

    def prepare_url(self, instance):
        return f"{settings.API_SUB_DOMAIN}{settings.API_DOMAIN}diseases/{instance.id}"

    def prepare_discussion_count(self, instance):
        return instance.discussions.filter(deleted=False, flagged=False, status=APPROVED).count()

    def prepare_report_count(self, instance):
        # TODO: include xlinked_disease reports ??
        return CureReport.objects.filter(flagged=False, report__disease=instance, status=APPROVED).count()

    def prepare_trial_count(self, instance):
        return instance.clinical_trials.filter(deleted=False, status=APPROVED).count()

    def prepare_article_count(self, instance):
        return instance.articles.filter(status=APPROVED).count()

    def prepare_event_count(self, instance):
        return instance.events.filter(status=APPROVED).count()

    def prepare_image_url(self, instance):
        image_name = instance.image_name
        if not image_name:
            image_name = ""
        return f"{settings.IMAGE_PARTIAL_URL}{image_name}"

    def prepare_times_viewed(self, instance):
        path = f"/diseases/{instance.id}"
        return LogRequest.objects.filter(path__contains=path).count()

    def get_instances_from_related(self, objct):
        if isinstance(objct, LogRequest):
            path = "/diseases/"
            if path in objct.path:
                length = len(path)
                pos = objct.path.index(path)
                potential_disease_id = objct.path[ pos + length : ]
                if potential_disease_id and potential_disease_id.isnumeric():
                    try:
                        return Disease.objects.get(pk=potential_disease_id)
                    except:
                        return Disease.objects.none()
        elif isinstance(objct, Discussion):
            return objct.disease
        elif isinstance(objct, CureReport):
            return objct.report.disease
        elif isinstance(objct, ClinicalTrial):
            return objct.disease
        elif isinstance(objct, Article):
            return objct.disease
        elif isinstance(objct, Event):
            return objct.disease

    class Index:
        name = 'disease'

    class Django:
        model = Disease
        fields = [ 'name', ]
        related_models = [ LogRequest, Discussion, CureReport, ClinicalTrial, Article, Event]


#@registry.register_document
class DiscussionDocument(Document):
    disease = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'name': fields.TextField(),
            'url': fields.TextField(),
        }
    )
    author = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'first_name': fields.TextField(),
            'last_name': fields.TextField(),
            'qualification': fields.TextField(),
        }
    )
    comment_count = fields.IntegerField()
    comment_latest = fields.ObjectField(
        properties={
            'body': fields.TextField(),
        }
    )
    comment_authors = fields.NestedField(
        properties={
            "name": fields.TextField(),
        }
    )

    def prepare_author(self, instance):
        data = {
            "id": instance.author.id,
            "first_name": instance.author.first_name,
            "last_name": instance.author.last_name,
            "qualification": instance.author.profile.qualification,
        }
        return data

    def prepare_comment_count(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        comment_count = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).count()
        return comment_count

    def prepare_comment_latest(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        latest_comment = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).order_by('updated', 'created').last()
        data = {
            "body": "",
        }
        if latest_comment:
            data["body"] = latest_comment.body
        return data

    def prepare_comment_authors(self, instance):
        data = []
        content_type = ContentType.objects.get_for_model(instance)
        comments = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False)
        for comment in comments:
            author = f"{comment.author.first_name} {comment.author.last_name}"
            data.append({"name": author})
        return data

    class Index:
        name = 'discussions'

    class Django:
        model = Discussion
        fields = [ 'body', 'title', 'deleted', 'flagged', 'anonymous' ]
        related_models = [ Disease, User, Profile, Comment ]

    def get_queryset(self):
        qs = super().get_queryset().filter(deleted=False, flagged=False, status=APPROVED).select_related('author', 'disease')
        return qs

    def get_instances_from_related(self, related_instance):
        if isinstance(related_instance, Disease):
            return related_instance.discussions.filter(status=APPROVED, deleted=False, flagged=False)
        #elif isinstance(related_instance, User):
        #    return related_instance.discussion_set.filter(status=APPROVED, deleted=False, flagged=False)
        elif isinstance(related_instance, Profile):
            return related_instance.favorited_discussions.filter(status=APPROVED, deleted=False, flagged=False)
        elif isinstance(related_instance, Comment):
            return Discussion.objects.filter(status=APPROVED, deleted=False, flagged=False, id=related_instance.object_id)


#@registry.register_document
class ClinicalTrialDocument(Document):
    drugs = fields.TextField()
    disease = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'name': fields.TextField(),
            'url': fields.TextField(),
        }
    )
    author = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'first_name': fields.TextField(),
            'last_name': fields.TextField(),
            'qualification': fields.TextField(),
        }
    )
    comment_count = fields.IntegerField()
    comment_latest = fields.ObjectField(
        properties={
            'body': fields.TextField(),
        }
    )
    comment_authors = fields.NestedField(
        properties={
            "name": fields.TextField(),
        }
    )

    def prepare_drugs(self, instance):
        if not instance.drugs:
            return ""
        # query = Q("match", drugs__key="Otilia")
        # cts = ClinicalTrialDocument.search()
        # search = cts.filter("nested", path="drugs", query=query)
        return "; ".join(instance.drugs)

    def prepare_author(self, instance):
        data = {
            "id": instance.author.id,
            "first_name": instance.author.first_name,
            "last_name": instance.author.last_name,
            "qualification": instance.author.profile.qualification,
        }
        return data

    def prepare_comment_count(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        comment_count = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).count()
        return comment_count

    def prepare_comment_latest(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        latest_comment = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).order_by('updated', 'created').last()
        data = {
            "body": "",
        }
        if latest_comment:
            data["body"] = latest_comment.body
        return data

    def prepare_comment_authors(self, instance):
        data = []
        content_type = ContentType.objects.get_for_model(instance)
        comments = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False)
        for comment in comments:
            author = f"{comment.author.first_name} {comment.author.last_name}"
            data.append({"name": author})
        return data

    class Index:
        name = 'clinical_trials'

    class Django:
        model = ClinicalTrial
        fields = [ 'title', 'sponsor', 'status', 'phase', 'interventions',
                'ct_status', 'locations', 'participants', 'start_year',
                'country', 'clinical_trials_gov_id', 'deleted']
        related_models = [ Disease, User, Profile, Comment ]

    def get_queryset(self):
        qs = super().get_queryset().filter(deleted=False, status=APPROVED).select_related('author', 'disease')
        return qs

    def get_instances_from_related(self, related_instance):
        if isinstance(related_instance, Disease):
            return related_instance.clinical_trials.filter(status=APPROVED, deleted=False)
        #elif isinstance(related_instance, User):
        #    return ClinicalTrial.objects.filter(author=related_instance, status=APPROVED, deleted=False)
        elif isinstance(related_instance, Profile):
            return ClinicalTrial.objects.filter(author=related_instance.user, status=APPROVED, deleted=False)
        elif isinstance(related_instance, Comment):
            return ClinicalTrial.objects.filter(status=APPROVED, deleted=False, id=related_instance.object_id)


#@registry.register_document
class ReportDocument(Document):
    disease = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'name': fields.TextField(),
            'url': fields.TextField(),
        }
    )
    author = fields.ObjectField(
        properties={
            'id': fields.IntegerField(),
            'first_name': fields.TextField(),
            'last_name': fields.TextField(),
            'qualification': fields.TextField(),
        }
    )
    comment_count = fields.IntegerField()
    comment_latest = fields.ObjectField(
        properties={
            'body': fields.TextField(),
        }
    )
    comment_authors = fields.NestedField(
        properties={
            "name": fields.TextField(),
        }
    )
    report = fields.ObjectField(
        properties={
            "id": fields.IntegerField(),
            "regimens": fields.NestedField(
                properties={
                    "value": fields.ObjectField(
                        properties={
                            "id": fields.IntegerField(),
                            "drug": fields.ObjectField(
                                properties={
                                    "id": fields.IntegerField(),
                                    "name": fields.TextField(),
                                    "url": fields.TextField(),
                                    "rxnorm_id": fields.TextField(),
                                }
                            ),
                            "use_drug": fields.NestedField(
                                properties={
                                    "name": fields.TextField(),
                                }
                            ),
                            "dose": fields.TextField(),
                            "frequency": fields.TextField(),
                            "route": fields.TextField(),
                            "start_date": fields.TextField(),
                            "end_date": fields.TextField(),
                            "duration": fields.TextField(),
                            "severity": fields.TextField(),
                            "severity_detail": fields.TextField(),
                            "report": fields.IntegerField(),
                        }
                    ),
                }
            ),
            "resistant_drugs": fields.NestedField(
                properties={
                    "name": fields.TextField(),
                }
            ),
            "previous_drugs": fields.NestedField(
                properties={
                    "name": fields.TextField(),
                }
            ),
            "article": fields.ObjectField(
                properties={
                    "id": fields.IntegerField(),
                    "disease": fields.ObjectField(
                        properties={
                            "id": fields.IntegerField(),
                            "name": fields.TextField(),
                        }
                    ),
                    "author": fields.ObjectField(
                        properties={
                            "id": fields.IntegerField(),
                            "first_name": fields.TextField(),
                            "last_name": fields.TextField(),
                            "qualification": fields.TextField(),
                        }
                    ),
                    "name": fields.TextField(),
                    "title": fields.TextField(),
                    "published": fields.BooleanField(),
                    "pubmed_id": fields.IntegerField(),
                    "doi": fields.TextField(),
                    "article_url": fields.TextField(),
                    "pub_year": fields.IntegerField(),
                    "published_authors": fields.TextField(),
                    "publication_type": fields.TextField(),
                    "article_author_email": fields.TextField(),
                    "abstract": fields.TextField(),
                    "article_type": fields.TextField(),
                    "study_type": fields.TextField(),
                    "number_of_patients": fields.IntegerField(),
                    "article_language": fields.TextField(),
                    "full_text_available": fields.BooleanField(),
                    "status": fields.TextField(),
                }
            ),
            "organism": fields.NestedField(
                properties={
                    "name": fields.TextField(),
                }
            ),
            "how_diagnosis": fields.NestedField(
                properties={
                    "value": fields.TextField(),
                }
            ),
            "why_new_way": fields.NestedField(
                properties={
                    "value": fields.TextField(),
                }
            ),
            "drugs": fields.NestedField(
                properties={
                    "name": fields.TextField(),
                }
            ),
            "patient": fields.ObjectField(
                properties={
                    "id": fields.IntegerField(),
                    "comorbidities": fields.NestedField(
                        properties={
                            "id": fields.IntegerField(),
                            "value": fields.TextField(),
                        }
                    ),
                    "pregnancy": fields.ObjectField(
                        properties={
                            "id": fields.IntegerField(),
                            "neonates": fields.NestedField(
                                properties={
                                    "id": fields.IntegerField(),
                                    "diagnosed_with_disease": fields.TextField(),
                                    "abnormalities_or_defects": fields.TextField(),
                                    "other_outcome_details": fields.TextField(),
                                    "pregnancy": fields.IntegerField(),
                                }
                            ),
                            "treatment_gestational_age": fields.TextField(),
                            "delivery_gestational_age": fields.TextField(),
                            "outcome": fields.TextField(),
                        }
                    ),
                    "races": fields.NestedField(
                        properties={
                            "value": fields.TextField(),
                        },
                    ),
                    "age": fields.TextField(),
                    "age_group": fields.TextField(),
                    "ethnicity": fields.TextField(),
                    "sex": fields.TextField(),
                    "race": fields.TextField(),
                    "pregnant": fields.TextField(),
                    "other_coinfections": fields.TextField(),
                    "comorbidity": fields.NestedField(
                        properties={
                            "id": fields.IntegerField(),
                            "value": fields.TextField(),
                        }
                    ),
                },
            ),
            "outcome_computed": fields.TextField(),
            "how_outcome": fields.NestedField(
                properties={
                    "value": fields.TextField(),
                }
            ),
            "percentage_completed": fields.IntegerField(),
            "outcome": fields.TextField(),
            "surgery": fields.TextField(),
            "country_contracted": fields.TextField(),
            "country_treated": fields.TextField(),
            "began_treatment_year": fields.TextField(),
            "site_of_disease": fields.TextField(),
            "site_of_tuberculosis_infection": fields.NestedField(
                properties={
                    "value": fields.TextField(),
                }
            ),
            "clinical_syndrome": fields.TextField(),
            "unusual": fields.TextField(),
            "when_outcome": fields.TextField(),
            "relapse": fields.TextField(),
            "adverse_events": fields.TextField(),
            "have_adverse_events": fields.TextField(),
            "adverse_events_outcome": fields.TextField(),
            "extrapulmonary_site": fields.NestedField(
                properties={
                    "value": fields.TextField(),
                }
            ),
            "additional_info": fields.TextField(),
            "outcome_followup": fields.TextField(),
            "organisms": fields.NestedField(
                properties={
                    "value": fields.TextField(),
                }
            ),
        }
    )

    def prepare_report(self, instance):
        if instance.report:

            how_diagnosis = []
            if instance.report.how_diagnosis:
                how_diagnosis = [{"value": i} for i in instance.report.how_diagnosis]

            why_new_way = []
            if instance.report.why_new_way:
                why_new_way = [{"value": i} for i in instance.report.why_new_way]

            outcome = ""
            if instance.report.outcome in [PATIENT_WAS_CURED, PATIENT_IMPROVED]:
                outcome = 'Improved'
            elif instance.report.outcome in [PATIENT_CONDITION_UNCHANGED, UNKNOWN_OUTCOME]:
                outcome = 'Undetermined'
            elif instance.report.outcome in [PATIENT_DETERIORATED, PATIENT_DIED, TREATMENT_TERMINATED]:
                outcome = 'Deteriorated'

            if instance.report.how_outcome:
                how_outcome = [{"value": i} for i in instance.report.how_outcome]
            else:
                how_outcome = []

            site_of_tuberculosis_infection = []
            if instance.report.site_of_tuberculosis_infection:
                site_of_tuberculosis_infection = [{"value": i} for i in instance.report.site_of_tuberculosis_infection]

            extrapulmonary_site = []
            if instance.report.extrapulmonary_site:
                extrapulmonary_site = [{"value": i} for i in instance.report.extrapulmonary_site]

            regimens = []
            drugs = []
            for r in instance.report.regimens.all().select_related("drug"):
                data = {
                    "id": r.id,
                    "drug": {
                        "id": r.drug_id,
                        "name": r.drug.name,
                        "url": "%s%sdrugs/%s" % ( settings.API_SUB_DOMAIN, settings.API_DOMAIN, r.drug.id),
                        "rxnorm_id": r.drug.rxnorm_id,
                    },
                    "use_drug": [{"name": i} for i in r.use_drug] if r.use_drug else [],
                    "dose": r.dose,
                    "frequency": r.frequency,
                    "route": r.route,
                    "start_date": r.start_date,
                    "end_date": r.end_date,
                    "duration": r.duration,
                    "severity": r.severity,
                    "severity_detail": r.severity_detail,
                    "report": r.report_id,
                }
                regimens.append(data)
                drugs.append({"name": r.drug.name})

            article = {}
            if instance.report.article:
                article = {
                    "id": instance.report.article.id,
                    "disease": {
                        "id": instance.report.article.disease.id,
                        "name": instance.report.article.disease.name,
                    },
                    "author": {
                        "id": instance.report.article.author.id,
                        "first_name": instance.report.article.author.first_name,
                        "last_name": instance.report.article.author.last_name,
                        "qualification": instance.report.article.author.profile.qualification,
                    },
                    "name": instance.report.article.publication_name,
                    "title": instance.report.article.title,
                    "published": instance.report.article.published,
                    "doi": instance.report.article.doi,
                    "article_url": instance.report.article.article_url,
                    "pub_year": instance.report.article.pub_year,
                    "published_authors": instance.report.article.published_authors,
                    "publication_type": instance.report.article.publication_type,
                    "article_author_email": instance.report.article.article_author_email,
                    "abstract": instance.report.article.abstract,
                    "article_type": instance.report.article.article_type,
                    "study_type": instance.report.article.study_type,
                    "number_of_patients": instance.report.article.number_of_patients,
                    "article_language": instance.report.article.article_language,
                    "full_text_available": instance.report.article.full_text_available,
                    "status": instance.report.article.status,
                }

            patient = {}
            if instance.report.patient:
                p = instance.report.patient
                patient["id"] = p.id
                patient["comorbidities"] = [{"id": i.id, "value": i.value} for i in p.comorbidity.all()]
                patient["pregnancy"] = {}
                if p.pregnancy:
                    patient["pregnancy"]["id"] = p.pregnancy.id
                    patient["pregnancy"]["neonates"] = [
                        {
                            "id": i.id,
                            "diagnosed_with_disease": i.diagnosed_with_disease,
                            "abnormalities_or_defects": i.abnormalities_or_defects,
                            "pregnancy": p.pregnancy.id
                        } for i in p.pregnancy.neonates.all()
                    ]
                    patient["pregnancy"]["treatment_gestational_age"] = p.pregnancy.treatment_gestational_age
                    patient["pregnancy"]["delivery_gestational_age"] = p.pregnancy.delivery_gestational_age
                    patient["pregnancy"]["outcome"] = p.pregnancy.outcome
                patient["races"] = [{"value": "No Idea Where To Get This From"}]
                patient["age"] = p.age
                patient["age_group"] = p.age_group
                patient["ethnicity"] = p.ethnicity
                patient["sex"] = p.sex
                patient["race"] = p.race
                patient["pregnant"] = p.pregnant
                patient["other_coinfections"] = p.other_coinfections
                patient["comorbidity"] = patient["comorbidities"]

            return {
                "id": instance.report.id,
                "regimens": regimens,
                "resistant_drugs": [{"name": i.name} for i in instance.report.resistant_drugs.all()],
                "previous_drugs": [{"name": i.name} for i in instance.report.previous_drugs.all()],
                "article": article,
                "how_diagnosis": how_diagnosis,
                "why_new_way": why_new_way,
                "drugs": drugs,
                "patient": patient,
                "outcome_computed": outcome,
                "how_outcome": how_outcome,
                "percentage_completed": instance.report.percentage_completed,
                "outcome": instance.report.outcome,
                "surgery": instance.report.surgery,
                "country_contracted": instance.report.country_contracted,
                "country_treated": instance.report.country_treated,
                "began_treatment_year": instance.report.began_treatment_year,
                "site_of_disease": instance.report.site_of_disease,
                "site_of_tuberculosis_infection": site_of_tuberculosis_infection,
                "clinical_syndrome": instance.report.clinical_syndrome,
                "unusual": instance.report.unusual,
                "when_outcome": instance.report.when_outcome,
                "relapse": instance.report.relapse,
                "adverse_events": instance.report.adverse_events,
                "have_adverse_events": instance.report.have_adverse_events,
                "adverse_events_outcome": instance.report.adverse_events_outcome,
                "extrapulmonary_site": extrapulmonary_site,
                "additional_info": instance.report.additional_info,
                "outcome_followup": instance.report.outcome_followup,
                "organisms": [{"value": i.name} for i in instance.report.organisms.all()]
            }
        else:
            return {}

    def prepare_disease(self, instance):
        disease = getattr(instance.report, 'disease', None)
        if not disease:
            return {}

        url = "%s%sdiseases/%s" % (
            settings.API_SUB_DOMAIN,
            settings.API_DOMAIN,
            disease.id
        )
        return {"id": disease.id, "name": disease.name, "url": url}

    def prepare_author(self, instance):
        data = {
            "id": instance.author.id,
            "first_name": instance.author.first_name,
            "last_name": instance.author.last_name,
            "qualification": instance.author.profile.qualification,
        }
        return data

    def prepare_comment_count(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        comment_count = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).count()
        return comment_count

    def prepare_comment_latest(self, instance):
        content_type = ContentType.objects.get_for_model(instance)
        latest_comment = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False).order_by('updated', 'created').last()
        data = {
            "body": "",
        }
        if latest_comment:
            data["body"] = latest_comment.body
        return data

    def prepare_comment_authors(self, instance):
        data = []
        content_type = ContentType.objects.get_for_model(instance)
        comments = Comment.objects.filter(content_type=content_type, object_id=instance.id, deleted=False, flagged=False)
        for comment in comments:
            author = f"{comment.author.first_name} {comment.author.last_name}"
            data.append({"name": author})
        return data

    class Index:
        name = 'reports'

    class Django:
        model = CureReport
        fields = [ 'anonymous', 'when_reminder', 'status', ]
        related_models = [ Disease, User, Profile, Comment ]

    def get_queryset(self):
        qs = super().get_queryset().filter(status=APPROVED).select_related('author', 'report', 'report__disease')
        return qs

    def get_instances_from_related(self, related_instance):
        # TODO: not all models, add them when we do use this index
        if isinstance(related_instance, Disease):
            return related_instance.events.filter(status=APPROVED)
        #elif isinstance(related_instance, User):
        #    return related_instance.events.filter(status=APPROVED)
        elif isinstance(related_instance, Profile):
            return related_instance.user.events.filter(status=APPROVED)
        elif isinstance(related_instance, Comment):
            return Event.objects.filter(status=APPROVED, id=related_instance.object_id)


