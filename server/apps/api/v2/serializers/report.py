from rest_framework import serializers
from django.core.exceptions import ObjectDoesNotExist
from django.contrib.contenttypes.models import ContentType

from server.apps.core.constants import (
    PATIENT_CONDITION_UNCHANGED,
    PATIENT_DETERIORATED,
    PATIENT_DIED,
    PATIENT_IMPROVED,
    PATIENT_WAS_CURED,
    TREATMENT_TERMINATED,
    UNKNOWN_OUTCOME,
)
from server.apps.core.models import (
    CureReport,
    Report,
    Regimen,
    Disease,
    Drug,
    Patient,
    Organism,
    Comorbidity,
    Pregnancy,
    Neonate,
    AttachedImage,
    User,
    Comment
)

from server.apps.api.v2.serializers.drug import DrugSerializer
from server.apps.api.v2.serializers.disease import MinimalDiseaseSerializer
from server.apps.api.v2.serializers.patient import PatientSerializer
from server.apps.api.v2.serializers.article import ArticleSerializer
from server.apps.api.v2.serializers.organism import OrganismSerializer
from server.apps.api.v2.serializers.disease import MinimalDiseaseSerializer
from server.apps.api.v2.serializers.comment import CommentSerializer, MinimalCommentSerializer
from server.apps.api.v2.serializers.profile import MinimalUserSerializer, DiscussionUserSerializer
from server.apps.api.v2.serializers.regimen import RegimenSerializer
from server.apps.api.v2.serializers.image import AttachedImageSerializer


class ReportSerializer(serializers.ModelSerializer):
    regimens = RegimenSerializer(many=True, default=[])
    resistant_drugs = DrugSerializer(many=True, default=[])
    previous_drugs = DrugSerializer(many=True, default=[])
    patient = PatientSerializer(read_only=True)
    article = ArticleSerializer(read_only=True)
    organism = OrganismSerializer(many=True, default=[])
    how_diagnosis = serializers.SerializerMethodField()
    why_new_way = serializers.SerializerMethodField()
    drugs = DrugSerializer(many=True, default=[])
    patient = PatientSerializer()
    outcome_computed = serializers.SerializerMethodField()
    how_outcome = serializers.SerializerMethodField()
    percentage_completed = serializers.IntegerField()
    attached_images = serializers.SerializerMethodField()
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()
    disease = MinimalDiseaseSerializer()
    sample = serializers.SerializerMethodField()


    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(
                content_type_id=content_type.id,
                object_id=article.id,
                anonymous=False,
                deleted=False, flagged=False,
        ).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        count = Comment.objects.filter(
                content_type_id=content_type.id, object_id=discussion.id,
                deleted=False, flagged=False
            ).count()

        return count

    def get_comment_latest(self,discussion):
        content_type = ContentType.objects.get_for_model(discussion)
        try:
            latest = Comment.objects.filter(
                    content_type_id=content_type.id, object_id=discussion.id,
                    deleted=False, flagged=False
                ).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []

        return serializer.data

    def get_attached_images(self, ct_object):
        content_type = ContentType.objects.get_for_model(ct_object)
        attached_images = AttachedImage.objects.filter(content_type_id=content_type, object_id=ct_object.id)
        serializer = AttachedImageSerializer(attached_images, many=True)
        return serializer.data

    def get_outcome_computed(self, obj):
        if obj.outcome in [PATIENT_WAS_CURED, PATIENT_IMPROVED]:
            return 'Improved'
        if obj.outcome in [PATIENT_CONDITION_UNCHANGED, UNKNOWN_OUTCOME]:
            return 'Undetermined'
        if obj.outcome in [PATIENT_DETERIORATED, PATIENT_DIED, TREATMENT_TERMINATED]:
            return 'Deteriorated'

    def get_how_diagnosis(self, report):
        result = []
        if report.how_diagnosis:
            result = [{"value":hd } for hd in report.how_diagnosis]
        return result

    def get_why_new_way(self, report):
        result = []
        if report.why_new_way:
            result = [{"value":wnw } for wnw in report.why_new_way]
        return result

    def get_how_outcome(self, report):
        result = []
        if report.how_outcome:
            result = [{"value":ho} for ho in report.how_outcome]
        return result

    def get_sample(self,report):
        result = []
        if report.sample:
            result = [{"value": s} for s in report.sample]
        return result

    class Meta:
        model = Report
        fields = '__all__'

        depth = 1


class CureReportSerializer(serializers.ModelSerializer):
    report = ReportSerializer(required=False)
    author = serializers.SerializerMethodField()

    def get_author(self,report):
        if report.report.article:
            if report.report.article.published:
                serializer = MinimalUserSerializer(report.author)
                return serializer.data
        serializer = DiscussionUserSerializer(report.author)
        return serializer.data

    class Meta:
        model = CureReport
        exclude = [
            "flagged",
            "reminder",
            "is_author"
        ]

        depth = 1


class WritableCureReportSerializer(serializers.ModelSerializer):
    report = ReportSerializer(required=False)
    author = MinimalUserSerializer(required=False)

    class Meta:
        model = CureReport
        exclude = [
            "flagged",
            "reminder",
            "is_author"
        ]

        depth = 1

    def get_is_author(self, report):
        return report.author == self.context["request"].user

    def create(self, validated_data):
        author = self.context["request"].user

        # Auto-create disease if provided disease_id is null
        disease = validated_data.pop("report__disease", None)
        #remove drug field because causes errorwhen creating report
        # validated_data.pop('drugs')
        disease_name = disease["name"]
        # We need iexact here because some diseases (for ex. COVID-19)
        #  do not follow the rule of capitalization of names
        disease_obj, created = Disease.objects.get_or_create(name__iexact=disease_name.capitalize())
        validated_data["disease"] = disease_obj
        validated_data['extra_fields'] = {}
        patient = Patient.objects.create()
        report = Report.objects.create(**validated_data,patient=patient)


        cure_report = CureReport.objects.create(report=report, author=author)

        if author.profile.qualification == 'Patient':
            cure_report.report_type='patient'
            cure_report.anonymous=True

        cure_report.save()
        return cure_report

    def update(self, instance, validated_data):
        #for report object
        try:
            instance.status = validated_data.get('status', instance.status)
            instance.report.began_treatment_year = validated_data.get('report__began_treatment_year', instance.report.began_treatment_year)
            instance.report.country_contracted = validated_data.get(
                "report__country_contracted", instance.report.country_contracted
            )
            instance.report.country_treated = validated_data.get(
                "report__country_treated", instance.report.country_treated
            )

            instance.report.outcome = validated_data.get("report__outcome", instance.report.outcome)

            instance.report.unusual = validated_data.get("report__unusual", instance.report.unusual)
            instance.report.when_outcome = validated_data.get(
                "report__when_outcome", instance.report.when_outcome
            )
            instance.report.relapse = validated_data.get("report__relapse", instance.report.relapse)
            instance.report.surgery = validated_data.get("report__surgery", instance.report.surgery)
            instance.report.site_of_disease = validated_data.get(
                "report__site_of_disease", instance.report.site_of_disease
            )
            instance.report.extrapulmonary_site = validated_data.get("report__extrapulmonary_site", instance.report.extrapulmonary_site)
            instance.report.adverse_events_outcome = validated_data.get("report__adverse_events_outcome", instance.report.adverse_events_outcome)
            instance.report.site_of_tuberculosis_infection = validated_data.get("report__site_of_tuberculosis_infection", instance.report.site_of_tuberculosis_infection)
            instance.report.have_adverse_events = validated_data.get(
                "report__have_adverse_events", instance.report.have_adverse_events
            )
            instance.report.adverse_events = validated_data.get(
                "report__adverse_events", instance.report.adverse_events
            )

            instance.when_reminder = validated_data.get(
                "when_reminder", instance.when_reminder
            )

            instance.report.clinical_syndrome = validated_data.get(
                "report__clinical_syndrome", instance.report.clinical_syndrome
            )

            instance.report.outcome_followup = validated_data.get('report__outcome_followup', instance.report.outcome_followup)
            instance.report.additional_info = validated_data.get('report__additional_info', instance.report.additional_info)

            instance.report.patient.age_group = validated_data.get("report__patient__age_group", instance.report.patient.age_group)
            instance.report.patient.ethnicity = validated_data.get("report__patient__ethnicity", instance.report.patient.ethnicity)
            instance.report.patient.sex = validated_data.get("report__patient__sex", instance.report.patient.sex)
            instance.anonymous = validated_data.get("anonymous", instance.anonymous)
        except Exception as e:
            pass


        try:
            disease = validated_data.get('report__disease', instance.report.disease)
            updated_disease,created = Disease.objects.get_or_create(name=disease.get('name'))
            instance.report.disease = updated_disease
        except Exception as e:
            pass


        try:
            drugs_data = validated_data.pop('report__drugs')
            current_drugs = [d.get('id',None) for d in drugs_data]
            drug_in_db = [d for d in instance.report.drugs.all()]
            not_found_drugs = [d for d in drug_in_db if d.id not in current_drugs]
            # regi = Regimen.objects.filter(report_id = instance.report.id, drug_id__in=not_found_drugs)
            instance.report.drugs.remove(*not_found_drugs)
            regimen_to_delete = [r.id for r in instance.report.regimens.all() if r.drug.id not in current_drugs]
            Regimen.objects.filter(id__in = regimen_to_delete).delete()

            for drug_data in drugs_data:
                drug, created = Drug.objects.get_or_create(name__iexact=drug_data.get("name").capitalize())
                instance.report.drugs.add(drug)
                try:
                    Regimen.objects.get(drug_id=drug.id, report_id=instance.report.id)
                except ObjectDoesNotExist:
                    Regimen.objects.create(drug_id=drug.id, report_id=instance.report.id)
        except Exception as e:
            pass

        if 'report__how_diagnosis' in  validated_data:
            instance.report.how_diagnosis = [r.get('value') for r in validated_data.get("report__how_diagnosis")] or instance.report.how_diagnosis

        if 'report__why_new_way' in validated_data:
            instance.report.why_new_way = [r.get('value') for r in validated_data.get("report__why_new_way")] or instance.report.why_new_way

        if 'report__how_outcome' in  validated_data:
            instance.report.how_outcome = [r.get('value') for r in validated_data.get("report__how_outcome")] or instance.report.how_outcome

        if 'report__sample' in validated_data:
            instance.report.sample = [r.get('value') for r in validated_data.get("report__sample")] or instance.report.sample

        if 'report__patient__races' in validated_data:
            instance.report.patient.race = [r.get('value') for r in validated_data.get("report__patient__races")] or instance.report.patient.race

        if 'report__extra_fields__monkeypox_hiv_art' in validated_data:
            instance.report.extra_fields['monkeypox_hiv_art'] = validated_data.get('report__extra_fields__monkeypox_hiv_art') or instance.report.extra_fields.get('monkeypox_hiv_art')
       
        if 'report__extra_fields__monkeypox_cd4_count' in validated_data:
            instance.report.extra_fields['monkeypox_cd4_count'] = validated_data.get('report__extra_fields__monkeypox_cd4_count') or instance.report.extra_fields.get('monkeypox_cd4_count')
        
        if 'report__extra_fields__monkeypox_viral_load' in validated_data:
            instance.report.extra_fields['monkeypox_viral_load'] = validated_data.get('report__extra_fields__monkeypox_viral_load') or instance.report.extra_fields.get('monkeypox_viral_load')

        if 'report__extra_fields__monkeypox_symptoms' in validated_data:
            instance.report.extra_fields['monkeypox_symptoms'] = validated_data.get('report__extra_fields__monkeypox_symptoms') or instance.report.extra_fields.get('monkeypox_symptoms')

        if 'report__extra_fields__monkeypox_time_to_treatment_start' in validated_data:
            instance.report.extra_fields['monkeypox_time_to_treatment_start'] = validated_data.get('report__extra_fields__monkeypox_time_to_treatment_start') or instance.report.extra_fields.get('monkeypox_time_to_treatment_start')
        
        if 'report__extra_fields__monkeypox_time_complete_resolution_symptoms' in validated_data:
            instance.report.extra_fields['monkeypox_time_complete_resolution_symptoms'] = validated_data.get('report__extra_fields__monkeypox_time_complete_resolution_symptoms') or instance.report.extra_fields.get('monkeypox_time_complete_resolution_symptoms')
        
        if 'report__extra_fields__monkeypox_lesion_pain' in validated_data:
            instance.report.extra_fields['monkeypox_lesion_pain'] = validated_data.get('report__extra_fields__monkeypox_lesion_pain') or instance.report.extra_fields.get('monkeypox_lesion_pain')
        
        if 'report__extra_fields__monkeypox_lesion_pain_time_to_improvement' in validated_data:
            instance.report.extra_fields['monkeypox_lesion_pain_time_to_improvement'] = validated_data.get('report__extra_fields__monkeypox_lesion_pain_time_to_improvement') or instance.report.extra_fields.get('monkeypox_lesion_pain_time_to_improvement')
        
        if 'report__extra_fields__monkeypox_lesion_time_to_resolve' in validated_data:
            instance.report.extra_fields['monkeypox_lesion_time_to_resolve'] = validated_data.get('report__extra_fields__monkeypox_lesion_time_to_resolve') or instance.report.extra_fields.get('monkeypox_lesion_time_to_resolve')
        
        if 'report__extra_fields__monkeypox_hospitalization' in validated_data:
            instance.report.extra_fields['monkeypox_hospitalization'] = validated_data.get('report__extra_fields__monkeypox_hospitalization') or instance.report.extra_fields.get('monkeypox_hospitalization')
        
        if 'report__extra_fields__monkeypox_days_of_hospitalization' in validated_data:
            instance.report.extra_fields['monkeypox_days_of_hospitalization'] = validated_data.get('report__extra_fields__monkeypox_days_of_hospitalization') or instance.report.extra_fields.get('monkeypox_days_of_hospitalization')
        
        if 'report__extra_fields__monkeypox_reason_for_hospitalization' in validated_data:
            instance.report.extra_fields['monkeypox_reason_for_hospitalization'] = validated_data.get('report__extra_fields__monkeypox_reason_for_hospitalization') or instance.report.extra_fields.get('monkeypox_reason_for_hospitalization')
        
        if 'report__extra_fields__monkeypox_sexual_behavior' in validated_data:
            instance.report.extra_fields['monkeypox_sexual_behavior'] = validated_data.get('report__extra_fields__monkeypox_sexual_behavior') or instance.report.extra_fields.get('monkeypox_sexual_behavior')
        
        if 'report__extra_fields__monkeypox_intimate_contact' in  validated_data:
            instance.report.extra_fields['monkeypox_intimate_contact'] = validated_data.get('report__extra_fields__monkeypox_intimate_contact') or instance.report.extra_fields.get('monkeypox_intimate_contact')
        
        if 'report__extra_fields__monkeypox_smallpox_vaccine_status' in validated_data:
            instance.report.extra_fields['monkeypox_smallpox_vaccine_status'] = validated_data.get('report__extra_fields__monkeypox_smallpox_vaccine_status') or instance.report.extra_fields.get('monkeypox_smallpox_vaccine_status')
    
        if 'report__extra_fields__monkeypox_site_of_infection' in validated_data:
            instance.report.extra_fields['monkeypox_site_of_infection'] = validated_data.get('report__extra_fields__monkeypox_site_of_infection') or instance.report.extra_fields.get('monkey_smallpox_vaccine_status')


        try:
            instance.report.patient.pregnant = validated_data.get("report__patient__pregnant", instance.report.patient.pregnant)

            if instance.report.patient.pregnant and not instance.report.patient.pregnancy:
                new_pregnancy = Pregnancy.objects.create()
                instance.report.patient.pregnancy = new_pregnancy

            if not instance.report.patient.pregnant:
                instance.report.patient.pregnancy = None

            if instance.report.patient.pregnancy:
                instance.report.patient.pregnancy.delivery_gestational_age = validated_data.get("report__patient__pregnancy__delivery_gestational_age", instance.report.patient.pregnancy.delivery_gestational_age)
                instance.report.patient.pregnancy.treatment_gestational_age = validated_data.get("report__patient__pregnancy__treatment_gestational_age", instance.report.patient.pregnancy.treatment_gestational_age)
                instance.report.patient.pregnancy.outcome = validated_data.get("report__patient__pregnancy__outcome", instance.report.patient.pregnancy.outcome)
                instance.report.patient.pregnancy.save()

        except Exception as e:
            pass


        try:
            organisms_data = validated_data.pop('report__organisms')
            current_organisms = [o.get('id') for o in organisms_data]
            orgs_in_db = [o for o in instance.report.organisms.all()]
            not_found_orgs = [o for o in orgs_in_db if o.id not in current_organisms]
            instance.report.organisms.remove(*not_found_orgs)
            for organism_data in organisms_data:
                found_organism, created = Organism.objects.get_or_create(name__iexact=organism_data.get('name').capitalize())
                instance.report.organisms.add(found_organism)
        except Exception as e:
            pass


        #For regimen
        try:
            regimens_data = validated_data.pop('regimens')
            new_data = {}
            for data in validated_data:
                params = data.split('__')
                if type(params[2].isdigit()) is not dict:
                    regimen_index = params[2]
                    key = params[3]
                    new_data[key] = validated_data[data]

            regimen_data = regimens_data[int(regimen_index)]
            regimen_id = regimen_data.pop('id',None)
            drug_id = regimen_data.pop('drug').get('id')
            found_regimen = Regimen.objects.filter(id=regimen_id, report_id=instance.report.id)

            if found_regimen.exists():
                if 'use_drug' in new_data:
                    new_data['use_drug'] = instance.report.regimens.use_drug = [ud.get('value') for ud in new_data.get("use_drug")] or instance.report.regimens.use_drug
                updated = found_regimen.update(**new_data, drug_id=drug_id,report_id=instance.report.id)
                # else create regimen
            else:
                regimen = Regimen.objects.create(**new_data, drug_id=drug_id, report_id=instance.report.id)
                instance.report.regimens.add(regimen)
                instance.report.regimens.save()


        except Exception as e:
            pass

        try:
            res_drugs_data = validated_data.pop('report__resistant_drugs')
            current_res_drugs = [rd.get('id',None) for rd in res_drugs_data]
            resistant_drugs_in_db = [rd for rd in instance.report.resistant_drugs.all()]
            not_found_resistant_drugs = [rd for rd in resistant_drugs_in_db if rd.id not in current_res_drugs]
            instance.report.resistant_drugs.remove(*not_found_resistant_drugs)
            for res_drug_data in res_drugs_data:
                resistant_drug, created = Drug.objects.get_or_create(name__iexact=res_drug_data.get("name").capitalize())
                instance.report.resistant_drugs.add(resistant_drug)
        except:
            pass

        try:
            # update previous drugs
            previous_drugs_data = validated_data.pop("report__previous_drugs")
            for drug in instance.report.previous_drugs.all():
                if drug not in previous_drugs_data:
                    instance.report.previous_drugs.remove(drug)
            for previous_drug in previous_drugs_data:
                drug, created = Drug.objects.get_or_create(name=previous_drug.get("name"))
                instance.report.previous_drugs.add(drug)
        except:
            pass

        try:
            comorbidities_data = validated_data.pop("report__patient__comorbidities")
            for comorbidity in instance.report.patient.comorbidity.all():
                if comorbidity not in comorbidities_data:
                    instance.report.patient.comorbidity.remove(comorbidity)

            for comorbidity in comorbidities_data:
                new_comorbidity,create = Comorbidity.objects.get_or_create(**comorbidity)
                instance.report.patient.comorbidity.add(new_comorbidity)
                instance.report.patient.save()
        except:
            pass

        try:
            neonate_data = validated_data.pop("report__patient__pregnancy__neonates")
            current_neonates = [n.get('id') for n in neonate_data]
            n_in_db = [n.id for n in instance.report.patient.pregnancy.neonates.all()]
            not_found_neonate = [n for n in n_in_db if n not in current_neonates]
            Neonate.objects.filter(id__in=not_found_neonate).delete()
            for n in neonate_data:
                n.pop('pregnancy',None)
                n_id=n.pop('id',None)
                new_neonate, created = Neonate.objects.update_or_create(defaults=n,id=n_id, pregnancy=instance.report.patient.pregnancy)
                instance.report.patient.pregnancy.neonates.add(new_neonate)

        except Exception as e:
            pass

        if 'report__attached_images' in validated_data:
            images = validated_data.get('report__attached_images')
            current_images = [image.get('id', None) for image in images]
            images_in_db = [im for im in instance.report.attached_images.all()]
            images_to_delete = [i for i in images_in_db if i.id not in current_images]
            instance.report.attached_images.remove(*images_to_delete)

            if images:
                for image in images:
                    if 'id' in image and AttachedImage.objects.filter(real_name=image['url']).exists():
                        image_to_update = AttachedImage.objects.get(id=image['id'])
                        image_to_update.caption = image['caption']
                        image_to_update.save()
                    else:
                        new_image=AttachedImage(content_object=instance.report, real_name=image.get('url'), caption=image.get('caption',None))
                        new_image.save()

        instance.report.patient.save()
        instance.report.save()
        instance.save()
        return instance

    def to_internal_value(self, data):
        internal_value = super().to_internal_value(data)

        for key, value in data.items():
            if key not in internal_value:
                internal_value.update(
                    {key:value}
                )

        return internal_value

    def validate(self, attrs):
        attrs.author = self.context["request"].user
        # try:
        #     regimens = self.handle_regimens(attrs)
        #     attrs.update({"regimens": regimens})
        # except:
        #     pass
        return attrs



class MinimalReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = Report
        fields = ("id",)


class ReportNewsfeedSerializer(serializers.ModelSerializer):
    regimens = RegimenSerializer(many=True, default=[])
    resistant_drugs = DrugSerializer(many=True, default=[])
    previous_drugs = DrugSerializer(many=True, default=[])
    patient = PatientSerializer(read_only=True)
    article = ArticleSerializer(read_only=True)
    organism = OrganismSerializer(many=True, default=[])
    how_diagnosis = serializers.SerializerMethodField()
    why_new_way = serializers.SerializerMethodField()
    drugs = DrugSerializer(many=True, default=[])
    patient = PatientSerializer()
    outcome_computed = serializers.SerializerMethodField()
    how_outcome = serializers.SerializerMethodField()
    percentage_completed = serializers.IntegerField()
    attached_images = serializers.SerializerMethodField()
    comment_count = serializers.SerializerMethodField()
    comment_latest = serializers.SerializerMethodField()
    comment_authors = serializers.SerializerMethodField()

    def get_comment_authors(self, article):
        content_type = ContentType.objects.get_for_model(article)
        authors_list = Comment.objects.filter(
                content_type_id=content_type.id,
                object_id=article.id,
                anonymous=False,
                deleted=False, flagged=False,
        ).values_list('author')
        authors = User.objects.filter(id__in = authors_list)

        serializer = MinimalUserSerializer(authors, many=True)
        return serializer.data

    def get_comment_count(self, article):
        content_type = ContentType.objects.get_for_model(article)
        count = Comment.objects.filter(
                content_type_id=content_type.id, object_id=article.id,
                deleted=False, flagged=False,
            ).count()
        return count

    def get_comment_latest(self, article):
        content_type = ContentType.objects.get_for_model(article)
        try:
            latest = Comment.objects.filter(
                    content_type_id=content_type.id, object_id=article.id,
                    deleted=False, flagged=False,
                ).last()
            serializer = MinimalCommentSerializer(latest)
        except Comment.DoesNotExist:
            return []
        return serializer.data

    def get_attached_images(self, ct_object):
        content_type = ContentType.objects.get_for_model(ct_object)
        attached_images = AttachedImage.objects.filter(content_type_id=content_type, object_id=ct_object.id)
        serializer = AttachedImageSerializer(attached_images, many=True)
        return serializer.data

    def get_outcome_computed(self,obj):
        if obj.outcome in [PATIENT_WAS_CURED, PATIENT_IMPROVED]:
            return 'Improved'
        if obj.outcome in [PATIENT_CONDITION_UNCHANGED, UNKNOWN_OUTCOME]:
            return 'Undetermined'
        if obj.outcome in [PATIENT_DETERIORATED, PATIENT_DIED, TREATMENT_TERMINATED]:
            return 'Deteriorated'

    def get_how_diagnosis(self,report):
        return [{"value":hd } for hd in report.how_diagnosis]

    def get_why_new_way(self,report):
        return [{"value":wnw } for wnw in report.why_new_way]

    def get_how_outcome(self,report):
        result = []
        if report.how_outcome:
            result = [{"value": ho} for ho in report.how_outcome]
        return result

    class Meta:
        model = Report
        fields = '__all__'
        depth = 1
