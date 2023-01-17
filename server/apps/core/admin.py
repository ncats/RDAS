from django.contrib import admin
from django.core.exceptions import PermissionDenied
from django.contrib.admin.actions import delete_selected as delete_selected_
from .constants import ADMIN_ITEMS_PER_PAGE, REJECTED
from .models import *
from django.contrib.admin.actions import delete_selected
from django.db.models import Q, Exists, Case, When, Value, OuterRef
from django.urls import path
from django.contrib import admin
from django.template.response import TemplateResponse
from server.apps.core.constants import *
from django.contrib.sessions.models import Session
from rest_framework.authtoken.models import Token
from fcm_django.models import FCMDevice
from server.apps.api.v2.image_review import create_presigned_url, approve_disapprove_image
from urllib.parse import urlparse
from server.apps.api.v2.promotion_email import promotion_email

class MyAdminSite(admin.AdminSite):
    def image_review_page(self,request):
        images_to_review = AttachedImage.objects.filter(reviewed=False).values('id','real_name', 'caption')

        for image in images_to_review:
            if 'c-path-cureid-ncats' in image['real_name']:
                key = urlparse(image['real_name']).path
                temp_url = create_presigned_url(key.lstrip('/'))
                image['real_name'] = temp_url


        context={
            "images_for_review": images_to_review
        }

        return TemplateResponse(request, 'image_review.html', context=context)

    def get_urls(self):
        urls = super().get_urls()
        my_urls = [
            path(
                "image-review/",
                self.image_review_page,
                name="image-review",
            ),
            path(
                "image-review/image-approval",
                approve_disapprove_image,
                name="image-approval",
            ),
            path(
                "promotion-email",
                promotion_email,
                name='promotion-email'
            )
        ]
        return my_urls+urls


@admin.action(description='Approve selected')
def approved_selected(modeladmin, request, queryset):
    queryset.update(status='Approved')


@admin.action(description='Delete selected')
def delete_selected(modeladmin, request, queryset):
    if not modeladmin.has_delete_permission(request):
        raise PermissionDenied
    if request.POST.get('post'):
        for obj in queryset:
            if type(obj) == CureReport:
                obj.status = "Deleted"
            else:
                obj.deleted = True
            obj.save()
    else:
        return delete_selected_(modeladmin, request, queryset)
delete_selected.short_description = "Delete selected objects"
admin.site.disable_action('delete_selected')


class DiseaseAdmin(admin.ModelAdmin):
    model = Disease
    list_per_page = ADMIN_ITEMS_PER_PAGE
    list_display = [ "name", "meddra", "image_name" ]
    search_fields = [ "name", "synonyms", "transmitted_by" ]

    def get_queryset(self, request):
        return Disease.objects.all().order_by("-name")

    def has_delete_permission(self, request, obj=None):
        return False


class DrugAdmin(admin.ModelAdmin):
    list_per_page = ADMIN_ITEMS_PER_PAGE
    search_fields = ['name']
    list_display = ['name']

    def get_queryset(self, request):
        return Drug.objects.all().order_by('name')

    def has_delete_permission(self, request, obj=None):
        return False


class DiscussionAdmin(admin.ModelAdmin):
    list_per_page = 25
    model = Discussion
    actions = [delete_selected, approved_selected]
    list_display = [
        'id',
        'disease',
        'author',
        'updated',
        'status',
    ]

    search_fields = ['id','title','status' 'disease__name']

    def get_queryset(self, request):
        return self.model.objects.order_by('-updated', '-created')

    def has_delete_permission(self, request, obj=None):
        return False


class ClinicalTrialAdmin(admin.ModelAdmin):
    list_per_page = 25
    model = ClinicalTrial
    actions = [delete_selected, approved_selected]
    list_display = [
        'id',
        'title',
        'disease',
        'clinical_trials_gov_id',
        'status',
    ]
    search_fields = ['id','title', 'clinical_trials_gov_id', 'disease__name']
    readonly_fields = ['matched_against', ]

    def get_queryset(self, request):
        return self.model.objects.order_by('-updated', '-created')

    def get_search_results(self, request, queryset, search_term):
        queryset, use_distinct = super().get_search_results(request, queryset, search_term)
        # Don't understand why I couldn't just ADD the filter to queryset,
        # but it just wouldn't work eventhough the SQL looked correct
        queryset = ClinicalTrial.objects.filter(
                Q(disease__name__icontains=search_term) |
                Q(title__icontains=search_term) |
                Q(clinical_trials_gov_id__icontains=search_term)
        )
        return queryset, use_distinct

    def has_delete_permission(self, request, obj=None):
        return False


class ArticleAdmin(admin.ModelAdmin):
    list_per_page = ADMIN_ITEMS_PER_PAGE
    model = Article
    actions = [delete_selected, approved_selected]
    list_display = ['id', 'disease', 'title', 'status','pubmed_id', 'published', 'publication_type']
    list_filter = ('publication_type',)
    search_fields = ['id', 'title', 'article_url', 'status', 'disease__name', 'pubmed_id']

    def has_delete_permission(self, request, obj=None):
        return False


#class NewsArticle(Article):
#    class Meta:
#        proxy = True


#class NewsArticleAdmin(ArticleAdmin):
#    def get_queryset(self, request):
#        return self.model.objects.filter(publication_type__iexact='news').order_by('-updated', '-created')


#class JournalArticle(Article):
#    class Meta:
#        proxy = True


#class JournalArticleAdmin(ArticleAdmin):
#    def get_queryset(self, request):
#        return self.model.objects.filter(publication_type__iexact='journal').order_by('-updated', '-created')


class EventAdmin(admin.ModelAdmin):
    list_per_page = ADMIN_ITEMS_PER_PAGE
    model = Event
    actions = [delete_selected, approved_selected]
    list_display = [
        'id',
        'author',
        'status',
        'contact',
        'event_description',
        'event_sponsor',
        'location',
        'url'
    ]
    list_editable = [
        'event_description',
        'event_sponsor',
        'location',
        'url'
    ]
    search_fields = ('id','url', 'status',)

    def get_queryset(self, request):
        return self.model.objects.order_by('-updated', '-created')

    def has_delete_permission(self, request, obj=None):
        return False


class NeonateInline(admin.StackedInline):
    model = Neonate
    extra = 0


class ComorbidityInline(admin.StackedInline):
    model = Comorbidity.comorbidities.through
    extra = 0
# class PregnancyInline(admin.StackedInline):
#     model = Pregnancy.pregnancy.through
#     extra = 0
#     inlines = ['NeonateInline']


class PatientAdmin(admin.ModelAdmin):
    list_per_page = ADMIN_ITEMS_PER_PAGE
    model = Patient
    actions = [delete_selected,]
    list_display = ['id', 'pregnant']
    filter_horizontal = (
        'comorbidity',
    )


class DrugInLine(admin.StackedInline):
    model = Drug.reports.through
    extra = 0
    fieldsets = (
        (None, {
            'fields': ('drug', 'report', 'dose', 'frequency', 'route',
                    'duration', 'severity', 'severity_detail',),
        }),
    )


class ReportInline(admin.TabularInline):
    model = CureReport
    extra = 3


class OrganismInLine(admin.StackedInline):
    model = Organism.reports_organisms.through
    extra = 1


class ReportAdmin(admin.ModelAdmin):
    list_per_page = ADMIN_ITEMS_PER_PAGE
    model =  Report
    actions = [delete_selected, approved_selected]
    list_display = [
        'id',
        'report_status',
        'report_author',
        'report_updated',
        'disease',
        'article_title',
        'article_pubmed_id',
    ]

    list_select_related = ('patient',)

    # def get_queryset(self, request):
    #     return self.model.objects.order_by('-updated', '-created')

    def report_status(self,instance):
        return instance.reports.status

    def report_author(self, instance):
        return instance.reports.author.get_full_name()

    def report_updated(self, instance):
        return instance.reports.updated

    def article_title(self, instance):
        if not instance.article:
            return ""
        return shorten_text(instance.article.title, 100)

    def article_pubmed_id(self, instance):
        if not instance.article:
            return ""
        return instance.article.pubmed_id

    inlines = [ReportInline, DrugInLine,]
    search_fields = ('article__pubmed_id', 'regimens__drug__name', 'disease__name','id',)

    filter_horizontal = (
        'organisms', 'resistant_drugs',
        'previous_drugs', 'cross_linked_diseases',
    )

    # fieldsets = (
    #     (Patient, {'fields':(
    #             'patient','surgery','patient.pregnant'
    #     )
    #     }),
    # )

    def has_delete_permission(self, request, obj=None):
        return False


class ProfileAdmin(admin.ModelAdmin):
    raw_id_fields = ['user', ]
    model = Profile
    list_per_page = ADMIN_ITEMS_PER_PAGE
    list_display = [
        'user',
    ]
    list_filter = [
        'created'
    ]
    search_fields = ['user__email', 'user__first_name', 'user__last_name']
    filter_horizontal = ('favorited_diseases', 'favorited_drugs' )

    fieldsets = (
        (None, { 'fields': (
                'user', 'status', 'title', 'institution',
                'qualification', 'country', 'favorited_diseases',
                'favorited_drugs', 'favorited_discussions',
                'favorited_reports', 'favorited_articles', 'favorited_events', 'favorited_clinical_trials','notifications')
               }
        ),
    )

    def has_delete_permission(self, request, obj=None):
        return False


class NeonateInline(admin.StackedInline):
    model=Neonate
    extra = 1


class PregnancyAdmin(admin.ModelAdmin):
    model = AttachedImage
    list_per_page = ADMIN_ITEMS_PER_PAGE
    list_display = ['treatment_gestational_age', 'delivery_gestational_age', 'outcome']
    inlines = [NeonateInline]

    def has_delete_permission(self, request, obj=None):
        return False


class AttachedImageAdmin(admin.ModelAdmin):
    model = AttachedImage
    list_per_page = ADMIN_ITEMS_PER_PAGE
    list_display = ('real_name', 'caption', 'reviewed', 'reviewer')
    list_filter = ('reviewed', )
    readonly_fields = ('url', 'reviewer', 'reviewed', )


class PhotoCreditAdmin(admin.ModelAdmin):
    model = PhotoCredit
    list_per_page = ADMIN_ITEMS_PER_PAGE
    list_display = ('disease', 'title', 'author')
    search_fields = ('disease__name', 'author')

    def has_delete_permission(self, request, obj=None):
        return False


class UserProposedArticleAdmin(admin.ModelAdmin):
    model = UserProposedArticle
    list_per_page = ADMIN_ITEMS_PER_PAGE
    list_display = ('pubmed_id', 'article_url', 'author', 'needs_review')

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        queryset = queryset.annotate(
            _needs_review=Case(
                When(status=REJECTED, then=Value(True)),
                When(
                    pubmed_id__isnull=False,
                    article_url__isnull=False,
                    then=Exists(Article.objects.filter(Q(pubmed_id=OuterRef('pubmed_id')) | Q(article_url=OuterRef('article_url'))))
                ),
                When(
                    pubmed_id__isnull=False,
                    article_url__isnull=True,
                    then=Exists(Article.objects.filter(pubmed_id=OuterRef('pubmed_id')))
                ),
                When(
                    article_url__isnull=False,
                    then=Exists(Article.objects.filter(article_url=OuterRef('article_url')))
                ),
                default=Value(False)
            )
        ).order_by('_needs_review')
        return queryset

    def has_delete_permission(self, request, obj=None):
        return False


admin.autodiscover()
admin_site = MyAdminSite()
# admin_site.enable_nav_sidebar = False
admin_site.site_header = 'CURE ID Admin'
admin_site.index_title = 'CURE ID Admin'
admin_site.register(FCMDevice)
admin_site.register(Token)
admin_site.register(User)
admin_site.register(AttachedImage, AttachedImageAdmin)
admin_site.register(Disease, DiseaseAdmin)
admin_site.register(Drug, DrugAdmin)
admin_site.register(Discussion, DiscussionAdmin)
admin_site.register(ClinicalTrial, ClinicalTrialAdmin)
admin_site.register(Article,ArticleAdmin)
admin_site.register(Event, EventAdmin)
admin_site.register(Organism)
admin_site.register(Comorbidity)
admin_site.register(Profile, ProfileAdmin)
admin_site.register(Report, ReportAdmin)
admin_site.register(Patient, PatientAdmin)
admin_site.register(PhotoCredit, PhotoCreditAdmin)
admin_site.register(Pregnancy)
admin_site.register(UserProposedArticle, UserProposedArticleAdmin)
