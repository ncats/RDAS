from django.urls import path
from rest_framework.routers import SimpleRouter

from .authentication import admin_login, logout, link_accounts, unlink_accounts
from .views import (
    get_unfinished_reports,
    get_all_reports,
    get_all_discussions,
    get_all_clinical_trials,
    get_all_articles,
    get_all_events,
    most_common_regimen,
    disease_drug_typeahead,
    get_reports_by_interest,
    get_cts_by_interest,
    get_discussions_by_interest,
    get_articles_by_interest,
    get_events_by_interest,
    get_favorite_reports,
    get_favorite_discussions,
    get_favorite_clinical_trials,
    get_favorite_articles,
    get_favorite_events,
    get_sponsors,
    get_countries,
    get_authors,
    get_years,
    search_diseases,
    get_tuberculosis_resistant_drugs,
    get_article_by_url_pubmed_id,
    public_profile,
    search_user,
    report_comment,
    untag_comment,
    add_user_proposed_article,
)

from server.apps.notifications.views import (
  clear_notifications,
  fcm_token,
  unseen_notifications,
  daily_digest_unsubscribe
)

from .viewsets import (
    clinical_trial,
    disease,
    drug,
    report,
    discussion,
    article,
    event,
    user,
    profile,
    comment,
    photo_credit,
    newsfeed,
    organism
)
from server.apps.ui_forms.views import get_pageinfo

app_name = 'API'
router = SimpleRouter(trailing_slash=False)

router.register('clinical-trials', clinical_trial.ClinicalTrialViewSet)
router.register('diseases', disease.DiseaseViewSet)
router.register('drugs', drug.DrugViewSet)
router.register('reports', report.ReportViewSet)
router.register('discussions', discussion.DiscussionViewSet)
router.register('profiles', profile.ProfileViewSet, 'profiles')
# I don't like the idea of exposing this
#router.register('users', user.UserViewSet)
router.register('events', event.EventViewSet)
router.register('articles', article.ArticleViewSet)
router.register('comments', comment.CommentViewSet)
router.register('photo-credits', photo_credit.PhotoCreditViewSet)
router.register('newsfeed', newsfeed.NewsfeedViewSet)
router.register('organisms', organism.OrganismViewSet)
urlpatterns = router.urls

urlpatterns = [
  path('login', admin_login),
  path('logout', logout),
  path('link-accounts', link_accounts),
  path('unlink-accounts', unlink_accounts),
  path('get-form-page', get_pageinfo, name='get-page-info'),
  path('unfinished-reports', get_unfinished_reports, name='unfinished-reports'),
  path('all-reports', get_all_reports, name='all-reports'),
  path('all-discussions', get_all_discussions, name='all-discussions'),
  path('all-clinical-trials', get_all_clinical_trials, name='all-clinical-trials'),
  path('all-articles', get_all_articles, name='all-articles'),
  path('all-events', get_all_events, name='all-events'),
  path('disease-treatments', most_common_regimen, name='disease-treatments'),
  path('drug-and-disease', disease_drug_typeahead, name='disease-drug-typeahead'),
  path("reports-by-disease", get_reports_by_interest, name="reports-by-interest"),
  path("cts-by-disease", get_cts_by_interest, name="cts-by-interest"),
  path("discussions-by-disease", get_discussions_by_interest, name="discussions-by-interest"),
  path("articles-by-disease", get_articles_by_interest, name="discussions-by-interest"),
  path("events-by-disease", get_events_by_interest, name="eve-by-interest"),
  path("favorite-reports", get_favorite_reports, name="favorite-reports"),
  path("favorite-discussions", get_favorite_discussions, name="favorite-discussions"),
  path("favorite-clinical-trials", get_favorite_clinical_trials, name="favorite-clinical-trials"),
  path("favorite-articles", get_favorite_articles, name="favorite-articles"),
  path("favorite-events", get_favorite_events, name="favorite-events"),
  path("fcm-token", fcm_token, name="fcm-token"),
  path("unseen-notifications", unseen_notifications, name="unseen-tokens"),
  path("clear-notifications", clear_notifications, name="clear-tokens"),
  path("authors", get_authors, name="authors"),
  path("countries", get_countries, name="countries"),
  path("sponsors", get_sponsors, name="sponsors"),
  path("years", get_years, name="years"),
  path("search/disease", search_diseases, name="search-diseases"),
  path("tuberculosis-resistant-drugs", get_tuberculosis_resistant_drugs, name='tuberculosis-resistant-drugs'),
  path("article-search", get_article_by_url_pubmed_id, name='article-url-pubmed_id'),
  path("users/<int:user_id>", public_profile, name='public-profile'),
  path("search-user", search_user, name='search-user'),
  path("report-comment/<int:comment_id>", report_comment, name='report-comment'),
  path("untag-comment/<int:comment_id>", untag_comment, name='untag-comment'),
  path("unsubscribe", daily_digest_unsubscribe, name='unsubscribe'),
  path("add-article", add_user_proposed_article, name="add-user-proposed-article"),
] + urlpatterns
