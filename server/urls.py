"""server URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, re_path

from django.conf import settings
from django.conf.urls import include

from server.apps.api.v2 import routers
from server.apps.api.v2.authentication import admin_login
from django.conf.urls.static import static
from server.apps.core.admin import admin_site

admin.autodiscover()

urlpatterns = [
  path('admin/login/', admin_login),
  re_path(r'^admin/', admin_site.urls),
  path('v2/', include(routers, 'v2')),
]

if settings.DEBUG:  # pragma: no cover
  import debug_toolbar  # noqa: WPS433

  urlpatterns += [
    # URLs specific only to django-debug-toolbar:
    path('__debug__/', include(debug_toolbar.urls)),  # noqa: DJ05
  ] + static(settings.MEDIA_URL, document_root='')
