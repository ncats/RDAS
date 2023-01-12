from celery import Celery
from django.conf import settings

import os


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "server.settings.prod")
app = Celery("server")

app.config_from_object("django.conf:settings")
app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)


@app.task(bind=True)
def debug_task(self):
        print("task::Request: {0!r}".format(self.request))
