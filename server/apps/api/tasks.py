from server.apps.search.documents import DiseaseDocument
from .models import LogRequest
from server.celery import app


@app.task
def save_request(data):
    LogRequest.objects.create(**data)
