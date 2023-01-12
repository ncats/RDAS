from django.db import models

class LogRequest(models.Model):
    created = models.DateTimeField(
        auto_now_add=True,
        help_text="Datetime when the object was created."
    )
    # TODO: no idea whether firebase can change userids
    #   didn't make it as a ForeignKey field because we can have unauth requests
    user = models.CharField(
        max_length=128,
        null=True,
        blank=True,
        help_text="Username or none of the user making the request."
    )
    path = models.CharField(
        max_length=256,
        null=False,
        blank=False,
        help_text="Path being requested."
    )
    method = models.CharField(
        max_length=10,
        null=False,
        blank=False,
        help_text="Method of the request, for ex. GET, POST, etc"
    )
    arguments = models.TextField(
        help_text="Will hold values from QueryString or request.POST."
    )
