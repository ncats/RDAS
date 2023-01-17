from django.conf import settings
from django.http import JsonResponse
from os import path
from re import match

from .tasks import save_request

VERSIONS = ['v2']

class CUREMaintenanceMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        self.__save_request_info(request)

        if self.__under_maintenance():
            return JsonResponse({
                'message': 'Currently under maintenance, please come back later.',
                'status': 'error',
            }, status=503)
        elif self.__invalid_api_version(request):
            return JsonResponse({
                'message': f'Invalid API version, only {VERSIONS} are accepted.',
                'status': 'error'
            }, status=400)

        response = self.get_response(request)
        return response


    def __under_maintenance(self):
        return path.exists(path.join(settings.BASE_DIR, 'cure_maintenance_on'))


    def __invalid_api_version(self, request):
        path = request.path

        # Return false for other than api version urls
        pattern = r"^\/v\d+\.*\d*\/"
        if not match(pattern, path):
            return False

        requested_version = path[1:path.index("/", 2)]
        # TODO: expects versions to be in v2 or v2.1 form; situations with v2_2 not handled
        version_pattern = r'^v\d+\.*\d*$'
        return match(version_pattern, requested_version) and requested_version not in VERSIONS


    def __save_request_info(self, request):
        data = {
            'user': request.user.username,
            'path': request.path,
            'method': request.method,
        }

        args = []
        if request.GET:
            args = request.GET
        elif request.POST:
            args = request.POST
        data['arguments'] = args

        save_request.delay(data)
