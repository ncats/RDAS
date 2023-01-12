from rest_framework.renderers import JSONRenderer


# TODO: DEFAULT_RENDERER_CLASSES in settings instead of renderer_classes in ViewSets
class CustomRenderer(JSONRenderer):

  def render(self, data, accepted_media_type=None, renderer_context=None):
    if type(data) == dict:
        data['api_version'] = 'v2.0'
    return super().render(data, accepted_media_type, renderer_context)
