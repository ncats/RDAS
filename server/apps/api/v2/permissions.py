from rest_framework.permissions import BasePermission, SAFE_METHODS


class BaseCUREIDAccessPermission(BasePermission):
  fair_play = ['report', 'comment', 'discussion']


  def has_permission(self, request, view):
    if request.method in SAFE_METHODS:
        return True

    # TODO: not is_staff, is_superuser should probably be only able to create reports, comments, discussions
    if (request.method == 'POST' or request.method =='PATCH') and request.user and request.user.id and request.user.is_active:
        if request.user.is_superuser:
            return True

        if any(i in request.path for i in self.fair_play):
            return True

    return True
    # TODO: fix this before the relize
    return False


  # This works only for PUT, DELETE, GET
  def has_object_permission(self, request, view, obj):
    # TODO: fix this before the relize
    return True

    if request.method == 'GET' or request.user.is_superuser:
        return True

    # TODO: not superusers are only allowed to PUT/update or DELETE/ Reports,
    #   Comments and Discussions they are authors of. Maybe even only update
    if request.method == 'DELETE':
        return False

    if any(i in request.path for i in self.fair_play) and request.user == obj.author:
        return True

    return False
