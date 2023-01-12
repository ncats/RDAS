"""
To change settings file:
`DJANGO_ENV=production python manage.py runserver`
"""

import importlib

from os import environ
from os.path import isfile


# https://stackoverflow.com/questions/43059267/how-to-do-from-module-import-using-importlib
def ugly_hack(module_name):
    """
    To make settings a little more flexible and be able to load any DJANGO_ENV
      modules there might be it was necessary to implement "from module import *"
      for variable modules.
    """

    # get a handle on the module
    mdl = importlib.import_module(module_name)

    # is there an __all__?  if so respect it
    if "__all__" in mdl.__dict__:
        names = mdl.__dict__["__all__"]
    else:
        # otherwise we import all names that don't begin with _
        names = [x for x in mdl.__dict__ if not x.startswith("_")]

    # now drag them in
    globals().update({k: getattr(mdl, k) for k in names})


environ.setdefault('DJANGO_ENV', 'development')
_ENV = environ['DJANGO_ENV']

# Select the right env:
environment = 'server.settings.{0}'.format(_ENV)
ugly_hack(environment)

# Optionally override some settings:
if isfile('./local.py'):
    ugly_hack('server.settings.local')
