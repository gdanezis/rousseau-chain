import os.path
import os
import re
import fnmatch

from paver.tasks import task, cmdopts
from paver.easy import sh, needs, pushd
from paver.virtual import *

@task
def docs(quiet=False):
    """ Build the documentation. """
    sh('cd docs; make html')


@task
def test():
    """ Run all the unit tests in a generic py.test context. """
    print("Generic Unit tests")
    sh('py.test -vs --doctest-modules tests/test_*.py hippiehug/*.py')

@task
def build(quiet=True):
    """ Builds the distribution, ready to be uploaded to pypi. """
    print("Build dist")
    sh('python setup.py sdist bdist bdist_wheel', capture=quiet)

@task
def win(quiet=True):
    """ Builds the binary distribution for windows. """
    print("Build windows distribution")
    sh('python setup.py build bdist_wininst', capture=quiet)

@task
def upload(quiet=False):
    """ Uploads the latest distribution to pypi. """
    
    lib = file(os.path.join("hippiehug", "__init__.py")).read()
    v = re.findall("VERSION.*=.*['\"](.*)['\"]", lib)[0]

    print("upload dist: %s" % v)
    sh('python setup.py sdist bdist')
    sh("twine upload dist/*%s*" % v)
    
