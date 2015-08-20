'''shove fabfile'''

from fabric.api import local


def release():
    '''release shove'''
    local('python setup.py bdist_wheel sdist --format=gztar,bztar,zip upload')