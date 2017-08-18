import os
from pathlib import Path
import toml
import boto3
from s3client import path as s3path


__author__ = 'Junya Kaneko <jyuneko@hotmail.com>'


__cwd = []

_session = None
_s3 = None
_bucket = None


if os.path.exists('.s3client'):
    _conf = toml.load('.s3client')
else:
    _conf = toml.load(os.path.join(str(Path.home()), '.s3client'))


def init(**kwargs):
    global _session, _s3, _bucket
    _session = boto3.session.Session(**kwargs)
    _s3 = _session.resource('s3')
    _bucket = _s3.Bucket(_conf['bucket'])


init()
    

def getcwd():
    return os.path.join('/', '/'.join(__cwd))


def chdir(path):
    global __cwd
    __cwd = s3path.abspath(path).split('/')
    __cwd.remove('')

    
def chbucket(bucket_name):
    global _conf, _bucket
    _conf['bucket'] = bucket_name
    _bucket = _s3.Bucket(_conf['bucket'])
    chdir('/')


def listdir(path='.'):
    path = s3path.abspath(path)
    if path == '/':
        path = ''
        prefix = ''
        objects = _bucket.objects.all()
    else:
        path = path[1:]
        prefix = path + '/'
        objects = _bucket.objects.filter(Delimiter='/', Prefix=prefix)

    object_names = set([obj.key.replace(prefix, '').split('/')[0] for obj in objects])
    if '' in object_names:
        object_names.remove('')
    return tuple(object_names)

def mkdir(path):
    raise NotImplementedError


def remove(path):
    raise NotImplementedError


def removedirs(path):
    raise NotImplementedError


def rename(path):
    raise NotImplementedError


def renames(path):
    raise NotImplementedError


def replace(path):
    raise NotImplementedError


def rmdir(path):
    raise NotImplementedError
