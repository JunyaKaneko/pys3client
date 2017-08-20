import os
from pathlib import Path
import toml
import boto3
from s3client import path as s3path


__author__ = 'Junya Kaneko <jyuneko@hotmail.com>'


__cwd = []

_session = None
_client = None
_s3 = None
_bucket = None


if os.path.exists('.s3client'):
    _conf = toml.load('.s3client')
else:
    _conf = toml.load(os.path.join(str(Path.home()), '.s3client'))


class S3ClientError(Exception):
    pass
    

def init(**kwargs):
    global _session, _client, _s3, _bucket
    _session = boto3.session.Session(**kwargs)
    _client = _session.client('s3')
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
        objects = _bucket.objects.filter(Prefix=prefix)
        if not sum(1 for _ in objects):
            raise FileNotFoundError('/' + path)

    object_names = set([obj.key.replace(prefix, '', 1).split('/')[0] for obj in objects])
    if '' in object_names:
        object_names.remove('')
    return tuple(object_names)


def mkdir(path):
    path = s3path.abspath(path)
    if s3path.exists(path) and s3path.isdir(path):
        raise FileExistsError(path)
    if not s3path.exists(s3path.dirname(path)):
        raise FileNotFoundError(path)
    _bucket.put_object(Key=path[1:] + '/')


def makedirs(path):
    path = s3path.abspath(path)
    if s3path.exists(path) and s3path.isdir(path):
        raise FileExistsError
    dirs = [path, ]
    dirname = path
    while True:
        dirname = s3path.dirname(dirname)
        if (s3path.exists(dirname) and s3path.isdir(dirname)) or dirname == '/':
            break
        else:
            dirs.insert(0, dirname)
    for d in dirs:
        mkdir(d)

        
def remove(path):
    path = s3path.abspath(path)
    if s3path.isfile(path):
        _bucket.delete_objects(Delete={'Objects': [{'Key': path[1:]}]})
    else:
        raise S3ClientError(path)

    
def rename(src, dist):
    src = s3path.abspath(src)
    dist = s3path.abspath(dist)
    if s3path.exists(dist):
        if s3path.isdir(dist) or (s3path.isfile(src) and s3path.isfile(dist)):
            raise S3ClientError
    if s3path.isdir(src):
        src = src + '/'
        if not s3path.exists(s3path.dirname(dist)):
            raise FileNotFoundError(s3path.dirname(dist))
        dist = dist + '/'
    objects = _bucket.objects.filter(Prefix=src[1:])
    print([obj.key for obj in objects])
    for obj in objects:
        new_key = obj.key.replace(src[1:], dist[1:], 1)
        _s3.Object(_conf['bucket'], new_key)\
           .copy_from(CopySource={'Bucket': _conf['bucket'], 'Key': obj.key})
        _s3.Object(_conf['bucket'], obj.key).delete()

    
def rmdir(path):
    global _bucket
    path = s3path.abspath(path)
    if s3path.isdir(path) and not listdir(path):
        _bucket.delete_objects(Delete={'Objects': [{'Key': path[1:] + '/'}]})
    else:
        raise S3ClientError(path)


def removedirs(path):
    def recursive_rmdir(path):
        for dir_name in listdir(path):
            dir_path =s3path.join(path, dir_name)
            if s3path.isdir(dir_path):
                recursive_rmdir(dir_path)
        rmdir(path)

    path = s3path.abspath(path)
    recursive_rmdir(path)
        
                           
from s3client import file
