import os
from pathlib import Path
import toml
import boto3
from s3client import path as s3path


__author__ = 'Junya Kaneko <jyuneko@hotmail.com>'


_cwd = []


s3conf = toml.load(os.path.join(str(Path.home()), '.s3client'))


class S3Client:    
    def __init__(self, **kwargs):
        session = boto3.session.Session(**kwargs)
        self._s3 = session.resource('s3')

    def __getattr__(self, name):
        try:
            return getattr(self._s3, name)
        except AttributeError:
            raise AttributeError
            

_s3client = S3Client()


def set_new_client(**kwargs):
    global _s3client
    _s3client = S3Client(**kwargs)

    
def getcwd():
    return os.path.join('/', '/'.join(_cwd))


def chdir(path):
    global _cwd
    _cwd = s3path.abspath(path).split('/')
    _cwd.remove('')

    
def chbucket(bucket_name):
    global s3conf
    s3conf['bucket'] = bucket_name
