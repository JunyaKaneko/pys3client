import os
from pathlib import Path
import toml
import boto3
from s3client import path as s3path


__author__ = 'Junya Kaneko <jyuneko@hotmail.com>'


_cwd = []

_session = None
_s3 = None
_bucket = None

_conf = toml.load(os.path.join(str(Path.home()), '.s3client'))


def init(**kwargs):
    global _session, _s3, _bucket
    _session = boto3.session.Session(**kwargs)
    _s3 = session.resource('s3')
    _bucket = _s3.Bucket(_conf['bucket'])
    

def getcwd():
    return os.path.join('/', '/'.join(_cwd))


def chdir(path):
    global _cwd
    _cwd = s3path.abspath(path).split('/')
    _cwd.remove('')

    
def chbucket(bucket_name):
    global _conf, _bucket
    _conf['bucket'] = bucket_name
    _bucket = _s3.Bucket(_conf['bucket'])
