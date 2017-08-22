import os
import random
import string
import datetime
import pytz
import botocore
import s3client


__author__ = 'Junya Kaneko <jyuneko@hotmail.com>'


def _split_name(name):
    splitted_name = name.split('.')
    if len(splitted_name) == 1:
        return name, ''
    else:
        return ''.join(splitted_name[:-1]), splitted_name[-1]


def _generate_versioned_name(basename, version=None, length=48):
    name, extension = _split_name(basename)
    if version is None:
        version = ''.join([random.choice(string.ascii_uppercase + string.digits) for _ in range(48)])
    return name + '_' + version + '.' + extension


def _generate_path(basedir, basename, versioned=True, version=None, length=48, retry=100):
    for i in range(retry):
        if versioned:
            basename = _generate_versioned_name(basename, version, length)
        path = os.path.join(basedir, basename)
        if version is not None or not os.path.exists(path):
            return path
    return None


class S3File:
    def __init__(self, s3path, cache_dir=s3client._conf['cache_dir'], cache_name=None,
                 auto_remote_update=True, auto_remove_cache=True):
        self.s3path = s3client.path.abspath(s3path)
        self.cache_dir = os.path.abspath(cache_dir)
        self.cache_name = cache_name
        self.fd = None
        self.auto_remote_update = auto_remote_update
        self.auto_remove_cache = auto_remove_cache
        
    @property
    def cache_path(self):
        if self.cache_name is not None:
            return os.path.join(self.cache_dir, self.cache_name)
        else:
            self.init_cache_path()
            return self.cache_path

    def init_cache_path(self):
        self.cache_path = _generate_path(self.cache_dir, s3client.path.basename(self.s3path))

    @cache_path.setter
    def cache_path(self, path):
        self.cache_dir = os.path.dirname(path)
        self.cache_name = os.path.basename(path)

    def get_newer(self):
        remote_exists = s3client.path.exists(self.s3path)
        cache_exists = os.path.exists(self.cache_path)

        if remote_exists:
            if not cache_exists:
                return 'remote'
            else:
                s3obj = s3client._s3.Object(s3client._conf['bucket'], self.s3path[1:])
                s3obj.load()
                if s3obj.last_modified < datetime.datetime.fromtimestamp(
                    os.stat(self.cache_path).st_mtime).replace(tzinfo=pytz.UTC):
                    return 'cache'
                else:
                    return 'remote'
        elif cache_exists:
            return 'cache'
        else:
            return None

    @property
    def is_cache_up_to_date(self):
        if self.get_newer() == 'cache':
            return True
        else:
            return False

    @property
    def is_remote_up_to_date(self):
        if self.get_newer() == 'remote':
            return True
        else:
            return False
        
    def update_cache(self):
        s3client._s3.Object(s3client._conf['bucket'], self.s3path[1:])\
                    .download_file(self.cache_path)

    def remove_cache(self):
        if os.path.exists(self.cache_path):
            os.remove(self.cache_path)
        self.cache_name = None

    def update_remote(self):
        if s3client.path.isdir(s3client.path.dirname(self.s3path)):
            s3client._s3.Object(s3client._conf['bucket'], self.s3path[1:])\
                        .upload_file(self.cache_path)
        else:
            raise FileNotFoundError(s3client.path.dirname(self.s3path))

    def open(self, mode='r', buffering=-1, encoding=None, errors=None, \
             newline=None, closefd=True, opener=None, force_update_cache=False):
        if self.is_remote_up_to_date or force_update_cache:
            self.update_cache()
        self.fd = open(self.cache_path, mode, buffering, encoding, errors, newline, closefd, opener)
        return self

    def close(self, force_update_remote=True):
        self.fd.close()
        if self.auto_remote_update and self.is_cache_up_to_date and \
           (self.fd.mode.find('w') != -1 or self.fd.mode.find('+')) or foce_update_remote:
            '''TODO: Prevent Unnecessary upload'''
            self.update_remote()
            
    def __getattr__(self, name):
        try:
            print(name)
            return getattr(self.fd, name)
        except AttributeError:
            raise AttributeError('S3File does not have attribute %s' % name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        if self.auto_remove_cache:
            self.remove_cache()
        
