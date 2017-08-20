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
    def __init__(self, s3path, cache_dir=s3client._conf['cache_dir'], cache_name=None):
        self.s3path = s3client.path.abspath(s3path)
        self.cache_dir = os.path.abspath(cache_dir)
        self.cache_name = cache_name
        self.fd = None

    @property
    def cache_path(self):
        return os.path.join(self.cache_dir, self.cache_name)

    def update_cache(self):
        cache_path = ''
        try:
            s3obj = s3client._s3.Object(s3client._conf['bucket'], self.s3path[1:])
            s3obj.load()

            if self.cache_name is not None:
                if s3obj.last_modified < datetime.datetime.fromtimestamp(
                        os.stat(self.cache_path).st_mtime).replace(tzinfo=pytz.UTC):
                    return
                else:
                    cache_path = self._cache_path
                    self.remove_cache()
            else:
                cache_path = _generate_path(self.cache_dir, s3client.path.basename(s3obj.key),
                                            version=s3obj.version_id)    
            s3obj.download_file(cache_path)
        except botocore.exceptions.ClientError as e:
            cache_path = _generate_path(self.cache_dir, s3client.path.basename(s3obj.key))
        self.cache_name = os.path.basename(cache_path)

    def remove_cache(self):
        os.remove(self.cache_path)
        self.cache_name = None

    def update_remote(self):
        if s3client.path.isdir(s3client.path.dirname(self.s3path)):
            s3client._s3.Object(s3client._conf['bucket'], self.s3path[1:]).upload_file(self.cache_path)
        else:
            raise FileNotFoundError(s3client.path.dirname(self.s3path))
        
    def open(self, mode='r', buffering=-1,
             encoding=None, errors=None, newline=None, closefd=True, opener=None):
        self.update_cache()
        self.fd = open(self.cache_path, mode, buffering, encoding, errors, newline, closefd, opener)
        return self

    def close(self, remove_cache=False, update_remote=True):
        self.fd.close()
        if remove_cache:
            self.remove_cache()
        if update_remote and (self.fd.mode.find('w') != -1 or self.fd.mode.find('+')):
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


        
