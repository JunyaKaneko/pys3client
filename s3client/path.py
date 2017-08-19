import os
import botocore
import s3client


__author__ = 'Junya Kaneko <jyuneko@hotmail.com>'


def abspath(path, root_delimiter=True):
    path = os.path.normpath(os.path.join(s3client.getcwd(), path))
    if root_delimiter:
        return path
    else:
        return path[1:]


def kind(path):
    path = abspath(path)
    
    if path == '/':
        try:
            s3client._s3.meta.client.head_bucket(Bucket=s3client._conf['bucket'])
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return FileNotFoundError
        return 'dir'
    
    path = path[1:]
    
    try:
        s3client._s3.Object(s3client._conf['bucket'], path).load()
    except botocore.exceptions.ClientError as e:
        keys = s3client._bucket.objects.filter(MaxKeys=1, Prefix=path + '/')
        if sum(1 for _ in keys):
            return 'dir'
        else:
            return FileNotFoundError
    return 'file'


def exists(path):
    try:
        kind(path)
        return True
    except FileNotFoundError:
        return False


def isdir(path):
    try:
        if kind(path) == 'dir':
            return True
        else:
            return False
    except FileNotFoundError:
        return False
    
    
def isfile(path):
    try:
        if kind(path) == 'file':
            return True
        else:
            return False
    except FileNotFoundError:
        return False


def basename(path):
    return os.path.basename(path)


def dirname(path):
    return os.path.dirname(path)
