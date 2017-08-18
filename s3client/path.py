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


def exists(path):
    path = abspath(path)
    
    if path == '/':
        try:
            s3client._s3.meta.client.head_bucket(Bucket=_conf['bucket'])
        except botocore.exceptions.ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                return False
            else:
                True
    
    path = path[1:]
    
    try:
        s3client._s3.Object(s3client._conf['bucket'], path).load()
    except botocore.exceptions.ClientError as e:
        keys = s3client._bucket.objects.filter(Delimiter='/', MaxKeys=1, Prefix=path + '/')
        if sum(1 for _ in keys):
            return True
        else:
            return False
    return True


def basename(path):
    return os.path.basename(path)


def dirname(path):
    return os.path.dirname(path)
