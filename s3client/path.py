import os
import botocore
import s3client


__author__ = 'Junya Kaneko <jyuneko@hotmail.com>'


def abspath(path):
    return os.path.normpath(os.path.join(s3client.getcwd(), path))


def exists(path):
    path = abspath(path)[1:]
    try:
        s3client._s3.Object(s3client._conf['bucket'], path).load()
    except botocore.exceptions.ClientError as e:
        keys = s3client._bucket.objects.filter(Delimiter='/', MaxKeys=1, Prefix=path + '/')
        if sum(1 for _ in keys):
            return True
        else:
            return False
    return True


def dirname(path):
    from_root = False
    if path[0] == '/':
        from_root = True
        path = path[1:]
    path = '/'.join(path.split[:-1])
    if from_root:
        path = '/' + path
    return os.path.normpath(path)
