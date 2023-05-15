#!/usr/bin/env python

"""
python manage.py action filename

"""
import logging
import json
import sys
import argparse
import boto3
from pathlib import Path, PosixPath
from botocore.exceptions import ClientError

# Logger ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',
)
log = logging.getLogger()

# s3 ------------------------------------------------------------------
def create_bucket(name, region=None):
    region = region or 'us-east-2'
    client = boto3.client('s3', region_name=region)
    params = {
        'Bucket': name,
        'CreateBucketConfiguration': {
            'LocationConstraint': region,
        }
    }

    try:
        client.create_bucket(**params)
        return True
    except ClientError as err:
        log.error(f'{err} - Params {params}')
        return False


def get_bucket(name, create=False, region=None):
    client = boto3.resource('s3')
    bucket = client.Bucket(name=name)
    if bucket.creation_date:
        return bucket
    else:
        if create:
            create_bucket(name, region=region)
            return get_bucket(name)
        else:
            log.warning(f'Bucket {name} does not exist!')
            return


def list_buckets():
    s3 = boto3.resource('s3')

    count = 0
    for bucket in s3.buckets.all():
        print(bucket.name)
        count += 1
    print(f'Found {count} buckets!')


def create_bucket_object(bucket_name, file_path, key_prefix=None):
    bucket = get_bucket(bucket_name)
    dest = f'{key_prefix or ""}{file_path}'
    bucket_object = bucket.Object(dest)
    bucket_object.upload_file(Filename=file_path)
    return bucket_object


def get_bucket_object(bucket_name, object_key, dest=None, version_id=None):
    bucket = get_bucket(bucket_name)
    params = {'Key': object_key}
    if version_id:
        params['VersionId'] = version_id
    bucket_object = bucket.Object(**params)
    dest = Path(f'{dest or ""}')
    file_path = dest.joinpath(PosixPath(object_key).name)
    bucket_object.download_file(f'{file_path}')
    return bucket_object, file_path


def enable_bucket_versioning(bucket_name):
    bucket = get_bucket(bucket_name)
    versioned = bucket.Versioning()
    versioned.enable()
    return versioned.status


def delete_bucket_objects(bucket_name, key_prefix=None):
    bucket = get_bucket(bucket_name)
    objects = bucket.object_versions
    if key_prefix:
        objects = objects.filter(Prefix=key_prefix)
    else:
        objects = objects.iterator()

    targets = []
    for obj in objects:
        targets.append({
            'Key': obj.object_key,
            'VersionId': obj.version_id,
        })

    bucket.delete_objects(Delete={
        'Objects': targets,
        'Quiet': True,
    })

    return len(targets)


def delete_buckets(name=None):
    count = 0
    if name:
        bucket = get_bucket(name)
        if bucket:
            bucket.delete()
            bucket.wait_until_not_exists()
            count += 1
    else:
        count = 0
        client = boto3.resource
        client = boto3.resource('s3')
        for bucket in client.buckets.all():
            try:
                bucket.delete()
                bucket.wait_until_not_exists()
                count += 1
            except ClientError as err:
                log.warning(f'Bucket {bucket.name}: {err}')

    return count

# inputs------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='Commands')

    sp_create_bucket = subparsers.add_parser('create_bucket', help='Create bucket')
    sp_create_bucket.add_argument('bucket_name', help='Name of the bucket')
    sp_create_bucket.set_defaults(func=create_bucket)

    sp_get_bucket = subparsers.add_parser('get_bucket', help='Get bucket')
    sp_get_bucket.add_argument('bucket_name', help='Name of the bucket')
    sp_get_bucket.set_defaults(func=get_bucket)

    sp_list_buckets = subparsers.add_parser('list_buckets', help='List buckets')
    sp_list_buckets.set_defaults(func=list_buckets)

    sp_create_bucket_object = subparsers.add_parser('create_bucket_object', help='Create bucket object')
    sp_create_bucket_object.add_argument('bucket_name', help='Name of the bucket')
    sp_create_bucket_object.add_argument('file_path', help='Path to the file')
    sp_create_bucket_object.add_argument('--key_prefix', help='Prefix for the object key')
    sp_create_bucket_object.set_defaults(func=create_bucket_object)

    sp_get_bucket_object = subparsers.add_parser('get_bucket_object', help='Get bucket object')
    sp_get_bucket_object.add_argument('bucket_name', help='Name of the bucket')
    sp_get_bucket_object.add_argument('object_key', help='Key of the object')
    sp_get_bucket_object.add_argument('--dest', help='Destination directory to save the object')
    sp_get_bucket_object.add_argument('--version_id', help='Version ID of the object')
    sp_get_bucket_object.set_defaults(func=get_bucket_object)

    sp_enable_bucket_versioning = subparsers.add_parser('enable_bucket_versioning', help='Enable bucket versioning')
    sp_enable_bucket_versioning.add_argument('bucket_name', help='Name of the bucket')
    sp_enable_bucket_versioning.set_defaults(func=enable_bucket_versioning)

    sp_delete_bucket_objects = subparsers.add_parser('delete_bucket_objects', help='Delete bucket objects')
    sp_delete_bucket_objects.add_argument('bucket_name', help='Name of the bucket')
    sp_delete_bucket_objects.add_argument('--key_prefix', help='Prefix for the object keys')
    sp_delete_bucket_objects.set_defaults(func=delete_bucket_objects)

    sp_delete_buckets = subparsers.add_parser('delete_buckets', help='Delete buckets')
    sp_delete_buckets.add_argument('bucket_names', nargs='*', help='Names of the buckets')
    sp_delete_buckets.set_defaults(func=delete_buckets)

    args = parser.parse_args()

    if hasattr(args, 'func'):
        func = args.func
        if func == create_bucket:
            create_bucket(args.bucket_name, region=None)
        elif func == list_buckets:
            list_buckets()
        elif func == get_bucket:
            get_bucket(args.bucket_name, region=args.region)
        elif func == create_bucket_object:
            create_bucket_object(args.bucket_name, args.file_path, args.key_prefix)
        elif func == get_bucket_object:
            get_bucket_object(args.bucket_name, args.object_key, args.dest, args.version_id)
        elif func == enable_bucket_versioning:
            enable_bucket_versioning(args.bucket_name)
        elif func == delete_bucket_objects:
            delete_bucket_objects(args.bucket_name, args.key_prefix)
        elif func == delete_buckets:
            delete_buckets(args.name)
            
        print('Done')
    else:
        parser.print_help()
