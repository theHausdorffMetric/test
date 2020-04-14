"""AWS S3 interfaces."""

from contextlib import contextmanager
from io import BytesIO
import json
import logging

import boto3
from botocore.exceptions import ClientError

from kp_scrapers.lib.compression import gzip_uncompress
from kp_scrapers.lib.services.shub import global_settings as Settings


logger = logging.getLogger(__name__)
logging.getLogger('botocore').setLevel(logging.CRITICAL)
logging.getLogger('boto3').setLevel(logging.CRITICAL)


@contextmanager
def connect_to_s3(config=None):
    """Wraps S3 conn with defaults and error handler.

    Args:
        config(dict): AWS credentials defined like standard ENV

    """
    settings = config or Settings()
    logger.debug('Connecting to S3 service')

    try:
        yield boto3.resource(
            's3',
            aws_access_key_id=settings['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=settings['AWS_SECRET_ACCESS_KEY'],
        )
    except ClientError as e:
        logger.error(f'Failed to connect to S3: {e}')


def iter_files(bucket_name, **kwargs):
    with connect_to_s3() as s3:
        bucket = s3.Bucket(bucket_name)
        yield from bucket.objects.filter(**kwargs)


def fetch_file(bucket_name, key_name, deserializer=json.loads, uncompress=False):
    # TODO handle exceptions, especially on bucket or key not found
    with connect_to_s3() as s3:
        logger.debug(f'Downloading S3 object: {bucket_name}/{key_name}')
        s3_object = s3.Object(bucket_name, key_name).get()

        if key_name.endswith('.gz') or uncompress:
            yield from gzip_uncompress(_download_fileobj(s3_object), deserialize=deserializer)

        else:
            yield _download_fileobj(s3_object)


def upload_blob(bucket, key, blob, serializer=json.dumps):
    """Dump the given json to the given s3 location."""
    data = serializer(blob)
    with connect_to_s3() as s3:
        return s3.Bucket(bucket).put_object(Key=key, Body=data)


def _download_fileobj(s3_obj):
    return BytesIO(s3_obj['Body'].read())
