# -*- coding: utf-8 -*-

"""AWS-KMS powered encryption/decryption"""

from __future__ import absolute_import, unicode_literals
from base64 import b64decode, b64encode

import boto3


KPLER_AWS_REGION = 'eu-west-1'
# be gentle with KMS requests
DECRYPTED_VALUE_CACHE = {}
KMS_KEY_ID = '20063843-83d9-4211-8795-70bf3b2749d9'
DEFAULT_DECRYPT_PATTERN = '*.crypt'


def decrypt(value):
    """Decrypt the given token using AWS KMS.

    Only users authorized by their AWS credentials will be granted with the
    true value of the encrypted token given.

    """
    global DECRYPTED_VALUE_CACHE

    if value in DECRYPTED_VALUE_CACHE.keys():
        return DECRYPTED_VALUE_CACHE[value]

    # first time we see it, let's ask kms to decrypt it
    kms = boto3.client('kms', region_name=KPLER_AWS_REGION)
    cipher = b64decode(value)
    DECRYPTED_VALUE_CACHE[value] = kms.decrypt(CiphertextBlob=cipher)['Plaintext']

    return DECRYPTED_VALUE_CACHE[value]


def encrypt(key_id, value):
    """Helper to encrypt initial value.

    Typical case would be to initially generate the token to set in
    configuration files, requirements, ...

    NOTE: no global cache is used here since we it seems one only need to use
          this method punctually.

    """
    kms = boto3.client('kms', region_name=KPLER_AWS_REGION)
    response = kms.encrypt(KeyId=key_id, Plaintext=value)
    # NOTE we could take a look at response['HTTPStatusCode']
    return b64encode(response['CiphertextBlob'])
