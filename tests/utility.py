from unittest import *
import subprocess as sp
import botocore

BUCKET_NAME = 'wow'
BUCKET_PATH = '/tempZone/home/rods/wow/test/'
USER = 'rods'


def touch_file(filename):
    sp.run(["itouch", f"{BUCKET_PATH}{filename}"], check=True)


def set_access(filename, access_level):
    sp.run(["ichmod", "-M", access_level, USER, BUCKET_PATH+filename])


def read_file(client, filename):
    print(filename)
    return client.get_object(Bucket=BUCKET_NAME, Key=filename)
