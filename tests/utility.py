from unittest import *
import subprocess as sp
import botocore

from typing import *

BUCKET_NAME = 'wow'
BUCKET_PATH = '/tempZone/home/rods/wow/test/'
USER = 'rods'


def touch_file(filename, access_level=None, contents: Optional[bytes] = None):
    if contents is None:
        sp.run(["itouch", f"{BUCKET_PATH}{filename}"], check=True)
    else:
        iput_com = sp.Popen(
            ['iput', '-', f"{BUCKET_PATH}{filename}"], stdin=sp.PIPE, shell=False)
        iput_com.stdin.write(contents)
        iput_com.stdin.close()
    if access_level is not None:
        set_access(filename, access_level)


def set_access(filename, access_level, recursive: bool = False):
    sp.run(["ichmod"]+(['-r'] if recursive else []) +
           ["-M", access_level, USER, BUCKET_PATH+filename])


def read_file(client, filename):
    print(filename)
    return client.get_object(Bucket=BUCKET_NAME, Key=filename)


def mkdir(dirname, access_level=None):
    sp.run(['imkdir', '-p', BUCKET_PATH+dirname], check=True)
    if access_level is not None:
        set_access(BUCKET_PATH+dirname, access_level, recursive=True)

def remove_file(client, filename, recursive: bool = False):
    """
    Remove a file or directory
    """
    sp.run(['irm'] + ([] if recursive else ['-r']) +
           [f'{BUCKET_PATH}{filename}{"/" if recursive else ""}'])
    if recursive:
        sp.run(['irmdir', f'{BUCKET_PATH}{filename}'])
