# irods_client_s3_cpp
C++ S3 API for iRODS

We will implement a subset of the S3 API:
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html

Initial effort to include:
  - [x] CopyObject
  - CompleteMultipartUpload
  - CreateMultipartUpload
  - [x] DeleteObject
  - DeleteObjects
  - [x] GetObject
  - GetObjectAcl ? (Mapping the needs of S3 access control versus the iRODS access control is complicated)
  - GetObjectTagging (The way this translates into AVUs is also not entirely clear)
  - [x] HeadObject
  - ListObjects ?
  - [x] ListObjectsV2
  - [x] PutObject
  - PutObjectAcl ?
  - PutObjectTagging
  - UploadPart
  - UploadPartCopy ?

Goal is to support the equivalent of:
 - ils - aws s3 ls s3://bucketname/a/b/c/
 - iput - aws s3 cp localfile s3://bucketname/a/b/c/filename
 - iget - aws s3 cp s3://bucketname/a/b/c/filename localfile
 - irm - aws s3 rm s3://bucketname/a/b/c/filename
 - imv - aws s3 mv s3://bucketname/a/b/c/filename1 s3://bucketname/a/b/c/filename2

# Limitations (What's Missing?)

## Multipart Uploads

Multipart uploads are an important part of S3, however, they aren't simple to implement and have substantial performance
and storage implications for a naive approach. As of now, this bridge does not attempt to provide that functionality

## Tagging

iRODS does have its own metadata system, however it is not especially clear how it should map to S3 metadata, so it is not
included at the moment.

## Paging

Paging requires engineering work to provide paging through lists of objects efficient, so right now the bridge does not
attempt to paginate its output for things such as listobjects.

## Checksum handling

Amazon S3 provides many places to place checksums for the data as received by the server. iRODS provides MD5 checksums, 
however the bridge does not use that to verify data objects created through PutObject.

## ETags

ETags are not provided for or used consistently.

## Versioning

Versioning is not supported at this time.

# Setting it up <a id="Interesting_Part"/>

## Compilation

Ideally, the irods development packages and their dependencies will be sufficient to build this. You may
however have to set your environmental variables to make sure that they are found when CMake goes looking for them.

## Installation

## iRODS Connection

## The Configuration File

Well, once you've got this installed, however that looks at the moment,
the included plugins can be set up as such, in a config.json file in whatever
directory the `irods_s3_bridge` executable is executed.

First you must set up an iRODS client environment, logged into an account
with administrative permissions(This is not the permission S3 commands operate at)

```json
{
  "plugins": {
    "static_bucket_resolver": { // Each of these keys corresponds to the plugin's .so file name, minus the 'lib'-prefix
      "name": "static_bucket_resolver",// This is a convention
      "mappings": {
        "the-bucket": "/tempZone/home/rods/bucket/"//You address s3 uploads to the 'the-bucket' bucket
      }
    },
    "static_authentication_resolver": {
      "name": "static_authentication_resolver",
      "users": {
        "<username>": {// You log into the s3 with *this* username
          "username": "rods",// This is the irods username
          "secret_key": "<secret key>" // This is the secret key on the s3 side
        }
      }
    }
  },
  // This may be relevant to your performance if you have many rules in your iRODS installation
  "resource": "The_Fastest_iRODS_Resource",
  // This is automatically set to 3 times the number of processors if this value is not specified.
  "threads": 10
}
```

When an S3 request passes through to iRODS, it becomes a connection to the
irods server which is pretending to be the user that it is mapped to.
This means that you are limited to what you can do there, as an irods user,
rather than whatever expectation might exist with a proper S3 environment.

## Connecting with Botocore

As a simple example, this is how you pass that in through botocore, one of the various libraries from Amazon which happen
to provide S3 connectivity.

```python
import botocore.session

session = botocore.session.get_session()
client = session.create_client("s3",
                               use_ssl=False,
                               endpoint_url="http://127.0.0.1:8080",
                               aws_access_key_id="<username>",
                               aws_secret_access_key="<secret key>")
```
