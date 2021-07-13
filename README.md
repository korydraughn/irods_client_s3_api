# irods_client_s3_cpp
C++ S3 API for iRODS

We will implement a subset of the S3 API:
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations.html

Initial effort to include:
  - CopyObject
  - CompleteMultipartUpload
  - CreateMultipartUpload
  - DeleteObject
  - DeleteObjects
  - GetObject
  - GetObjectAcl ?
  - GetObjectTagging
  - HeadObject
  - ListObjects ?
  - ListObjectsV2
  - PutObject
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
