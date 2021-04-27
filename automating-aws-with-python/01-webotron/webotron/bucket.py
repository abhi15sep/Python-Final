# -*- coding: utf-8 -*-

"""Classes for S3 Buckets."""
import mimetypes
from functools import reduce
from hashlib import md5

from botocore.exceptions import ClientError
from pathlib import Path
import util
import boto3


class BucketManager:
    """manage  an S3 bucket."""

    CHUNK_SIZE = 8388608

    def __init__(self, session):
        """Create a bucket manager."""
        self.session = session
        self.s3 = self.session.resource('s3')
        self.transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_chunksize=self.CHUNK_SIZE,
            multipart_threshold=self.CHUNK_SIZE

        )
        self.manifest = {}

    def get_region_name(self, bucket):
        """Get the bucket's region name."""
        client = self.s3.meta.client
        bucket_location = client.get_bucket_location(Bucket=bucket.name)

        return bucket_location["LocationConstraint"] or 'us-east-1'

    def get_bucket_url(self, bucket):
        """Get the website URL for this bucket."""
        return "http://{}.{}".format(
            bucket.name,
            util.get_endpoint(self.get_region_name(bucket)).host
        )

    def all_buckets(self):
        """Get an iterator for all buckets."""
        return self.s3.buckets.all()

    def all_objects(self, bucket):
        """Get an iterator for all objects in bucket."""
        return self.s3.Bucket(bucket).objects.all()

    def init_bucket(self, bucket_name):
        """Create a new bucket with name provided."""
        try:
            s3_bucket = self.s3.create_bucket(  # pylint: disable=maybe-no-member
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': self.session.region_name
                }
            )
        except ClientError as error:
            if error.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                s3_bucket = self.s3.Bucket(bucket_name)  # pylint: disable=maybe-no-member
                print("%s is already owned by you." % bucket_name)
            else:
                raise error
        return s3_bucket

    @staticmethod
    def set_policy(s3_bucket):
        """Set bucket policy to be readable by everyone."""
        policy = """
            {
              "Version":"2012-10-17",
              "Statement":[{
              "Sid":"PublicReadGetObject",
              "Effect":"Allow",
              "Principal": "*",
                  "Action":["s3:GetObject"],
                  "Resource":["arn:aws:s3:::%s/*"
                  ]
                }
              ]
            }
            """ % s3_bucket.name
        policy = policy.strip()
        pol = s3_bucket.Policy()
        pol.put(Policy=policy)

    @staticmethod
    def configure_website(s3_bucket):
        """Configure bucket as static website."""
        s3_bucket.Website().put(WebsiteConfiguration={
            'ErrorDocument': {
                'Key': 'error.html'
            },
            'IndexDocument': {
                'Suffix': 'index.html'
            }
        })

    def load_manifest(self, bucket):
        """Load manifest for caching purposes."""
        paginator = self.s3.meta.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket.name):
            for obj in page.get('Contents', []):
                self.manifest[obj['Key']] = obj['ETag']

    @staticmethod
    def hash_data(data):
        """Generate md5 hash for data"""
        hash = md5()
        hash.update(data)

        return hash

    def gen_etag(self, path):
        """Generate etag for file."""
        hashes = []

        with open(path, 'rb') as f:
            while True:
                data = f.read(self.CHUNK_SIZE)

                if not data:
                    break

                hashes.append(self.hash_data(data))

        if not hashes:
            return
        elif len(hashes) == 1:
            return '"{}"'.format(hashes[0].hexdigest())
        else:
            hash = self.hash_data(reduce(lambda x, y: x + y, (h.digest() for h in hashes)))
            return '"{}-{}"'.format(hash.hexdigest(), len(hashes))

    def upload_file(self, s3_bucket, path, key):
        """Upload a file or directory to s3 bucket."""
        content_type = mimetypes.guess_type(key)[0] or 'text/plain'

        etag = self.gen_etag(path)

        if self.manifest.get(key, '') == etag:
            print("Skipping %s, ETags match" % key)
            return

        return s3_bucket.upload_file(
            path,
            key,
            ExtraArgs={
                'ContentType': content_type
            },
            Config=self.transfer_config
        )

    def sync(self, pathname, bucket_name):
        """Sync files and directories to s3 bucket."""
        bucket = self.s3.Bucket(bucket_name)
        root = Path(pathname).expanduser().resolve()
        self.load_manifest(bucket)

        def handle_directory(target):
            for path_found in target.iterdir():
                if path_found.is_dir():
                    handle_directory(path_found)
                if path_found.is_file():
                    self.upload_file(bucket,
                                     str(path_found),
                                     str(path_found.relative_to(root)
                                         )
                                     )

        handle_directory(root)
        print("Successfully synced %s to %s" % (pathname, bucket_name))
