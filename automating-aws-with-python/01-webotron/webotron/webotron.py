#!usr/bin/python
# -*- coding: utf-8 -*-

"""
Webotron: Deploy websites to AWS.

Webotron automates the process of deploying static websites to AWS
- Configure and create aws buckets
- Set them up for static website hosting
- Configure DNS with Route 53
- Configure a CDN with Cloudfront
"""

import boto3
import click
from bucket import BucketManager

SESSION = None
BUCKET_MANAGER = None


@click.group()
@click.option('--profile', default=None,
              help="Use a given AWS profile")
@click.option('--region', default='us-east-2',
              help="Set a region (defaults to us-east-2)")
def cli(profile, region):
    """Webotron deploys websites to AWS."""
    global SESSION, BUCKET_MANAGER

    session_cfg = {}
    if profile:
        session_cfg['profile_name'] = profile
        session_cfg['region_name'] = region
    SESSION = boto3.Session(**session_cfg)
    BUCKET_MANAGER = BucketManager(SESSION)


@cli.command('list-buckets')
def list_buckets():
    """List all s3 Buckets."""
    for bucket in BUCKET_MANAGER.all_buckets():  # pylint: disable=maybe-no-member
        print(bucket)


@cli.command('list-bucket-objects')
@click.argument('bucket')
def list_bucket_objects(bucket):
    """List objects in an s3 bucket."""
    for obj in BUCKET_MANAGER.all_objects(bucket):  # pylint: disable=maybe-no-member
        print(obj.key)
    if sum(1 for object in BUCKET_MANAGER.all_objects(bucket)) == 0:
        print("No objects found in %s" % bucket)


@cli.command('setup-bucket')
@click.argument('bucket')
def setup_bucket(bucket):
    """Create and configure an s3 bucket."""
    s3_bucket = BUCKET_MANAGER.init_bucket(bucket)
    BUCKET_MANAGER.set_policy(s3_bucket)
    BUCKET_MANAGER.configure_website(s3_bucket)


@cli.command('sync')
@click.argument('pathname', type=click.Path(exists=True))
@click.argument('bucket')
def sync(pathname, bucket):
    """Sync contents of PATHNAME to BUCKET."""
    BUCKET_MANAGER.sync(pathname, bucket)
    print(BUCKET_MANAGER.get_bucket_url(BUCKET_MANAGER.s3.Bucket(bucket)))


if __name__ == '__main__':
    cli()
