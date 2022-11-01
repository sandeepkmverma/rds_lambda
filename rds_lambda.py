import json
import os
from tracemalloc import Snapshot
import boto3
from datetime import datetime as dt
import datetime
import time
from dateutil.tz import tzlocal
from random import randint, randrange

client = boto3.client('rds')

def lambda_handler(event, context):
    # TODO implement
    
    iam_role_name = 'arn:aws:iam::281176377529:role/production-export-rds-snapshots-to-s3'
    s3_bucket_name = 'production-tp-rds-snapshots'
    kms_key_id = '49631129-1f33-48bc-acbd-d0aa548ae2e7'

    db_list = ['tatasky-production-aurora-cluster', 'tatasky-production-native-selfcare-aurora-cluster']
    export_snapshot_arn = []

    for db_name in db_list:
        db_snapshots_dict = {}
        snapshots_metadata = client.describe_db_cluster_snapshots(DBClusterIdentifier=db_name,SnapshotType='automated')

        for snapshot in snapshots_metadata['DBClusterSnapshots']:
            db_snapshots_dict.update([(snapshot["DBClusterSnapshotArn"], snapshot["SnapshotCreateTime"])])

        snapshot_arn = min(db_snapshots_dict.items())[0]
        export_snapshot_arn.append(snapshot_arn)
    
    c = 0
    
    for arn in export_snapshot_arn:
            random_number = str(randrange(100, 10000))
            exportTaskId = arn[57:108] + random_number
            print(exportTaskId)
            s3_Prefix = db_list[c]
            response = client.start_export_task(
                ExportTaskIdentifier=exportTaskId,
                SourceArn=arn,
                S3BucketName=s3_bucket_name,
                IamRoleArn=iam_role_name,
                KmsKeyId=kms_key_id,
                S3Prefix=s3_Prefix,
                )
            c += 1
            print(response)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
