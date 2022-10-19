import json
import os
import boto3
from datetime import datetime
from datetime import timedelta

client = boto3.client('rds')

def lambda_handler(event, context):
    # TODO implement
    
    db_name_1 = 'tatasky-production-aurora-cluster'
    db_name_2 = 'tatasky-production-native-selfcare-aurora-cluster'
    
    snapshot_dict_1 = client.describe_db_cluster_snapshots(DBClusterIdentifier=db_name_1,SnapshotType='automated')
    db_snapshots_1 = {}
    for snapshot in snapshot_dict_1['DBClusterSnapshots']:
        db_snapshots_1.update([("DBClusterSnapshotArn", snapshot["DBClusterSnapshotArn"]), ("SnapshotCreateTime", snapshot["SnapshotCreateTime"]), ("DBClusterSnapshotIdentifier", snapshot["DBClusterSnapshotIdentifier"])])
        
    arn_1 = min(db_snapshots_1.items())[0]
    print(arn_1)
    snapshot_name_1 = db_snapshots_1.get('DBClusterSnapshotIdentifier')
    print(snapshot_name_1) 

    
    snapshot_dict_2 = client.describe_db_cluster_snapshots(DBClusterIdentifier=db_name_2,SnapshotType='automated')
    db_snapshots_2 = {}
    for snapshot in snapshot_dict_2['DBClusterSnapshots']:
        db_snapshots_2.update([("DBClusterSnapshotArn", snapshot["DBClusterSnapshotArn"]), ("SnapshotCreateTime", snapshot["SnapshotCreateTime"]), ("DBClusterSnapshotIdentifier", snapshot["DBClusterSnapshotIdentifier"])])
    
    arn_2 = min(db_snapshots_2.items())[0]
    print(arn_2)
    snapshot_name_2 = db_snapshots_2.get('DBClusterSnapshotIdentifier')
    print(snapshot_name_2) 
    
    #yesterday = datetime.today().date() - timedelta(days = 1)
    #today_date = yesterday.strftime("%Y-%m-%d")
    

    export_task_1 = "test-1-" + snapshot_name_1
    s3_name_1 = 'production-tp-rds-snapshots'
    s3_Prefix = 'tatasky-production-aurora-cluster'

    iam_role_name = 'arn:aws:iam::281176377529:role/production-export-rds-snapshots-to-s3'
    kms_key_id = '49631129-1f33-48bc-acbd-d0aa548ae2e7'

    response_1 = client.start_export_task(
        ExportTaskIdentifier=export_task_1,
        SourceArn=arn_1,
        S3BucketName=s3_name_1,
        IamRoleArn=iam_role_name,
        KmsKeyId=kms_key_id,
        S3Prefix=s3_Prefix,
    )
    
    export_task_2 = "test-1-" + snapshot_name_2
    s3_name_2 = 'production-tp-rds-snapshots'
    s3_Prefix_2 = 'tatasky-production-native-selfcare-aurora-cluster'
    
    response_2 = client.start_export_task(
        ExportTaskIdentifier=export_task_2,
        SourceArn=arn_2,
        S3BucketName=s3_name_2,
        IamRoleArn=iam_role_name,
        KmsKeyId=kms_key_id,
        S3Prefix=s3_Prefix_2,
    )



    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
    
    
    
