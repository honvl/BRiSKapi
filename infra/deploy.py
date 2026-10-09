#!/usr/bin/env python3
"""Provision/update a standalone S3 + Lambda + DynamoDB archive in Tokyo."""
import argparse
import io
import json
from pathlib import Path
import secrets
import time
import zipfile

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
REGION = 'ap-northeast-1'
NAME = 'brisk-recorder-archive'

def ignore_exists(call, **kwargs):
    try:
        return call(**kwargs)
    except ClientError as e:
        if e.response['Error']['Code'] not in {'EntityAlreadyExists', 'ResourceInUseException', 'ResourceConflictException', 'BucketAlreadyOwnedByYou', 'ResourceAlreadyExistsException'}:
            raise

def package():
    """The Lambda zip: the service, the shared schema and reference fingerprints only."""
    code=io.BytesIO()
    with zipfile.ZipFile(code,'w',zipfile.ZIP_DEFLATED) as z:
        z.write(ROOT/'archive_service.py','archive_service.py')
        # An empty package init keeps the client API (and its dependencies) out.
        z.writestr('briskapi/__init__.py','')
        z.write(ROOT/'briskapi/schema.py','briskapi/schema.py')
        # Reference fingerprints verify demo/synthetic content; SBI is structurally checked.
        for path in sorted((ROOT/'briskapi/references').glob('*.json')):
            z.write(path,f'briskapi/references/{path.name}')
    return code.getvalue()

def deploy(bucket):
    session = boto3.Session(region_name=REGION)
    s3, iam, lam, db = [session.client(n) for n in ('s3', 'iam', 'lambda', 'dynamodb')]
    account = session.client('sts').get_caller_identity()['Account']
    ignore_exists(s3.create_bucket, Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': REGION})
    location = s3.get_bucket_location(Bucket=bucket)['LocationConstraint']
    if location != REGION:
        raise ValueError('Archive bucket must be in Tokyo')
    s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={'Status':'Enabled'})
    s3.put_bucket_ownership_controls(Bucket=bucket, OwnershipControls={'Rules':[{'ObjectOwnership':'BucketOwnerEnforced'}]})
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={'Rules':[{'ApplyServerSideEncryptionByDefault':{'SSEAlgorithm':'AES256'}}]})
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        'BlockPublicAcls':True, 'IgnorePublicAcls':True, 'BlockPublicPolicy':False, 'RestrictPublicBuckets':False})
    arn = f'arn:aws:s3:::{bucket}'
    policy = {'Version':'2012-10-17', 'Statement':[
        {'Sid':'ReadPublished', 'Effect':'Allow','Principal':'*','Action':'s3:GetObject','Resource':[arn+'/archive/*',arn+'/timing/*']},
        {'Sid':'ListPublished', 'Effect':'Allow','Principal':'*','Action':'s3:ListBucket','Resource':arn,
         'Condition':{'StringLike':{'s3:prefix':['archive/','archive/*','timing/','timing/*']}}},
        {'Sid':'TLSOnly','Effect':'Deny','Principal':'*','Action':'s3:*','Resource':[arn,arn+'/*'],
         'Condition':{'Bool':{'aws:SecureTransport':'false'}}}]}
    s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={'Rules':[
        {'ID':'ExpireStaging','Status':'Enabled','Filter':{'Prefix':'incoming/'},'Expiration':{'Days':2},
         'NoncurrentVersionExpiration':{'NoncurrentDays':1},'AbortIncompleteMultipartUpload':{'DaysAfterInitiation':1}}]})
    ignore_exists(db.create_table, TableName=NAME, AttributeDefinitions=[{'AttributeName':'id','AttributeType':'S'}],
        KeySchema=[{'AttributeName':'id','KeyType':'HASH'}], BillingMode='PAY_PER_REQUEST')
    db.get_waiter('table_exists').wait(TableName=NAME)
    if db.describe_time_to_live(TableName=NAME)['TimeToLiveDescription']['TimeToLiveStatus'] == 'DISABLED':
        db.update_time_to_live(TableName=NAME, TimeToLiveSpecification={'Enabled':True,'AttributeName':'expires'})
    trust={'Version':'2012-10-17','Statement':[{'Effect':'Allow','Principal':{'Service':'lambda.amazonaws.com'},'Action':'sts:AssumeRole'}]}
    role = ignore_exists(iam.create_role, RoleName=NAME, AssumeRolePolicyDocument=json.dumps(trust))
    role_arn = iam.get_role(RoleName=NAME)['Role']['Arn']
    iam.put_role_policy(RoleName=NAME, PolicyName=NAME, PolicyDocument=json.dumps({'Version':'2012-10-17','Statement':[
        {'Effect':'Allow','Action':['s3:GetObject','s3:GetObjectVersion','s3:PutObject'],'Resource':[arn+'/incoming/*',arn+'/archive/*',arn+'/timing/*']},
        # Staging uploads are removed once validated, rejected or superseded.
        {'Effect':'Allow','Action':['s3:DeleteObject','s3:DeleteObjectVersion'],'Resource':arn+'/incoming/*'},
        {'Effect':'Allow','Action':'dynamodb:UpdateItem','Resource':f'arn:aws:dynamodb:{REGION}:{account}:table/{NAME}'},
        {'Effect':'Allow','Action':['logs:CreateLogStream','logs:PutLogEvents'],'Resource':f'arn:aws:logs:{REGION}:{account}:log-group:/aws/lambda/{NAME}:*'}]}))
    logs=session.client('logs')
    ignore_exists(logs.create_log_group, logGroupName=f'/aws/lambda/{NAME}')
    logs.put_retention_policy(logGroupName=f'/aws/lambda/{NAME}', retentionInDays=14)
    code=io.BytesIO(package())
    try:
        existing=lam.get_function_configuration(FunctionName=NAME)['Environment']['Variables']
    except (lam.exceptions.ResourceNotFoundException, KeyError):
        existing={}
    # Keyed IP hashing for rate limits; keep the key across redeploys.
    salt=existing.get('QUOTA_SALT') or secrets.token_hex(32)
    # Full-replay validation and recompression need about one vCPU.
    config=dict(Runtime='python3.12',Role=role_arn,Handler='archive_service.handler',Timeout=600,MemorySize=1769,
                Environment={'Variables':{'ARCHIVE_BUCKET':bucket,'QUOTA_TABLE':NAME,'QUOTA_SALT':salt}},
                EphemeralStorage={'Size':512})
    try:
        lam.get_function(FunctionName=NAME)
    except lam.exceptions.ResourceNotFoundException:
        for attempt in range(12):
            try:
                lam.create_function(FunctionName=NAME,Code={'ZipFile':code.getvalue()},Architectures=['arm64'],**config)
                break
            except lam.exceptions.InvalidParameterValueException:
                if attempt == 11:
                    raise
                time.sleep(5)
        lam.get_waiter('function_active_v2').wait(FunctionName=NAME)
    else:
        lam.update_function_configuration(FunctionName=NAME,**config)
        lam.get_waiter('function_updated_v2').wait(FunctionName=NAME)
        lam.update_function_code(FunctionName=NAME,ZipFile=code.getvalue(),Architectures=['arm64'])
        lam.get_waiter('function_updated_v2').wait(FunctionName=NAME)
    try:
        lam.put_function_concurrency(FunctionName=NAME,ReservedConcurrentExecutions=4)
    except lam.exceptions.InvalidParameterValueException as e:
        if 'UnreservedConcurrentExecution' not in str(e):
            raise
        print('Regional account quota prevents reserving concurrency; application upload quotas remain active.')
    lam.put_function_event_invoke_config(FunctionName=NAME,MaximumRetryAttempts=2,MaximumEventAgeInSeconds=3600)
    function_arn=lam.get_function(FunctionName=NAME)['Configuration']['FunctionArn']
    ignore_exists(lam.add_permission,FunctionName=NAME,StatementId='S3Ingest',Action='lambda:InvokeFunction',
        Principal='s3.amazonaws.com',SourceArn=arn,SourceAccount=account)
    # Current function URL authorization requires both permissions for NONE auth.
    ignore_exists(lam.add_permission,FunctionName=NAME,StatementId='PublicURL',Action='lambda:InvokeFunctionUrl',
        Principal='*',FunctionUrlAuthType='NONE')
    ignore_exists(lam.add_permission,FunctionName=NAME,StatementId='PublicURLInvoke',Action='lambda:InvokeFunction',
        Principal='*',InvokedViaFunctionUrl=True)
    try:
        url=lam.get_function_url_config(FunctionName=NAME)['FunctionUrl']
    except lam.exceptions.ResourceNotFoundException:
        url=lam.create_function_url_config(FunctionName=NAME,AuthType='NONE')['FunctionUrl']
    s3.put_bucket_notification_configuration(Bucket=bucket,NotificationConfiguration={'LambdaFunctionConfigurations':[
        {'Id':'AutomaticIngest','LambdaFunctionArn':function_arn,'Events':['s3:ObjectCreated:*'],
         'Filter':{'Key':{'FilterRules':[{'Name':'prefix','Value':'incoming/'},{'Name':'suffix','Value':'/events.jsonl.gz'}]}}}]})
    result={'bucket':bucket,'region':REGION,'api_url':url}
    (ROOT/'briskapi/archive.json').write_text(json.dumps(result,indent=2)+'\n')
    return result

if __name__ == '__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--bucket',required=True,help='Use a new dedicated globally unique bucket name')
    args=p.parse_args()
    print(json.dumps(deploy(args.bucket),indent=2))
