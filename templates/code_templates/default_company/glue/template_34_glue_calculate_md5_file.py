import sys
import json
import botocore
import boto3
import datetime
from dateutil.tz import tzlocal
import hashlib, base64
from awsglue.utils import getResolvedOptions

import pandas as pd

def get_md5_record(bucket, key):
    s3_client = boto3.client('s3')
    f = s3_client.get_object(Bucket=bucket, Key=key)
    content=f["Body"].read()
    m = hashlib.md5()
    m.update(content)
    value = m.digest()
    hash = str(base64.b64encode(value))
    record = {"file":key,"hash":hash}
    return record

def get_all_files(r):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=r["bucket"], Prefix=r["prefix"])
    print(response)
    all = response['Contents']        
    
    result=[]
    
    for item in all:
        key =  item['Key']
        record = get_md5_record(bucket, key)
        result.append(record)

    return result

def start (r):
   
    result = get_all_files(r)
    
    #prepare the output path for the file
    r["prefix"] = r["output_root_prefix"] + "/" + r["prefix"]
    r["filename"]="hash"

    #save it as csv
    save_csv(r,result)
    

def save_json(r, json_obj):
    s3 = boto3.resource('s3')

    s="json"

    
    b=r['bucket']
    p=r['prefix'] + "/" + s
    f=p + "/" + r['filename'] +"." + s
    
    
    s3object = s3.Object(b ,f)
    
    json_string = json.dumps(json_obj)

    s3object.put(
        Body=(json_string)
    )
   
def save_csv(r, result):
    s="csv"

    s3 = boto3.resource('s3')
    b=r['bucket']
    p=r['prefix'] 
    f=p + "/" + r['filename'] +"." + s
    
    df = pd.DataFrame(result)
    csv_str=df.to_csv( index=False, sep=',', encoding='utf-8')
    
    s3object = s3.Object(b ,f)
    s3object.put(Body=csv_str)
    
#main
g_region='[:REPLACE_AWS_REGION]'
args = getResolvedOptions(sys.argv, ['bucket', 'prefix', 'output_root_prefix'])
bucket = args['bucket'] 
prefix = args['prefix']
output_root_prefix=args['output_root_prefix']

r = {"bucket": bucket, "prefix":prefix, "output_root_prefix":output_root_prefix}


start(r)