import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from datetime import *
import time
import botocore
import boto3
import logging

import odl_common
import odl_monitoring
import odl_settings
import odl_statistics

import pandas as pd
import hashlib, base64


####################################################################################################
# SCRIPT name : uc0003_create_md5_hashcode_files_glue.py
# Author      : anders.ac.carlsson@volvo.com
# Purpose     : The main purpose of this script is to create md5 hash values for each file in current prefix (Folder).
#               For each prefix, a file is created in the output prefix (folder) containing all all files in the input prefix with the correspong md5 hash sum
#               Current prefix is prefix which are used by the tables VUL_RAW, TRACKINGEVENT hub, sat1,sat2,sat3 & sat4
#
# Changes     :  2022-04-25/AC, change to create md5 sums for all tables in "trev" when one record is created in the forces-rerun re-processed folder. 
#                               Which means that the software in package odl_usecase_common.py only creates one records per data data source
#                               previous version of this softwareh which was not used in any execution, was taking care of each sub table in trev.
#                          
# Changes     :  2022-04-28/AC, changed the file name created myby trev when it comes to which data that shall be re-processed.
#                                 it does not now contain the partion key value (ttd=)
#
# Change      : 2022-06-09/AC, Changes search comment  ### AC 20220609
#                              Removed the creation of message to LDA/GTT that files has been re-created.
#                              This must be maually sovled until later implemmentation  
#  
### trev/trackingevent/HUB/ttd=20200120/
### trev/trackingevent/SAT1/ttd=20200120/
### trev/trackingevent/SAT4/ttd=20200120/
##### Alla omkörninngar lagras på det nya sättet.
#
### trev/trackingevent/HUB/ttd=20210120/
### trev/trackingevent/SAT1/ttd=20210120/
### trev/trackingevent/SAT4/ttd=20210120/
##  8h
#
####################################################################################################
C_CURRENT_JOB              = "uc0003_create_md5_hashcode_files_glue"
C_AWS_REGION               = "eu-west-1"
C_USE_CASE                 = "uc0003"


# TABLES
C_TABLE_TREV_PREFIX = 'trev_'

# SUB Tables
C_TREV_HUB   = 'hub'
C_TREV_SAT1  = 'sat1'
C_TREV_SAT2  = 'sat2'
C_TREV_SAT3  = 'sat3'
C_TREV_SAT4  = 'sat4'
C_TREV_BUS_DATASET  = 'bus_dataset'

C_RE_PROCESSED_DATASOURCE_VUL = '/vul/'
C_RE_PROCESSED_DATASOURCE_TREV = '/trev/'

# Output Files 
C_COL_1     = 'file_name'
C_COL_2     = 'md5_hash'
C_HEADINGS  = {C_COL_1:[],C_COL_2:[]} 

C_FILES = 'files'

#######################################################
##
##   save_folder_finalized_file.
##    
def save_folder_finalized_file(pDate2Process):
    logger.info(f'ODL {C_USE_CASE} save_folder_finalized_file STEP: pDate2Process [{pDate2Process}] ')
    B_COL_1     = 'job_id'
    B_COL_2     = 'job_name'
    B_HEADINGS  = {B_COL_1:[],B_COL_2:[]} 

    wPD_File = pd.DataFrame(B_HEADINGS)  
    wPD_File.loc[len(wPD_File.index)] = [B_CURRENT_JOB_RUN_ID, JOBPARM_CURRENT_JOB_NAME]  

    wOutpath = JOBPARM_GTT_DOWNLOAD_INFO_FOLDER +pDate2Process + '/'
    wFileName2Write = wOutpath  + JOBPARM_FOLDER_FINALIZED_FILENAME

    s3 = boto3.resource('s3', region_name = C_AWS_REGION)
    csv_str=wPD_File.to_csv( index=False, sep=',', encoding='utf-8')
    s3object = s3.Object(JOBPARM_USECASE_BUCKET ,wFileName2Write)
    s3object.put(Body=csv_str)

#######################################################
## 
## create_md5_hash_file
##        
def create_md5_hash_file(pFiles, pTableName, pProcessDate, pLakeArrivalDate, pOutFileName):
    print(f'ODL {C_USE_CASE} STEP: create_md5_hash_file, pTableName=[{pTableName}], pProcessDate=[{pProcessDate}], pLakeArrivalDate=[{pLakeArrivalDate}] , pOutFileName=[{pOutFileName}]')


    wPD_FileHashFile = pd.DataFrame(C_HEADINGS)  
    
    s3Client = boto3.client('s3', region_name = C_AWS_REGION)
    wCounter = 0    
    for key in pFiles[C_FILES].keys():
        wCounter = wCounter + 1 
        wFullFileName = key

        wData = s3Client.get_object(Bucket=JOBPARM_USECASE_BUCKET, Key=wFullFileName)
        wContent = wData['Body'].read()
        m = hashlib.md5()
        m.update(wContent)
        value = m.digest()
        wMD5_Hash = base64.b64encode(value)

#       Add records to the Panda Dataframe
        wPD_FileHashFile.loc[len(wPD_FileHashFile.index)] = [wFullFileName, wMD5_Hash]  
#        print(f'ODL {C_USE_CASE} MSG: Full File name is [{wFullFileName}]   hash [{wMD5_Hash}] ')

    wOutpath = JOBPARM_GTT_DOWNLOAD_INFO_FOLDER +pProcessDate + '/'+ pLakeArrivalDate + '_' + pTableName.lower() + '/'
    wOutFileName = pOutFileName + '_'  + pTableName.lower() + ".csv"
    wFileName2Write = wOutpath  + wOutFileName 

    s3 = boto3.resource('s3', region_name = C_AWS_REGION)
    csv_str=wPD_FileHashFile.to_csv( index=False, sep=',', encoding='utf-8')
    s3object = s3.Object(JOBPARM_USECASE_BUCKET ,wFileName2Write)
    s3object.put(Body=csv_str)

    print(f'ODL {C_USE_CASE} MSG: create_md5_hash_file, output file written=[{wFileName2Write}]')
    
    return wCounter

#######################################################
## 
## addStatistics
def addStatistics(pTable, pPrefix, pNoFiles, pNofBytes, pDataLakeArrivelData):
## bucket                   x  uc0003
## number_of_bytes          x   
## number_of_files          x 
## prefix                   x  PATH to the input files/sdfsdfdfs /20200101/ 
## table                    x  md5_VUL, md5_TREV_HUB,mdd5_TREV_SAT1 
## tag                      X  md5:date:

    wTable  = 'md5_' +pTable.lower()
    wTag    = 'md5:' +pDataLakeArrivelData.replace('-','') + ':'+ JOBPARM_PROCESS_DATE.replace('-','')
    odl_statistics.odl_statistics_action({"action":"put", "bucket":JOBPARM_USECASE_BUCKET, "table":wTable, "number_of_bytes":pNofBytes, "number_of_files":pNoFiles, "prefix": pPrefix, "tag":wTag, "task_id":B_CURRENT_JOB_RUN_ID})
#######################################################
## 
## process_vul_data
def process_vul_data(pDate2Process,pLakeArrivalDate):

    logger.info(f'ODL {C_USE_CASE} STEP : process_vul_data pDate2Process=[{pDate2Process}], pLakeArrivalDate=[{pLakeArrivalDate}]   ')

    wPath2process = JOBPARM_VUL_DATA_FOLDER + JOBPARM_VUL_TABLE  +'/' +  pLakeArrivalDate+ '/'  

    wFiles2CalculateHash  =  odl_common.get_files_from_prefix_and_below(JOBPARM_USECASE_BUCKET, wPath2process, C_FILES)


    wCounter = create_md5_hash_file(wFiles2CalculateHash['def_get_files_dict'], JOBPARM_VUL_TABLE , pDate2Process, pLakeArrivalDate, pLakeArrivalDate )
    wFilesInFolder = wFiles2CalculateHash['nof_files']
    if wCounter != wFilesInFolder:
        print(f'ODL {C_USE_CASE}   ERROR: process_vul_data. Added files in the CSV files is not the same as the files in the source folder!!!!  Files Hashed [{wCounter}] wFilesInFolder=[{wFilesInFolder}] ')
        sys.exit(255 ) 

    addStatistics(JOBPARM_VUL_TABLE, wPath2process, wCounter, wFiles2CalculateHash['total_filesize'],pLakeArrivalDate )
    return wCounter


def process_trev_data(pSubTable, pDate2Process,pLakeArrivalDate):
    logger.info(f'ODL {C_USE_CASE} STEP: process_trev_data pSubTable=[{pSubTable}],  pDate2Process=[{pDate2Process}],  pLakeArrivalDate=[{pLakeArrivalDate}]  ')

    wProcessDate = pLakeArrivalDate.replace('-','')
    wPath2process = JOBPARM_TREV_DATA_FOLDER + JOBPARM_TREV_TABLE+ '/'+ pSubTable  +'/ttd=' +  wProcessDate+ '/'  

    wFiles2CalculateHash  =  odl_common.get_files_from_prefix_and_below(JOBPARM_USECASE_BUCKET, wPath2process, C_FILES)
    wCounter = create_md5_hash_file(wFiles2CalculateHash['def_get_files_dict'], C_TABLE_TREV_PREFIX + pSubTable , pDate2Process, pLakeArrivalDate, pLakeArrivalDate )
    
    wFilesInFolder = wFiles2CalculateHash['nof_files']
    if wCounter != wFilesInFolder:
        print(f'ODL {C_USE_CASE}   ERROR: process_trev_data. Added files in the CSV files is not the same as the files in the source folder!!!!  Files Hashed [{wCounter}] wFilesInFolder=[{wFilesInFolder}] ')
        sys.exit(255 ) 
 
    wTable = JOBPARM_TREV_TABLE+ '_' + pSubTable
    addStatistics(wTable, wPath2process, wCounter,  wFiles2CalculateHash['total_filesize'],pLakeArrivalDate)
    
    return wCounter

#######################################################
## 
## start_processing_day
def start_processing_day(pDate2Process):
    logger.info(f'ODL {C_USE_CASE} start_processing_day STEP: pDate2Process [{pDate2Process}]  ')
    wNoF_vul=0
    wNoF_trev_hub =0
    wNoF_trev_sat1 =0
    wNoF_trev_sat2 =0
    wNoF_trev_sat3=0
    wNoF_trev_sat4=0
    wNoF_trev_bus_dataset=0    


    wNoF_vul =  process_vul_data(pDate2Process, pDate2Process )
    wNoF_trev_hub  =  process_trev_data(C_TREV_HUB, pDate2Process, pDate2Process)
    wNoF_trev_sat1 =  process_trev_data(C_TREV_SAT1, pDate2Process, pDate2Process)
    wNoF_trev_sat2 =  process_trev_data(C_TREV_SAT2, pDate2Process, pDate2Process)
    wNoF_trev_sat3 =  process_trev_data(C_TREV_SAT3, pDate2Process, pDate2Process)
    wNoF_trev_sat4 =  process_trev_data(C_TREV_SAT4, pDate2Process, pDate2Process)
    wNoF_trev_bus_dataset =  process_trev_data(C_TREV_BUS_DATASET, pDate2Process, pDate2Process)

    wVUL_Nof_reprocessed_prefix = 0
    wHUB_Nof_reprocessed_prefix = 0
    wSAT1_Nof_reprocessed_prefix = 0
    wSAT2_Nof_reprocessed_prefix = 0
    wSAT3_Nof_reprocessed_prefix = 0
    wSAT4_Nof_reprocessed_prefix = 0
    wBUS_DATASET_Nof_reprocessed_prefix = 0    

    wReProcessedFolder =JOBPARM_RE_PROCESSED_FOLDER + pDate2Process + '/'
    wReProcessedFiles  =  odl_common.get_files_from_prefix_and_below(JOBPARM_USECASE_BUCKET, wReProcessedFolder, C_FILES)
    wFiles  =  wReProcessedFiles['def_get_files_dict']
 
    if not wFiles:
        logger.info(f'ODL {C_USE_CASE} start_processing_day MSG: Current reprocessed folder did not contain any files. Reprocessed foldername[{wReProcessedFolder}] ')
    else:
        for key in wFiles[C_FILES].keys():
            wFullFileName = key
            if wFullFileName[-1] == '/':
                print('Ignore Item ', wFullFileName)
            else:
                if wFullFileName.find(C_RE_PROCESSED_DATASOURCE_VUL) > 0:
                    # VUL DATA
                    logger.info(f'ODL {C_USE_CASE} start_processing_day MSG: wFullFileName, re process [{C_RE_PROCESSED_DATASOURCE_VUL}] data  [{wFullFileName}]  ')
                    wPos= wFullFileName.rfind('/')
                    wLakeArrivalate = wFullFileName[wPos +1:-4]

                    wVUL_Nof_reprocessed_prefix = wVUL_Nof_reprocessed_prefix + 1

                elif  wFullFileName.find(C_RE_PROCESSED_DATASOURCE_TREV) > 0:    
                    # TRACKING DATA
                    logger.info(f'ODL {C_USE_CASE} start_processing_day MSG: wFullFileName, re process [{C_RE_PROCESSED_DATASOURCE_TREV}] data  [{wFullFileName}]  ')
                    wPos= wFullFileName.rfind('/')

                    wLakeArrivalate = wFullFileName[wPos +1:-4]
                    wHUB_Nof_reprocessed_prefix = wHUB_Nof_reprocessed_prefix + 1
                    wSAT1_Nof_reprocessed_prefix = wSAT1_Nof_reprocessed_prefix + 1
                    wSAT2_Nof_reprocessed_prefix = wSAT2_Nof_reprocessed_prefix + 1
                    wSAT3_Nof_reprocessed_prefix = wSAT3_Nof_reprocessed_prefix + 1
                    wSAT4_Nof_reprocessed_prefix = wSAT4_Nof_reprocessed_prefix + 1
                    wBUS_DATASET_Nof_reprocessed_prefix = wBUS_DATASET_Nof_reprocessed_prefix + 1                    


                else:
                    print(f'ODL {C_USE_CASE} ERROR: start_processing_day: the Folder [{JOBPARM_RE_PROCESSED_FOLDER}] contains not vaid information. Current error valus is [{wFullFileName}]. The folder content is=[{wFiles}] ')
                    sys.exit(255 ) 


    save_folder_finalized_file(pDate2Process)
    wRT = {'nof_vul':wNoF_vul,
         'nof_trev_hub':wNoF_trev_hub,
         'nof_trev_sat1':wNoF_trev_sat1,
         'nof_trev_sat2':wNoF_trev_sat2,
         'nof_trev_sat3':wNoF_trev_sat3,
         'nof_trev_sat4':wNoF_trev_sat4,
         'nof_trev_bus_dataset':wNoF_trev_bus_dataset,

         'nof_vul_reprocessed_prefix': wVUL_Nof_reprocessed_prefix,       
         'nof_HUB_reprocessed_prefix': wHUB_Nof_reprocessed_prefix,
         'nof_SAT1_reprocessed_prefix': wSAT1_Nof_reprocessed_prefix,
         'nof_SAT2_reprocessed_prefix': wSAT2_Nof_reprocessed_prefix,
         'nof_SAT3_reprocessed_prefix': wSAT3_Nof_reprocessed_prefix,
         'nof_SAT4_reprocessed_prefix': wSAT4_Nof_reprocessed_prefix,
         'nof_BUS_DATASET_reprocessed_prefix': wBUS_DATASET_Nof_reprocessed_prefix}
    return wRT





args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                    'parent_task_run_id',
                                    'process_date',
                                    'processing_time',
                                    'usecase_bucket',
                                    'gtt_download_info_folder',
                                    're_processed_folder',
                                    'folder_finalized_filename',
                                    'vul_data_folder',
                                    'vul_table',
                                    'trev_data_folder',
                                    'trev_table'
                                    ])

sc = SparkContext()
#sqlContext = SQLContext(sc)
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


JobTimeStamp =  datetime.now()
JobTimeStampDB =  datetime.today() 

#JOBPARM_CURRENT_JOB_NAME    = __name__
JOBPARM_CURRENT_JOB_NAME          = C_CURRENT_JOB
JOBPARM_PARENT_TASK_RUN_ID        = args['parent_task_run_id'].replace('null','') 

JOBPARM_PROCESS_DATE              = args['process_date'].replace('null','') 
JOBPARM_PROCESSING_TIME           = args['processing_time'] 
JOBPARM_USECASE_BUCKET            = args['usecase_bucket'] 
JOBPARM_GTT_DOWNLOAD_INFO_FOLDER  = args['gtt_download_info_folder']
JOBPARM_RE_PROCESSED_FOLDER       = args['re_processed_folder']
JOBPARM_FOLDER_FINALIZED_FILENAME = args['folder_finalized_filename']
JOBPARM_VUL_DATA_FOLDER           = args['vul_data_folder'] 
JOBPARM_VUL_TABLE                 = args['vul_table']
JOBPARM_TREV_DATA_FOLDER          = args['trev_data_folder'] 
JOBPARM_TREV_TABLE                = args['trev_table'] 

B_JOB_START_TIMESTAMP    = JobTimeStamp.strftime('%Y-%m-%d %H:%M:%S.%f')
B_JOB_START_TIMESTAMP    = B_JOB_START_TIMESTAMP[0:23]
B_JOB_TIME               = JobTimeStamp.strftime('%Y-%m-%d %H:%M:%S') 
B_JOB_START_TIME         = JobTimeStamp.strftime('%Y-%m-%d %H:%M:%S')
B_CURRENT_DATE           = JobTimeStamp.strftime('%Y-%m-%d')
B_CURRENT_DATE_OBJ       = datetime.strptime(B_CURRENT_DATE, '%Y-%m-%d')

B_DATE_TO_PROCESS =""
B_DATE_TO_PROCESS_OBJ=""
B_LAST_DATE_TO_PROCESS_OBJ=""


glueClient = boto3.client('glue', region_name = C_AWS_REGION)
response = glueClient.get_job_runs(JobName=JOBPARM_CURRENT_JOB_NAME, MaxResults=1)
wCurJobRuns = response['JobRuns']
wFirstJob = wCurJobRuns[0]
B_CURRENT_JOB_RUN_ID =  wFirstJob['Id']

JOBPARM_CURRENT_JOB_NAME     = args['JOB_NAME']
logger =  odl_common.get_logger(JOBPARM_CURRENT_JOB_NAME)
    

JobTimeStamp   =  datetime.now()
JobTimeStampDB =  datetime.today() 

B_CURRENT_DATE       = JobTimeStamp.strftime('%Y-%m-%d')
B_CURRENT_DATE_OBJ   = datetime.strptime(B_CURRENT_DATE, '%Y-%m-%d')

wStat = start_processing_day(JOBPARM_PROCESS_DATE)

wParmEnd =  {'action': 'success', 'task_id': B_CURRENT_JOB_RUN_ID, 'parent_task_id': JOBPARM_PARENT_TASK_RUN_ID, 'task_name': JOBPARM_CURRENT_JOB_NAME }
odl_monitoring.odl_reg_task_action(wParmEnd)

job.commit()

JobTimeStampEnd = datetime.now()
startTime = JobTimeStamp.strftime('%Y-%m-%d %H:%M:%S.%f') 
startTimeTxt = startTime[:-3]

endTime = JobTimeStampEnd.strftime('%Y-%m-%d %H:%M:%S.%f')
endTimeTxt = endTime[:-3]

logger.info(f' {C_USE_CASE} JOB: JOB STATISTICS ARE:"')
logger.info(f' {C_USE_CASE} JOB: JOB STARTED TIME                  = [{startTimeTxt}]') 
logger.info(f' {C_USE_CASE} JOB: JOB ENDED TIME                    = [{endTimeTxt}]') 
logger.info(f' {C_USE_CASE} ###############################################################') 
logger.info(f' {C_USE_CASE} ###############################################################') 
logger.info(f' {C_USE_CASE} JOB: PROCESS  DATE                     = [{JOBPARM_PROCESS_DATE}]') 
logger.info(f' {C_USE_CASE} JOB: VUL  NOF records in process date  = [{ str(wStat["nof_vul"]) }]') 
logger.info(f' {C_USE_CASE} JOB: HUB  NOF records in process date  = [{ str(wStat["nof_trev_hub"]) }]') 
logger.info(f' {C_USE_CASE} JOB: SAT1 NOF records in process date  = [{ str(wStat["nof_trev_sat1"])}]') 
logger.info(f' {C_USE_CASE} JOB: SAT2 NOF records in process date  = [{ str(wStat["nof_trev_sat2"])}]') 
logger.info(f' {C_USE_CASE} JOB: SAT3 NOF records in process date  = [{ str(wStat["nof_trev_sat3"])}]') 
logger.info(f' {C_USE_CASE} JOB: SAT4 NOF records in process date  = [{ str(wStat["nof_trev_sat4"])}]') 
logger.info(f' {C_USE_CASE} JOB: BUS DATASET NOF records in process date  = [{ str(wStat["nof_trev_bus_dataset"])}]') 
logger.info(f' {C_USE_CASE} JOB:                                                   ') 
logger.info(f' {C_USE_CASE} JOB: VUL       reprocessed dates       = [{ str(wStat["nof_vul_reprocessed_prefix"])}]') 
logger.info(f' {C_USE_CASE} JOB: TREV HUB  reprocessed dates       = [{ str(wStat["nof_HUB_reprocessed_prefix"])}]') 
logger.info(f' {C_USE_CASE} JOB: TREV SAT1 reprocessed dates       = [{ str(wStat["nof_SAT1_reprocessed_prefix"])}]') 
logger.info(f' {C_USE_CASE} JOB: TREV SAT2 reprocessed dates       = [{ str(wStat["nof_SAT2_reprocessed_prefix"])}]') 
logger.info(f' {C_USE_CASE} JOB: TREV SAT3 reprocessed dates       = [{ str(wStat["nof_SAT3_reprocessed_prefix"])}]') 
logger.info(f' {C_USE_CASE} JOB: TREV SAT4 reprocessed dates       = [{ str(wStat["nof_SAT4_reprocessed_prefix"])}]') 
logger.info(f' {C_USE_CASE} JOB: TREV BUS DATASET reprocessed dates       = [{ str(wStat["nof_BUS_DATASET_reprocessed_prefix"])}]') 