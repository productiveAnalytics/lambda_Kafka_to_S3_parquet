#!/usr/bin/env python3

import logging
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)

import findspark
findspark.init()

from pathlib import Path
import os
logger.info('current folder=%s', Path.cwd())

import sys
print('Python version='+ sys.version)
print(sys.path)

# Create spark context
from pyspark import SparkContext
sc = SparkContext.getOrCreate()
print('Spark version='+ sc.version)

# Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# imports
from pyspark.sql.functions import col, lit, count
from pyspark.sql.functions import to_timestamp, to_date

PARQUET_SUFFIX:str='.parquet'
USE_KMS:bool=False

def register_kms_key() -> bool:
    try:
        # kms_key_landing = 'arn:aws:kms:us-east-1:550060283415:key/1c9916d5-8214-4a84-b4ed-ae5570e2ea43'
        kms_key_landing = 'arn:aws:kms:us-east-1:550060283415:key/2bb9d33c-8b5b-4f67-bccc-6d9f603d7609'

        spark._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        spark._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption-algorithm', 'SSE-KMS')
        spark._jsc.hadoopConfiguration().set('fs.s3a.impl.disable.cache', 'true')
        spark._jsc.hadoopConfiguration().set('fs.s3a.server-side-encryption.key', kms_key_landing)

        logger.info('KMS key setup for reading S3')
        return True
    except Exception as ex:
        logger.error(ex)
        return False

def check_parquet(use_local:bool=False) -> None:
    parquet_location:str = None

    if (use_local):
        # Iterate over current folder
        curr_folder_path_obj:Path = Path('.')
        logger.info('Canonical current folder: %s', curr_folder_path_obj.as_posix())
        logger.info('Absolute  current folder: %s', curr_folder_path_obj.absolute())

        local_filename:str = None
        for f_obj in curr_folder_path_obj.iterdir():
            logger.info(f'{type(f_obj)} : {f_obj}  {f_obj.suffix}')
            if (f_obj.suffix == PARQUET_SUFFIX and f_obj.stem.endswith('local_test')):
                logger.info('Found local_test parquet file: %s', f_obj)
                local_filename = f_obj.absolute()
                break

        if not local_filename:
            raise FileNotFoundError('Could not find any parquet file locally!')
        
        try:
            local_filepath_obj:Path = Path(f'./{local_filename}')
            local_filepath:str = local_filepath_obj.absolute().name
            # local_filepath:str = './lndcdcadsrtcrd_ratecard_1630606319.275855_local_test.parquet'
            logger.info('Absolute path of local parquet: %s', local_filepath)
            parquet_location = local_filepath
            logger.info('Using local parquet file: %s', parquet_location)
        except Exception as e:
            logger.error(e.message)
            raise e
    else:
        if (USE_KMS and (not register_kms_key())):
            raise Exception('ERROR during registering KMS key')

        s3_folder = 'lambda_datasync/lndcdcadsrtcrd_ratecard'
        partition = '2021/09/02/18/*'
        s3_location = f's3a://lineardp-conformance-common-flink-dev/{s3_folder}/{partition}'
        parquet_location = s3_location
        logger.info(f'Using parquet from S3 location: {parquet_location}')
    
    try:
        df =  spark.read.parquet(parquet_location)
        logger.info('Uploaded schema:')
        df.printSchema()
        logger.info('Total Records: %d', df.count())
        df.show(truncate=False)
    except Exception as ex:
        print(f'Issue reading {parquet_location}: {ex}')

def main():
    # check_parquet(use_local=False) # read from S3
    check_parquet(use_local=True)

if __name__ == "__main__":
    main()