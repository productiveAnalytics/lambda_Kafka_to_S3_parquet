import os
import json
import boto3
import logging
import base64
import requests
from pprint import pprint
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
# import numpy as np

import avro.schema
from avro.io import DatumReader, BinaryDecoder

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.serialization import IntegerDeserializer, StringDeserializer
from confluent_kafka.schema_registry.avro import AvroDeserializer   # for Kafka value
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient, RegisteredSchema
from confluent_kafka.schema_registry.error import SchemaRegistryError


logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

ENV_TOPIC:str=os.getenv('INPUT_TOPIC')
if ENV_TOPIC:
    logger.info(f'Found ENV Variable "INPUT_TOPIC". Value={ENV_TOPIC}')
else:
   raise Exception('Could not fined ENV Variable "INPUT_TOPIC".')

#TODO get this from the lambda function event or context and return these to the avro deserializtion methods
# TEST_SCHEMA_NAME:str='lndcdcadsrtcrd_ratecard'
TEST_SCHEMA_NAME:str='lndcdcadsprpsl_flightrange'
TEST_S3_BUCKET:str='lineardp-conformance-common-flink-dev'
TEST_S3_FOLDER:str='lambda_datasync_10k'
# KMS_KEY_ARN:str='arn:aws:kms:us-east-1:550060283415:key/1c9916d5-8214-4a84-b4ed-ae5570e2ea43' # lineardp-credential-kms-dev
KMS_KEY_ARN:str='arn:aws:kms:us-east-1:550060283415:key/2bb9d33c-8b5b-4f67-bccc-6d9f603d7609' # lineardp-conformed-kms-dev

KEY_SERIALIZATION_CTX:SerializationContext = SerializationContext(topic=TEST_SCHEMA_NAME, field=MessageField.KEY)
VALUE_SERIALIZATION_CTX:SerializationContext = SerializationContext(topic=TEST_SCHEMA_NAME, field=MessageField.VALUE)

AVRO_SCHEMA_REGISTRY_BASE_URL:str='http://dev-cdp-schema-registry-pvt.us-east-1.espndev.pvt'
SCHEMA_REGISTRY_CONF:dict={"url": AVRO_SCHEMA_REGISTRY_BASE_URL}
AVRO_SCHEMA_REGISTRY_CLIENT:SchemaRegistryClient = SchemaRegistryClient(conf=SCHEMA_REGISTRY_CONF)

UTF_8_DESERIALIZER:StringDeserializer = StringDeserializer('utf_8')
INT_DESERIALIZER:IntegerDeserializer = IntegerDeserializer()

def write_parqut(topic:str, dataset:dict, column_names:list) -> str:
    now_utc:datetime = datetime.utcnow()
    epoch = now_utc.timestamp()
    temp_filename = f'/tmp/{topic}_{epoch}.parquet'
    try:
        df = pd.DataFrame(dataset, columns=column_names)
        pa_tbl = pa.Table.from_pandas(df)
        logger.info('Trying to write parquet file: %s', temp_filename)
        pq.write_table(pa_tbl, temp_filename)
        logger.info('Successfully written parquet file: %s', temp_filename)
        return (temp_filename, now_utc)
    except Exception as ex:
        logger.error(ex)
        return (None, None)
    
def upload_to_s3(topic:str, bucket_name:str, base_folder:str, input_filename:str, epoch:datetime, use_kms:bool=False) -> bool:
    path_parts = input_filename.split('/')
    filename:str = path_parts[-1:][0] # get last part
            
    # yyyy/MM/dd/HH
    partition_path:str = f'{epoch.year}/{epoch.month:02}/{epoch.day:02}/{epoch.hour:02}'
    s3_folder_path:str = f'{base_folder}/{topic}/{partition_path}/{filename}'
    logger.info('Using S3 Bucket %s, S3 key: %s', bucket_name, s3_folder_path)
    
    try:
        logger.info('Trying to establish connection to S3')
        # s3_resource = boto3.resource('s3')
        s3_client = boto3.client('s3')
        logger.info('Established connection to S3')
        # s3bucket = s3_resource.Bucket(bucket_name)
        logger.info('Established connection to S3 bucket: %s', bucket_name)
    
        logger.info('Before uploading parquet to S3')
        with open(input_filename, 'rb') as parquet_file:
            # s3bucket.upload_fileobj(Fileobj=parquet_file, Key=s3_folder_path)
            # s3bucket.put_object(Key=s3_folder_path, Body=parquet_file)
            # s3_client.put_object(Bucket=bucket_name, \
            #     Body=parquet_file, \
            #     Key=s3_folder_path)
            # s3_client.upload_fileobj(Fileobj=parquet_file, Bucket=bucket_name, Key=s3_folder_path)
            if (use_kms):
                logger.info('Before s3_client.put_object() with KMS key: %s', KMS_KEY_ARN)
                s3_upload_info:dict = s3_client.put_object(Bucket=bucket_name, Key=s3_folder_path, \
                    Body=parquet_file, \
                    ServerSideEncryption='aws:kms', SSEKMSKeyId=KMS_KEY_ARN)
                logger.info('After  s3_client.put_object() with KMS key: %s', KMS_KEY_ARN)
            else:
                logger.info('Before s3_client.put_object() without aws:kms')
                s3_upload_info:dict = s3_client.put_object(Bucket=bucket_name, Key=s3_folder_path, Body=parquet_file)
                logger.info('After s3_client.put_object() without aws:kms')
                
            if (s3_upload_info):
                logger.info('SUCCESS: Upload response: %s', s3_upload_info)
            else:
                logger.error('ERROR: Failed upload to ')
    
        logger.info('Successfully uploaded parquet to S3: %s', s3_folder_path)
        return True
    except Exception as e:
        logger.error(e)
        return False

def get_AVRO_schema(base_schema_name:str) -> str:
    value_schema_name:str = f'{base_schema_name}-value'
    schema_text:str = None

    # Method 1
    # schema_registry_url = f'{AVRO_SCHEMA_REGISTRY_BASE_URL}/subjects/{value_schema_name}/versions/latest'
    # logger.info('SCHEMA REGISTRY=%s', schema_registry_url)
    # response = requests.get(schema_registry_url)
    # if (response):
    #     # pprint(dir(response))
    #     logger.info('GET response code: %s', response.status_code)
    #     schema_text = response.text
    # else:
    #     logger.error('Unable to reach schema registry: %s', schema_registry_url)

    # Method 2
    # all_schemas = AVRO_SCHEMA_REGISTRY_CLIENT.get_subjects()
    # for schema in all_schemas:
    #     logger.info('SCHEMA: %s', schema)
    try:
        found_schema:RegisteredSchema = AVRO_SCHEMA_REGISTRY_CLIENT.get_latest_version(subject_name=value_schema_name)
        logger.info('Found schema: %s', found_schema)
        schema_text = found_schema.schema.schema_str
    except SchemaRegistryError as schema_reg_err:
        logger.error('ERROR %s', schema_reg_err)

    return schema_text

def decode_key(encoded_key:str):
    decoded_key_bytes:bytes = base64.b64decode(encoded_key)
    # logger.debug('Encoded Key: %s', encoded_key)
    # logger.debug('Decoded Key: %s', decoded_key_bytes)
    
    try:
        logger.debug('1. Trying with String Deserializer')
        return UTF_8_DESERIALIZER(decoded_key_bytes, ctx=KEY_SERIALIZATION_CTX)
    except Exception as e1:
        logger.error('ERROR %s', e1)
        try:
            logger.debug('2. Trying with Integer Deserializer')
            return INT_DESERIALIZER(decoded_key_bytes, ctx=KEY_SERIALIZATION_CTX)
        except Exception as e2:
            logger.error('ERROR %s', e2)
            logger.debug('3. Trying with simple ASCII decode')
            return decoded_key_bytes.decode('ascii')
            
def decode_avro(schema:str, encoded_value:str) -> dict:
    decoded_value_bytes:bytes = base64.b64decode(encoded_value)
    # logger.debug('Encoded Value: %s', encoded_value)
    # logger.debug('Decoded Value: %s', decoded_value_bytes)

    schema_text:str = get_AVRO_schema(base_schema_name=schema)

    try:
        value_deserializer = AvroDeserializer(schema_str=schema_text, schema_registry_client=AVRO_SCHEMA_REGISTRY_CLIENT)
        return_values = value_deserializer(decoded_value_bytes, ctx=VALUE_SERIALIZATION_CTX)
        return return_values
    except Exception as ex:
        logger.error("ERROR while decoding AVRO")
        logger.error(ex)
        decoded_value_bytes.seek(5)
        binary_decoder = BinaryDecoder(decoded_value_bytes)
        avro_schema = avro.schema.Parse(schema_text)
        avro_reader = DatumReader(avro_schema)
        return avro_reader.read(binary_decoder)

def lambda_handler(event, context):
    logger.info('Context: %s', context)

    logger.info('Event: %s', event)
    event_records = event['records']
    logger.info('Total records %d', len(event_records))
    
    avro_records:list = []
    
    fields_names:list = []
    datatypes_list:list = []
    dataset:dict = {}
    
    metadata_avail:bool = False

    for kafka_partition in event_records:
        logger.info('---')
        kafka_records = event['records'][kafka_partition]
        records_cnt:int = len(kafka_records)
        logger.info('Total %d records for partition: %s', records_cnt, kafka_partition)
        for krecord in kafka_records:
            krecord_key:str = krecord['key']
            decrypted_key = decode_key(encoded_key=krecord_key)

            krecord_val:str = krecord['value']
            # decrypted_val_bytes = base64.b64decode(krecord_val)
            decrypted_val = decode_avro(schema=TEST_SCHEMA_NAME, encoded_value=krecord_val)

            # logger.info('Orig Key=%s & Orig Value=%s', krecord_key, krecord_val)
            logger.info('%s===%s', decrypted_key, decrypted_val)
            
            if (not metadata_avail):
                for column_name in decrypted_val:
                    fields_names.append(column_name)
                    dataset[column_name] = []
                metadata_avail = True
                
            if (metadata_avail):
                for column_name in decrypted_val:
                    existing_data_list:list = dataset[column_name]
                    new_entry = decrypted_val[column_name]
                    existing_data_list.append(new_entry)
                    dataset[column_name] = existing_data_list
            
    return_json:json = json.dumps(dataset)
    # np_arr = np.asarray(dataset)
    # pprint(np_arr)
    
    return_tuple = write_parqut(topic=TEST_SCHEMA_NAME, dataset=dataset, column_names=fields_names)
    local_parquet_file_path:str = return_tuple[0]
    creation_epoch:datetime = return_tuple[1]
    if (local_parquet_file_path and creation_epoch):
        success_status:bool = upload_to_s3(topic=TEST_SCHEMA_NAME, \
            bucket_name=TEST_S3_BUCKET, \
            base_folder=TEST_S3_FOLDER, \
            input_filename=local_parquet_file_path, \
            epoch=creation_epoch,
            use_kms=False)

    if (success_status):
        return {
            'statusCode': 200,
            'body': return_json
        }
    else:
        return {
            'statusCode': 500,
            'body': f'Error converting to parquet or upload to S3: {TEST_S3_BUCKET}'
        }
