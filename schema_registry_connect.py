#!/usr/bin/env python3

import os
import logging
#import certifi
from pprint import pprint

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

AVRO_SCHEMA_REGISTRY_BASE_URL:str=None
AVRO_SCHEMA_REGISTRY_CLIENT:SchemaRegistryClient = None

UTF_8_DESERIALIZER:StringDeserializer = StringDeserializer('utf_8')
INT_DESERIALIZER:IntegerDeserializer = IntegerDeserializer()

INPUT_TOPIC:str = None
VALUE_SCHEMA:str = None
KEY_SERIALIZATION_CTX:SerializationContext = None
VALUE_SERIALIZATION_CTX:SerializationContext = None
AVRO_VALUE_DESERIALIZER:AvroDeserializer = None

def get_secret(secret_name:str) -> dict:
    import json
    import boto3
    import base64
    from botocore.exceptions import ClientError

    try:
        secrets_mgr_cli = boto3.client('secretsmanager')
        get_secret_value_response = secrets_mgr_cli.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logger.error('ERROR %s', e)
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(s=secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return json.loads(s=decoded_binary_secret)

def initialize(input_topic:str, isAuthenticated:bool=True) -> bool:
    global AVRO_SCHEMA_REGISTRY_CLIENT

    SCHEMA_REGISTRY_CONF:dict=dict()
    if (isAuthenticated):
        AVRO_SCHEMA_REGISTRY_BASE_URL = 'https://us-east-1-adsales-sb-main.data-integration.dtcisb.technology:8080'
        secret_as_dict = get_secret(secret_name='laap-sec-ue1-jarviskafkaconn-dev')
        jarvis_usr:str = secret_as_dict['schemaRegistryUsername']
        jarvis_pwd:str = secret_as_dict['schemaRegistryPassword']
        assert (jarvis_usr != None and jarvis_pwd != None)
        
        JARVIS_AUTH_INFO:str=f'{jarvis_usr}:{jarvis_pwd}'
        SCHEMA_REGISTRY_CONF['basic.auth.user.info'] = JARVIS_AUTH_INFO
    else:
        AVRO_SCHEMA_REGISTRY_BASE_URL = 'http://dev-cdp-schema-registry-pvt.us-east-1.espndev.pvt:8081'

    SCHEMA_REGISTRY_CONF['url'] = AVRO_SCHEMA_REGISTRY_BASE_URL

    #print('SCHEMA_REGISTRY_CONF=')
    #pprint(SCHEMA_REGISTRY_CONF)

    global AVRO_SCHEMA_REGISTRY_CLIENT
    AVRO_SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient(conf=SCHEMA_REGISTRY_CONF)

    if input_topic:
        global INPUT_TOPIC
        INPUT_TOPIC = input_topic
        
        global KEY_SERIALIZATION_CTX
        KEY_SERIALIZATION_CTX = SerializationContext(topic=input_topic, field=MessageField.KEY)
        
        global VALUE_SERIALIZATION_CTX
        VALUE_SERIALIZATION_CTX = SerializationContext(topic=input_topic, field=MessageField.VALUE)
        
        value_schema_name:str = f'{input_topic}-value'

        schema_registry_url:str = f'{AVRO_SCHEMA_REGISTRY_BASE_URL}/subjects/{value_schema_name}/versions/latest'
        logger.info('SCHEMA REGISTRY=%s', schema_registry_url)
    
        # Method 1
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
            logger.info('Found schema for %s [Version=%s] as Schema_Id: %s', found_schema.subject, found_schema.version, found_schema.schema_id)
            
            global VALUE
            VALUE_SCHEMA = found_schema.schema.schema_str
            assert (None != VALUE_SCHEMA)
            print(f'AVRO Schema for {value_schema_name} from {schema_registry_url}:\n---\n{VALUE_SCHEMA}\n---\n\n')
            
            global AVRO_VALUE_DESERIALIZER
            AVRO_VALUE_DESERIALIZER = AvroDeserializer(schema_str=VALUE_SCHEMA, schema_registry_client=AVRO_SCHEMA_REGISTRY_CLIENT)
        except SchemaRegistryError as schema_reg_err:
            logger.error('ERROR %s', schema_reg_err)
            return False
            
        return True
    else:
        raise RuntimeError('Need to provided valid topic name')

if __name__ == "__main__":
    initialize(input_topic='lndcdcncstcs_calendars')
    initialize(input_topic='lndcdcadsprpsl_flightrange', isAuthenticated=True)
    print('~~~~~~~~~')
    initialize(input_topic='lndcdcadsprpsl_flightrange', isAuthenticated=False)