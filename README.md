# Lambda to read from Kafka and write to S3 in parquet foramt

## How to make AWS Lambda layers
* Create requirements.txt file with python library dependency requirements
* Example confluent_kafka layer requirements.txt
```
  avro-python3==1.10.1
  fastavro==1.3.4
  confluent-kafka==1.5.0
```
* docker run -v "$PWD":/var/task "lambci/lambda:build-python3.7" /bin/sh -c "pip install -r requirements.txt -t python/lib/python3.7/site-packages/; exit"
* zip -r newlayer.zip python > /dev/null

## Refer AWS Data Wrangler: https://aws-data-wrangler.readthedocs.io/
Lambda layer: https://aws-data-wrangler.readthedocs.io/en/stable/install.html#aws-lambda-layer


## To register event for the Lambda
```
aws lambda create-event-source-mapping --topics lndcdcadsprpsl_flightrange --batch-size 10000 --source-access-configuration '[{"Type": "VPC_SUBNET", "URI": "subnet:subnet-c612ece8"}, {"Type": "VPC_SUBNET", "URI": "subnet:subnet-4220a008"},{"Type": "VPC_SUBNET", "URI": "subnet:subnet-f38a4a94"}, {"Type": "VPC_SECURITY_GROUP", "URI": "security_group:sg-09769e7b6903b5ecd"}]' --function-name kafka-to-s3-awsdatawrangler --self-managed-event-source '{"Endpoints":{"KAFKA_BOOTSTRAP_SERVERS":["10.122.90.115:9093", "10.122.80.105:9093", "10.122.85.200:9093"]}}'
```

## Lambda ENV parameters
* INPUT_TOPIC = lndcdcadsprpsl_flightrange
* BASE_FOLDER = lambda_python_10k_aws_wrangler

## Sample event to test the Lambda
```
{
  "bootstrapServers": "10.122.90.115:9093,10.122.80.105:9093,10.122.85.200:9093",
  "eventSource": "SelfManagedKafka",
  "records": {
    "lndcdcadsrtcrd_ratecard-1": [
      {
        "topic": "lndcdcadsrtcrd_ratecard",
        "partition": 1,
        "offset": 0,
        "timestamp": 1625877624954,
        "timestampType": "CREATE_TIME",
        "key": "NA==",
        "value": "AAAAAYcCCAISQURTX1JUQ1JEAjQyMDIwLTEyLTA1IDA3OjAzOjMwLjI1OTc3OAICAgICBAIaRVVSTyBSYXRlY2FyZAIaRVVSTyBSYXRlY2FyZAJYQWRkaXRpb24gb2YgTXVsdGktUmF0ZWNhcmQgZm9yIEludGVybmF0aW9uYWwCIAIMAgAAAgICNAxJTlNFUlQmMjAyMS0wNy0wMiAwNDowMDowMCYyMDIxLTA3LTAyIDA0OjAwOjAwEkFEU19SVENSRA==",
        "headers": []
      }
    ]
  }
}
```

## References:
- https://crashlaker.github.io/aws/data-engineer/2020/04/19/lambda_parquet.html
- https://github.com/awslabs/aws-data-wrangler
- https://aws-data-wrangler.readthedocs.io/