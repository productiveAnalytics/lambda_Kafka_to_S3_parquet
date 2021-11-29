#!/usr/bin/env bash

BATCH_SIZE=500
LAMBDA_NAME='lineardp-kafka-to-s3-awsdatawranger'

#KAFKA1_BROKER='10.122.85.200:9093'
#KAFKA2_BROKER='10.122.80.105:9093'
#KAFKA3_BROKER='10.122.90.115:9093'
KAFKA1_BROKER='10.122.94.203:9093'
KAFKA2_BROKER='10.122.81.111:9093'
KAFKA3_BROKER='10.122.93.195:9093'

topics=(lndcdcadsprpsl_flightyear
lndcdcadsprpsl_flightquarter
lndcdcadsprpsl_flightmonth
lndcdcadsprpsl_flightweek
lndcdcadsprpsl_flightday
lndcdcadsrtcrd_flightquarter
lndcdcadsprpsl_flightrange
lndcdcadsrtcrd_flightrange
lndcdcadsrtcrd_flight
lndcdcadsprpsl_flight
lndcdcncstcs_flightdates)

for my_topic in "${topics[@]}";
do
    aws lambda create-event-source-mapping --topics ${my_topic} --batch-size ${BATCH_SIZE} --source-access-configuration '[{"Type": "VPC_SUBNET", "URI": "subnet:subnet-c612ece8"}, {"Type": "VPC_SUBNET", "URI": "subnet:subnet-4220a008"},{"Type": "VPC_SUBNET", "URI": "subnet:subnet-f38a4a94"}, {"Type": "VPC_SECURITY_GROUP", "URI": "security_group:sg-09769e7b6903b5ecd"}]' --function-name ${LAMBDA_NAME} --self-managed-event-source "{\"Endpoints\":{\"KAFKA_BOOTSTRAP_SERVERS\":[\"${KAFKA3_BROKER}\", \"${KAFKA2_BROKER}\", \"${KAFKA1_BROKER}\"]}}";
    sleep 1;
    echo "Lambda ${LAMBDA_NAME} configured for topic: ${my_topic}"
    echo;
done

echo "Waiting for Lambda triggers for 5 minutes..."
sleep 300
aws lambda list-event-source-mappings --function-name ${LAMBDA_NAME}
echo
echo "Done!"