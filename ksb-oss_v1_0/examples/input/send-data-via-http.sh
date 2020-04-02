#!/bin/bash

if [ $# -eq 0 ]; then
  echo "No argument."
  echo "Usage: ./send-data-via-http.sh localhost:53001"
  exit 255
fi

URI=$1
DATE_TIME=`date '+%Y-%m-%d %H:%M:%S'`

JSON_DATA="{
  \"dateTime\": \"$DATE_TIME\",
  \"deviceId\": 9002,
  \"temperature\": 80.0,
  \"humidity\": 0.0,
  \"message\": \"hello\"
}"

echo "send data to: $URI"
curl -XPOST $URI -H 'Content-Type: application/json' --data "$JSON_DATA"
echo
