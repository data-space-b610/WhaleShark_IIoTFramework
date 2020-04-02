#!/bin/bash

if [ $# -eq 0 ]; then
  echo "No argument."
  echo "Usage: ./export-to-hdfs.sh web_hdfs_addr hdfs_user"
  echo "  web_hdfs_addr    eg. localhost:50070"
  echo "  hdfs_user        eg. csle"
  exit 255
fi

WEB_HDFS_URL=$1
HDFS_USER=$2

CSLE_GIT=$PWD/../../../../..

EXPORT_BASE="http://$WEB_HDFS_URL/user/guest/seoul_traffic"
TEMP_DIR="/tmp/user/guest/seoul_traffic"
MODEL_VERSION="001"
SIGNATURE_NAME="predict_speed"

python ../../../main/python/recurrent/rnn_saved_model.py \
  --isTrain=True \
  --root_path=$CSLE_GIT/examples/datasets/rnn \
  --input_data_path=trainset.csv \
  --checkpoint_dir=$TEMP_DIR/checkpoint \
  --model_path=rnn2_model.ckpt \
  --train_accuracy_dir=$TEMP_DIR/accuracy \
  --train_accuracy_path=rnn2_acc.csv \
  --log_dir=$TEMP_DIR/logs \
  --num_epoch=1 \
  --num_train=1000 \
  --num_validation=200 \
  --num_test=300 \
  --export_url=$EXPORT_BASE/model \
  --hdfs_user=$HDFS_USER \
  --model_version=$MODEL_VERSION \
  --signature_name=$SIGNATURE_NAME

