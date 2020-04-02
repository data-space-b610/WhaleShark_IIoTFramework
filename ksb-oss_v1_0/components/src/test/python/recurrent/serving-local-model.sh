#!/bin/bash

tensorflow_model_server \
  --model_name=seoul_traffic \
  --model_base_path=/tmp/seoul_traffic/model

