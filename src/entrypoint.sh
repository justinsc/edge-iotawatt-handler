#!/bin/sh

cd ${APP_HOME}
. venv/bin/activate
python src/iotawatt_publisher.py $@
