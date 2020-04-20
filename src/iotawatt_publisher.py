#!/usr/bin/env python
#
#  Copyright 2018, CRS4 - Center for Advanced Studies, Research and Development
# in Sardinia
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import sys
import json
import flask
import signal
import socket
import logging
import influxdb
import argparse
import datetime
import configparser
import paho.mqtt.publish as publish
from werkzeug.utils import cached_property


MQTT_LOCAL_HOST = "localhost"     # MQTT Broker address
MQTT_LOCAL_PORT = 1883            # MQTT Broker port
INFLUXDB_HOST = "localhost"     # INFLUXDB address
INFLUXDB_PORT = 8086            # INFLUXDB port
GPS_LOCATION = "0.0,0.0"        # DEFAULT location


APPLICATION_NAME = 'IOTAWATT_publisher'

app = flask.Flask(__name__)


class INFLUXDBRequest(flask.Request):
    # accept up to 1kB of transmitted data.
    max_content_length = 1024

    @cached_property
    def get_payload(self):
        _form_content_type = 'application/x-www-form-urlencoded'
        if self.headers.get('content-type') == _form_content_type:
            l_points = []
            v_payload = self.get_data()
            v_points = v_payload.splitlines()
            for _point in v_points:
                l_points.append(
                    dict(
                        zip(
                            ['tag_set', 'field_set', 'timestamp'],
                            _point.decode().split())))

            return l_points


PARAMETERS_MAP = {
    'A': 'current',
    'AC': 'apparentPower',
    'PF': 'powerFactor',
    'W': 'realPower',
    'Wh': 'consumedEnergy',
    'F': 'frequency',
    'V': 'voltage'
}

MESSAGE_PARAMETERS = PARAMETERS_MAP.keys()


@app.route("/query", methods=['POST'])
def query_data():
    v_logger = app.config['LOGGER']
    v_influxdb_host = app.config['INFLUXDB_HOST']
    v_influxdb_port = app.config['INFLUXDB_PORT']

    _payload = flask.request.form
    _db = _payload.get('db')
    _data = '&'.join(['='.join(_i) for _i in _payload.items()])

    _client = influxdb.InfluxDBClient(
        host=v_influxdb_host,
        port=v_influxdb_port,
        username='root',
        password='root',
        database=_db
    )

    _dbs = _client.get_list_database()
    if _db not in [_d['name'] for _d in _dbs]:
        v_logger.info(
            "InfluxDB database '{:s}' not found. Creating a new one.".
            format(_db))
        _client.create_database(_db)

    try:
        _result = _client.request(
            'query',
            'POST',
            params=_data,
            expected_response_code=200)
    except Exception as ex:
        v_logger.error(ex)
    finally:
        _client.close()

    _response = flask.make_response(_result.text, _result.status_code)

    return _response


@app.route("/write", methods=['POST'])
def publish_data():
    v_logger = app.config['LOGGER']

    v_mqtt_local_host = app.config['MQTT_LOCAL_HOST']
    v_mqtt_local_port = app.config['MQTT_LOCAL_PORT']
    v_topic = app.config['MQTT_TOPIC']
    v_influxdb_host = app.config['INFLUXDB_HOST']
    v_influxdb_port = app.config['INFLUXDB_PORT']

    _data = flask.request.get_data()
    _args = flask.request.args.to_dict()
    _db = _args.get('db')

    _client = influxdb.InfluxDBClient(
        host=v_influxdb_host,
        port=v_influxdb_port,
        username='root',
        password='root',
        database=_db
    )

    _dbs = _client.get_list_database()
    if _db not in [_d['name'] for _d in _dbs]:
        v_logger.info(
            "InfluxDB database '{:s}' not found. Creating a new one.".
            format(_db))
        _client.create_database(_db)

    try:
        _result = _client.request(
            'write',
            'POST',
            params=_args,
            data=_data,
            expected_response_code=204)
        v_logger.debug("Insert data into InfluxDB: {:s}".format(str(_data)))
    except Exception as ex:
        v_logger.error(ex)
    finally:
        _client.close()

    _response = flask.make_response(_result.text, _result.status_code)

    v_messages = []

    try:
        v_payload = flask.request.get_payload

        # Creates a dictionary with the sensor data
        for v_measure in v_payload:
            _sensor_tree = dict()

            _tag = v_measure['tag_set']
            _, _value = v_measure['field_set'].split('=')
            _timestamp = v_measure['timestamp']
            _dateObserved = datetime.datetime.fromtimestamp(
                int(_timestamp), tz=datetime.timezone.utc).isoformat()

            _label, _, _parameter = _tag.partition('_')
            _station_id = 'IOTAWATT'

            if _parameter in MESSAGE_PARAMETERS:
                if _label not in _sensor_tree:
                    _sensor_tree[_label] = {}

                _sensor_tree[_label].update({
                    PARAMETERS_MAP[_parameter]: _value,
                    'timestamp': _timestamp,
                    'dateObserved': _dateObserved,
                })

            # Insofar, one message is sent for each sensor
            for _label, _data in _sensor_tree.items():
                _message = dict()
                _message["payload"] = json.dumps(_data)
                _message["topic"] = "EnergyMonitor/{}.{}".format(
                    _station_id, _label)
                _message['qos'] = 0
                _message['retain'] = False

                v_messages.append(_message)

        v_logger.debug(
            "Message topic:\'{:s}\', broker:\'{:s}:{:d}\', "
            "message:\'{:s}\'".format(
                v_topic, v_mqtt_local_host, v_mqtt_local_port,
                json.dumps(v_messages)))
        publish.multiple(v_messages, hostname=v_mqtt_local_host,
                         port=v_mqtt_local_port)
    except socket.error:
        pass

    return _response


def signal_handler(sig, frame):
    sys.exit(0)


def configuration_parser(p_args=None):
    pre_parser = argparse.ArgumentParser(add_help=False)

    pre_parser.add_argument(
        '-c', '--config-file', dest='config_file', action='store',
        type=str, metavar='FILE',
        help='specify the config file')

    args, remaining_args = pre_parser.parse_known_args(p_args)

    v_general_config_defaults = {
        'mqtt_local_host'     : MQTT_LOCAL_HOST,
        'mqtt_local_port'     : MQTT_LOCAL_PORT,
        'logging_level' : logging.INFO,
        'influxdb_host' : INFLUXDB_HOST,
        'influxdb_port' : INFLUXDB_PORT,
        'gps_location'  : GPS_LOCATION,
    }

    v_specific_config_defaults = {
    }

    v_config_section_defaults = {
        'GENERAL': v_general_config_defaults,
        APPLICATION_NAME: v_specific_config_defaults
    }

    # Default config values initialization
    v_config_defaults = {}
    v_config_defaults.update(v_general_config_defaults)
    v_config_defaults.update(v_specific_config_defaults)

    if args.config_file:
        _config = configparser.ConfigParser()
        _config.read_dict(v_config_section_defaults)
        _config.read(args.config_file)

        # Filter out GENERAL options not listed in v_general_config_defaults
        _general_defaults = {_key: _config.get('GENERAL', _key) for _key in
                             _config.options('GENERAL') if _key in
                             v_general_config_defaults}

        # Updates the defaults dictionary with general and application specific
        # options
        v_config_defaults.update(_general_defaults)
        v_config_defaults.update(_config.items(APPLICATION_NAME))

    parser = argparse.ArgumentParser(
        parents=[pre_parser],
        description=('Collects data from IotaWatt sensor and '
                     'publish them to a local MQTT broker.'),
        formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.set_defaults(**v_config_defaults)

    parser.add_argument(
        '-l', '--logging-level', dest='logging_level', action='store',
        type=int,
        help='threshold level for log messages (default: {})'.
        format(logging.INFO))
    parser.add_argument(
        '--mqtt-host', dest='mqtt_local_host', action='store',
        type=str,
        help='hostname or address of the local broker (default: {})'.
        format(MQTT_LOCAL_HOST))
    parser.add_argument(
        '--mqtt-port', dest='mqtt_local_port', action='store',
        type=int,
        help='port of the local broker (default: {})'.
        format(MQTT_LOCAL_PORT))
    parser.add_argument(
        '--influxdb-host', dest='influxdb_host', action='store',
        type=str,
        help='hostname or address of the influx database (default: {})'.
        format(INFLUXDB_HOST))
    parser.add_argument(
        '--influxdb-port', dest='influxdb_port', action='store',
        type=int,
        help='port of the influx database (default: {})'.format(INFLUXDB_PORT))
    parser.add_argument(
        '--gps-location', dest='gps_location', action='store',
        type=str,
        help=('GPS coordinates of the sensor as latitude,longitude '
              '(default: {})').format(GPS_LOCATION))

    args = parser.parse_args(remaining_args)
    return args


def main():
    # Initializes the default logger
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO)
    logger = logging.getLogger(APPLICATION_NAME)

    # Checks the Python Interpeter version
    if (sys.version_info < (3, 0)):
        logger.fatal("This software requires Python version >= 3.0: exiting.")
        sys.exit(-1)

    args = configuration_parser()

    logger.setLevel(args.logging_level)

    logger.info("Starting %s", APPLICATION_NAME)
    logger.debug(vars(args))
    logger.debug("Logging at level %s", args.logging_level)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    v_mqtt_topic = 'sensor/' + 'IOTAWATT'
    v_latitude, v_longitude = map(float, args.gps_location.split(','))

    config_dict = {
        'LOGGER'     : logger,
        'MQTT_LOCAL_HOST'  : args.mqtt_local_host,
        'MQTT_LOCAL_PORT'  : args.mqtt_local_port,
        'LOG_LEVEL'  : args.logging_level,
        'MQTT_TOPIC' : v_mqtt_topic,

        'INFLUXDB_HOST' : args.influxdb_host,
        'INFLUXDB_PORT' : args.influxdb_port,
    }

    app.config.from_mapping(config_dict)
    app.request_class = INFLUXDBRequest
    app.run(host='0.0.0.0')


if __name__ == "__main__":
    main()

# vim:ts=4:expandtab
# References:
#   http://blog.vwelch.com/2011/04/combining-configparser-and-argparse.html
