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

import argparse
import configparser
import datetime
import flask
import influxdb
import json
import logging
import paho.mqtt.publish as publish
import re
import signal
import socket
import sys

from collections import defaultdict
from contextlib import contextmanager
from werkzeug.utils import cached_property


MQTT_LOCAL_HOST = "localhost"     # MQTT Broker address
MQTT_LOCAL_PORT = 1883            # MQTT Broker port
INFLUXDB_HOST = "localhost"     # INFLUXDB address
INFLUXDB_PORT = 8086            # INFLUXDB port
GPS_LOCATION = "0.0,0.0"        # DEFAULT location


APPLICATION_NAME = 'IOTAWATT_publisher'

app = flask.Flask(__name__)


## We expect to receive measurements of fields from the iotawatt
# with names like "{value}_{key}" with the key-value pairs from this
# dictionary.  For instance, "current_A", "powerFactor_PF".
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


def parse_write_payload(data):
    # prefer an iterator to parse payload to more easily support large requests
    return [ parse_line_protocol(match.group(0).decode()) for match in re.finditer(b'[^\n]+', data) ]


def parse_line_protocol(line):
    """
    This function extracts the information we require from a record
    formatted according to the InfluxDB line protocol:
      <measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<timestamp>]

    Notably, we ignore tags.
    """
    parts = line.split()
    if len(parts) < 2 or len(parts) > 3:
        raise ValueError(
                "Unexpected line format with {} parts (expected 2 or 3) -> {}".format(
                    len(parts), line))
    measurement = parts[0].split(',', 1)[0]
    fields = [ f.split('=') for f in parts[1].split(',') ]
    point = dict(
            measurement=measurement,
            fields=fields)
    if len(parts) == 3:
        point['timestamp'] = parts[2]
    else:
        point['timestamp'] = None

    return point


def get_value_from_point(label, name, point):
    # Look through point and take take last value that matches the name
    # we're looking for.
    # If the measurement name names, we take the value from the last field
    # in the point, regardless of the name.
    # Otherwise, we take the value from the last field that matches the name.
    # If neither measurement nor and field match the name we're looking for,
    # then we return None
    full_name = name + '_' + label
    if point['measurement'] == full_name:
        return point['fields'][-1][1]
    #else:
    fields = point['fields']
    for f in reversed(fields):
        if f[0] == full_name:
            return f[1]
    return None


@contextmanager
def influxdb_connection(db_name):
    v_logger = app.config['LOGGER']
    v_influxdb_host = app.config['INFLUXDB_HOST']
    v_influxdb_port = app.config['INFLUXDB_PORT']

    _client = influxdb.InfluxDBClient(
        host=v_influxdb_host,
        port=v_influxdb_port,
        username='root',
        password='root',
        database=db_name
    )

    _dbs = _client.get_list_database()
    if db_name not in [_d['name'] for _d in _dbs]:
        v_logger.info(
            "InfluxDB database '{:s}' not found. Creating a new one.".
            format(db_name))
        _client.create_database(db_name)

    try:
        yield _client
    finally:
        _client.close()


@app.route("/query", methods=['POST'])
def query_data():
    v_logger = app.config['LOGGER']

    _payload = flask.request.form
    _db = _payload.get('db')
    _data = '&'.join(['='.join(_i) for _i in _payload.items()])

    with influxdb_connection(_db) as _client:
        try:
            _result = _client.request(
                'query',
                'POST',
                params=_data,
                expected_response_code=200)
            response_text = _result.text
            response_code = _result.status_code
        except (influxdb.exceptions.InfluxDBClientError, influxdb.exceptions.InfluxDBServerError) as ex:
            response_text = ex.content
            response_code = ex.code if hasattr(ex, 'code') else 500
            v_logger.error(ex)
            v_logger.exception(ex)

    _response = flask.make_response(response_text, response_code)

    return _response


def _write_to_influxdb(db, args, data):
    v_logger = app.config['LOGGER']
    with influxdb_connection(db) as _client:
        try:
            _result = _client.request(
                'write',
                'POST',
                params=args,
                data=data,
                expected_response_code=204)
            v_logger.debug("Insert data into InfluxDB: {:s}".format(str(data)))
            return dict(response=_result.text, status_code=_result.status_code)
        except (influxdb.exceptions.InfluxDBClientError, influxdb.exceptions.InfluxDBServerError) as ex:
            v_logger.error(ex)
            v_logger.exception(ex)
            #  only the client exception has a `code` attribute
            return dict(response=ex.content, status_code=ex.code if hasattr(ex, 'code') else 500)


@app.route("/write", methods=['POST'])
def publish_data():
    v_logger = app.config['LOGGER']

    v_mqtt_local_host = app.config['MQTT_LOCAL_HOST']
    v_mqtt_local_port = app.config['MQTT_LOCAL_PORT']
    v_topic = app.config['MQTT_TOPIC']

    _data = flask.request.get_data()
    _args = flask.request.args.to_dict()
    _db = _args.get('db')

    ret = _write_to_influxdb(_db, _args, _data)
    _response = flask.make_response(ret['response'], ret['status_code'])

    v_messages = []

    _station_id = 'IOTAWATT'
    try:
        v_payload = parse_write_payload(_data)
        v_logger.debug("request payload: %s", v_payload)

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        for v_point in v_payload:
            # Create a dictionary with the sensor data for this point.
            # The dictionary has format: {
            #   measurement_name (i.e., a value from PARAMETERS_MAP) : {
            #       'measurement': measurement_name,
            #       'timestamp': integer timestamp
            #       'dateObserved': datetime in UTC timezone and iso format
            _sensor_tree = defaultdict(dict)

            _timestamp = v_point.get('timestamp')
            if _timestamp is None:
                _timestamp = int(now.timestamp())
                _dateObserved = now.isoformat()
            else:
                _dateObserved = datetime.datetime.fromtimestamp(
                        int(_timestamp), tz=datetime.timezone.utc).isoformat()

            #v_logger.debug("Processing point %s", v_point)

            for label, measurement in PARAMETERS_MAP.items():
                value = get_value_from_point(label, measurement, v_point)
                if value:
                    v_logger.debug("Found value %s for %s_%s", value, measurement, label)
                    _sensor_tree[measurement].update({
                        measurement: value,
                        'timestamp': _timestamp,
                        'dateObserved': _dateObserved,
                    })
                #else:
                #    v_logger.debug("%s_%s not present in point", measurement, label)

            # Queue one message to be sent for each sensor
            for measurement, msg_data in _sensor_tree.items():
                _message = dict()
                _message["payload"] = json.dumps(msg_data)
                _message["topic"] = "EnergyMonitor/{}.{}".format(
                    _station_id, measurement)
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
    app.run(host='0.0.0.0')


if __name__ == "__main__":
    main()

# vim:ts=4:expandtab
# References:
#   http://blog.vwelch.com/2011/04/combining-configparser-and-argparse.html
