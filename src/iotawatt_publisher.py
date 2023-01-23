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
INFLUXDB_DB = "iotawatt"          # INFLUXDB database
INFLUXDB_HOST = "localhost"     # INFLUXDB address
INFLUXDB_PORT = 8086            # INFLUXDB port
INFLUXDB_USER = "root"          # INFLUXDB user
INFLUXDB_PASSWORD = "root"      # INFLUXDB password
GPS_LOCATION = "0.0,0.0"        # DEFAULT location


APPLICATION_NAME = 'IOTAWATT_publisher'

app = flask.Flask(__name__)


## We expect to receive measurements of fields from the iotawatt
# with names like "{value}_{key}" with the key-value pairs from this
# dictionary.  For instance, "current_A", "powerFactor_PF".
PARAMETERS_MAP = {
    'I': 'current',
    'S': 'apparentPower',
    'PF': 'powerFactor',
    'P': 'realPower',
    'E': 'consumedEnergy',
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
    """
    parts = line.split()
    if len(parts) < 2 or len(parts) > 3:
        raise ValueError(
                "Unexpected line format with {} parts (expected 2 or 3) -> {}".format(
                    len(parts), line))

    measurement, _, tagset = parts[0].partition(',')
    tags = [ f.split('=') for f in tagset.split(',') if re.fullmatch('.*=.*', f) ]

    fields = [ f.split('=') for f in parts[1].split(',') ]
    point = dict(
            measurement=measurement,
            tags=tags,
            fields=fields)
    if len(parts) == 3:
        point['timestamp'] = parts[2]
    else:
        point['timestamp'] = None

    return point


def is_compliant(name):
    _sensor, _, _label = name.partition('_')
    return (_label in MESSAGE_PARAMETERS)


def get_parameter(name):
    _sensor, _, _label = name.partition('_')
    _parameter = PARAMETERS_MAP[_label]
    return _sensor, _parameter


@contextmanager
def influxdb_connection(db_name):
    v_logger = app.config['LOGGER']
    v_influxdb_host = app.config['INFLUXDB_HOST']
    v_influxdb_port = app.config['INFLUXDB_PORT']
    v_influxdb_user = app.config['INFLUXDB_USER']
    v_influxdb_password = app.config['INFLUXDB_PASSWORD']

    _client = influxdb.InfluxDBClient(
        host=v_influxdb_host,
        port=v_influxdb_port,
        username=v_influxdb_user,
        password=v_influxdb_password,
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
    _data_dict = { item[0].lower(): item[1] for item in _payload.items() }

    v_logger.debug('Query is: "%s"', _data)

    # The required 'db' must match the configured db
    if _db != app.config['INFLUXDB_DB']:
        v_logger.error('Query not allowed: invalid db.')
        _response = flask.make_response('Query not allowed: invalid db.', 400)
        return _response

    # This allows only the 'LAST' query to be made and blocks other queries
    allowed_query = r'^\s*SELECT\s+LAST\s*\(.*\)\s*FROM.*'
    if not re.match(allowed_query, _data_dict['q']):
        v_logger.error('Query not allowed: invalid query.')
        _response = flask.make_response('Query not allowed: invalid query.', 400)
        return _response

    with influxdb_connection(_db) as _client:
        try:
            _result = _client.request(
                'query',
                'POST',
                params=_data,
                headers={'Accept': 'application/json'},
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

    # The required 'db' must match the configured db
    if _db != app.config['INFLUXDB_DB']:
        v_logger.error('Query not allowed: invalid db.')
        _response = flask.make_response('Query not allowed: invalid db.', 400)
        return _response

    ret = _write_to_influxdb(_db, _args, _data)
    _response = flask.make_response(ret['response'], ret['status_code'])

    v_messages = []

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

            # If one or more tag set with key 'node' are present their values
            # are appended separated by '-' to form the station id
            _node_ids = [ tag[1] for tag in v_point['tags']
                         if tag[0].lower() == 'node' and len(tag[1]) > 0 ]
            _station_id = '-'.join(_node_ids) if len(_node_ids) else 'IOTAWATT'

            # There are two ways to configure InfluxDB in IotaWatt posting:
            # the first one is to not specify the 'measurement' and 'field'
            # values. In this case the measurement value is the channel name
            # and the field name is 'value', e.g: 'CHANNEL_A value=20.0'
            if is_compliant(v_point['measurement']):
                _sensor, _parameter = get_parameter(v_point['measurement'])
                for field in v_point['fields']:
                    _value = field[1]
                    _sensor_tree[_sensor].update({
                        _parameter: _value,
                        'timestamp': _timestamp,
                        'dateObserved': _dateObserved,
                    })
            # The second way is to specify the 'measurement' value and set the
            # value '$name' as field name. In this case the field name is the
            # channel name, e.g: 'MEASUREMENT CHANNEL_A=20.0'
            else:
                for field in v_point['fields']:
                    if is_compliant(field[0]):
                        _sensor, _parameter = get_parameter(field[0])
                        _value = field[1]
                        _sensor_tree[_sensor].update({
                            _parameter: _value,
                            'timestamp': _timestamp,
                            'dateObserved': _dateObserved,
                        })
                    else:
                        v_logger.debug(
                            "Field '%s' for measurement '%s' is not compliant",
                            field[0], v_point['measurement'])

            # Queue one message to be sent for each sensor
            for measurement, msg_data in _sensor_tree.items():
                _message = dict()
                _message["payload"] = json.dumps(msg_data)
                _message["topic"] = "EnergyMonitor/{}.{}".format(
                    _station_id, measurement)
                _message['qos'] = 0
                _message['retain'] = False

                v_messages.append(_message)

        # Trying to multi-publish an empty array of messages may results in
        # freezes and/or other odd behaviours
        if len(v_messages) > 0:
            v_logger.debug(
                "Message topic:\'{:s}\', broker:\'{:s}:{:d}\', "
                "message:\'{:s}\'".format(
                    v_topic, v_mqtt_local_host, v_mqtt_local_port,
                    json.dumps(v_messages)))

            publish.multiple(v_messages, hostname=v_mqtt_local_host,
                         port=v_mqtt_local_port)

    except socket.error as ex:
        v_logger.error(ex)
        v_logger.exception(ex)

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
        'influxdb_db'   : INFLUXDB_DB,
        'influxdb_host' : INFLUXDB_HOST,
        'influxdb_port' : INFLUXDB_PORT,
        'influxdb_user' : INFLUXDB_USER,
        'influxdb_password' : INFLUXDB_PASSWORD,
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
        '--influxdb-user', dest='influxdb_user', action='store',
        type=int,
        help='username for influx database (default: {})'.format(INFLUXDB_USER))
    parser.add_argument(
        '--influxdb-password', dest='influxdb_password', action='store',
        type=int,
        help='password for influx database (default: {})'.format(INFLUXDB_PASSWORD))
    parser.add_argument(
        '--influxdb-db', dest='influxdb_db', action='store',
        type=str,
        help='name of the database to use (default: {})'.format(INFLUXDB_DB))
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

        'INFLUXDB_DB' : args.influxdb_db,
        'INFLUXDB_HOST' : args.influxdb_host,
        'INFLUXDB_PORT' : args.influxdb_port,
        'INFLUXDB_USER' : args.influxdb_user,
        'INFLUXDB_PASSWORD' : args.influxdb_password,
    }

    app.config.from_mapping(config_dict)
    app.run(host='0.0.0.0')


if __name__ == "__main__":
    main()

# vim:ts=4:expandtab
# References:
#   http://blog.vwelch.com/2011/04/combining-configparser-and-argparse.html
