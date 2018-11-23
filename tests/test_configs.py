#!/usr/bin/env python
#
#  Copyright 2018, CRS4 - Center for Advanced Studies, Research and Development
#  in Sardinia
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

"""
This module tests:
    * that the GENERAL options defined for all the TDM modules are defined and
    work as expected;
    * the specific section overrides the GENERAL one;
    * the specific options work as expected;
    * the command line options override the configuration file.
"""

import os
import logging
import unittest

from unittest.mock import Mock
from iotawatt_publisher import configuration_parser
from iotawatt_publisher import APPLICATION_NAME
from iotawatt_publisher import (
    MQTT_LOCAL_HOST,
    MQTT_LOCAL_PORT,
    INFLUXDB_HOST,
    INFLUXDB_PORT,
    GPS_LOCATION)


COMMANDLINE_PARAMETERS = {
    'mqtt_local_host': {
        'cmdline': '--mqtt-host', 'default': MQTT_LOCAL_HOST},
    'mqtt_local_port': {
        'cmdline': '--mqtt-port', 'default': MQTT_LOCAL_PORT},
    'logging_level': {
        'cmdline': '--logging-level', 'default': logging.INFO},
    'influxdb_host': {
        'cmdline': '--influxdb-host', 'default': INFLUXDB_HOST},
    'influxdb_port': {
        'cmdline': '--influxdb-port', 'default': INFLUXDB_PORT},
    'gps_location': {
        'cmdline': '--gps-location', 'default': GPS_LOCATION}
}


class TestCommandLineParser(unittest.TestCase):
    """"
    Tests if the command line options override the settings in the
    configuration file.
    """

    def setUp(self):
        self._test_options = Mock()
        self._test_options.mqtt_local_host = 'mqtt_local_host_option'
        self._test_options.mqtt_local_port = MQTT_LOCAL_PORT + 10
        self._test_options.influxdb_host = 'influxdb_host_option'
        self._test_options.influxdb_port = INFLUXDB_PORT + 10
        self._test_options.logging_level = logging.INFO + 10
        self._test_options.gps_location = '1.2345678,1.2345678'

        self._test_configuration = Mock()
        self._test_configuration.mqtt_local_host = (
            'mqtt_local_host_configuration')
        self._test_configuration.mqtt_local_port = MQTT_LOCAL_PORT + 20
        self._test_configuration.influxdb_host = 'influxdb_host_configuration'
        self._test_configuration.influxdb_port = INFLUXDB_PORT + 20
        self._test_configuration.logging_level = logging.INFO + 20
        self._test_configuration.gps_location = '3.6925814,4.8260482'

        self._config_file = '/tmp/config.ini'

        _f = open(self._config_file, "w")
        _f.write("[{:s}]\n".format(APPLICATION_NAME))
        _f.write(
            "mqtt_local_host = {}\n".
            format(self._test_configuration.mqtt_local_host))
        _f.write(
            "mqtt_local_port = {}\n".
            format(self._test_configuration.mqtt_local_port))
        _f.write("influxdb_host = {}\n".format(
            self._test_configuration.influxdb_host))
        _f.write("influxdb_port = {}\n".format(
            self._test_configuration.influxdb_port))
        _f.write("logging_level = {}\n".format(
            self._test_configuration.logging_level))
        _f.write("gps_location  = {}\n".format(
            self._test_configuration.gps_location))
        _f.close()

    def test_command_line_long(self):
        """"
        Tests if the command line options are parsed.
        """
        _cmd_line = []

        _cmd_line.extend(['--config-file', None])
        _cmd_line.extend(
            ['--mqtt-host', str(self._test_options.mqtt_local_host)])
        _cmd_line.extend(
            ['--mqtt-port', str(self._test_options.mqtt_local_port)])
        _cmd_line.extend(
            ['--influxdb-host', str(self._test_options.influxdb_host)])
        _cmd_line.extend(
            ['--influxdb-port', str(self._test_options.influxdb_port)])
        _cmd_line.extend(
            ['--logging-level', str(self._test_options.logging_level)])
        _cmd_line.extend(
            ['--gps-location', str(self._test_options.gps_location)])

        _args = configuration_parser(_cmd_line)

        self.assertEqual(
            self._test_options.mqtt_local_host, _args.mqtt_local_host)
        self.assertEqual(
            self._test_options.mqtt_local_port, _args.mqtt_local_port)
        self.assertEqual(
            self._test_options.logging_level, _args.logging_level)
        self.assertEqual(
            self._test_options.influxdb_host, _args.influxdb_host)
        self.assertEqual(
            self._test_options.influxdb_port, _args.influxdb_port)
        self.assertEqual(
            self._test_options.gps_location, _args.gps_location)

    def test_command_line_long_override(self):
        """"
        Tests if the command line options override the settings in the
        configuration file (long options).
        """
        _cmd_line = []

        _cmd_line.extend(
            ['--config-file', str(self._config_file)])
        _cmd_line.extend(
            ['--mqtt-host', str(self._test_options.mqtt_local_host)])
        _cmd_line.extend(
            ['--mqtt-port', str(self._test_options.mqtt_local_port)])
        _cmd_line.extend(
            ['--influxdb-host', str(self._test_options.influxdb_host)])
        _cmd_line.extend(
            ['--influxdb-port', str(self._test_options.influxdb_port)])
        _cmd_line.extend(
            ['--logging-level', str(self._test_options.logging_level)])
        _cmd_line.extend(
            ['--gps-location', str(self._test_options.gps_location)])

        _args = configuration_parser(_cmd_line)

        self.assertEqual(
            self._test_options.mqtt_local_host, _args.mqtt_local_host)
        self.assertEqual(
            self._test_options.mqtt_local_port, _args.mqtt_local_port)
        self.assertEqual(
            self._test_options.logging_level, _args.logging_level)
        self.assertEqual(
            self._test_options.influxdb_host, _args.influxdb_host)
        self.assertEqual(
            self._test_options.influxdb_port, _args.influxdb_port)
        self.assertEqual(
            self._test_options.gps_location, _args.gps_location)

    def test_command_line_long_partial_override(self):
        """"
        Tests if the command line options override the settings in the
        configuration file (long options).
        """
        for _opt, _par in COMMANDLINE_PARAMETERS.items():
            _cmd_line = ['--config-file', str(self._config_file)]
            _cmd_line.extend([
                _par['cmdline'],
                str(getattr(self._test_options, _opt))])

            _args = configuration_parser(_cmd_line)

            self.assertEqual(
                getattr(_args, _opt),
                getattr(self._test_options, _opt))

            for _cfg, _val in COMMANDLINE_PARAMETERS.items():
                if _cfg == _opt:
                    continue
                self.assertEqual(
                    getattr(_args, _cfg),
                    getattr(self._test_configuration, _cfg))

    def tearDown(self):
        os.remove(self._config_file)


class TestGeneralSectionConfigFileParser(unittest.TestCase):
    """"
    Checks if the GENERAL section options are present in the parser, their
    default values are defined and the GENERAL SECTION of configuration file is
    read and parsed.
    """

    def setUp(self):
        self._default = Mock()
        self._default.mqtt_local_host = MQTT_LOCAL_HOST
        self._default.mqtt_local_port = MQTT_LOCAL_PORT
        self._default.influxdb_host = INFLUXDB_HOST
        self._default.influxdb_port = INFLUXDB_PORT
        self._default.logging_level = logging.INFO
        self._default.gps_location = GPS_LOCATION

        self._test = Mock()
        self._test.mqtt_local_host = 'mqtt_local_host_test'
        self._test.mqtt_local_port = MQTT_LOCAL_PORT + 100
        self._test.influxdb_host = 'influxdb_host_test'
        self._test.influxdb_port = INFLUXDB_PORT + 100
        self._test.logging_level = logging.INFO + 10
        self._test.gps_location = '1.2345678,1.2345678'

        self._override = Mock()
        self._override.mqtt_local_host = 'mqtt_local_host_override'
        self._override.mqtt_local_port = MQTT_LOCAL_PORT + 200
        self._override.influxdb_host = 'influxdb_host_override'
        self._override.influxdb_port = INFLUXDB_PORT + 200
        self._override.logging_level = logging.INFO + 20
        self._override.gps_location = '1.2345678,1.2345678'

        self._config_file = '/tmp/config.ini'
        _f = open(self._config_file, "w")
        _f.write("[GENERAL]\n")
        _f.write("mqtt_local_host = {}\n".format(self._test.mqtt_local_host))
        _f.write("mqtt_local_port = {}\n".format(self._test.mqtt_local_port))
        _f.write("influxdb_host = {}\n".format(self._test.influxdb_host))
        _f.write("influxdb_port = {}\n".format(self._test.influxdb_port))
        _f.write("logging_level = {}\n".format(self._test.logging_level))
        _f.write("gps_location  = {}\n".format(self._test.gps_location))
        _f.close()

    def test_general_arguments(self):
        """
        Checks the presence of the GENERAL section in the parser.
        """
        _cmd_line = []
        _args = configuration_parser(_cmd_line)

        self.assertIn('mqtt_local_host', _args)
        self.assertIn('mqtt_local_port', _args)
        self.assertIn('logging_level', _args)
        self.assertIn('influxdb_host', _args)
        self.assertIn('influxdb_port', _args)
        self.assertIn('gps_location', _args)

    def test_general_default(self):
        """
        Checks the defaults of the GENERAL section in the parser.
        """
        _cmd_line = []
        _args = configuration_parser(_cmd_line)

        self.assertEqual(self._default.mqtt_local_host, _args.mqtt_local_host)
        self.assertEqual(self._default.mqtt_local_port, _args.mqtt_local_port)
        self.assertEqual(self._default.logging_level, _args.logging_level)
        self.assertEqual(self._default.influxdb_host, _args.influxdb_host)
        self.assertEqual(self._default.influxdb_port, _args.influxdb_port)
        self.assertEqual(self._default.gps_location, _args.gps_location)

    def test_general_options(self):
        """
        Tests the parsing of the options in the GENERAL section.
        """
        _cmd_line = ['-c', self._config_file]
        _args = configuration_parser(_cmd_line)

        self.assertEqual(self._test.mqtt_local_host, _args.mqtt_local_host)
        self.assertEqual(self._test.mqtt_local_port, _args.mqtt_local_port)
        self.assertEqual(self._test.logging_level, _args.logging_level)
        self.assertEqual(self._test.influxdb_host, _args.influxdb_host)
        self.assertEqual(self._test.influxdb_port, _args.influxdb_port)
        self.assertEqual(self._test.gps_location, _args.gps_location)

        self.assertNotEqual(
            _args.mqtt_local_host,
            self._default.mqtt_local_host)
        self.assertNotEqual(
            _args.mqtt_local_port,
            self._default.mqtt_local_port)
        self.assertNotEqual(
            _args.logging_level,
            self._default.logging_level)
        self.assertNotEqual(
            _args.influxdb_host,
            self._default.influxdb_host)
        self.assertNotEqual(
            _args.influxdb_port,
            self._default.influxdb_port)
        self.assertNotEqual(
            _args.gps_location,
            self._default.gps_location)

    def test_general_override_options(self):
        """
        Tests if the options in the GENERAL section are overridden by the same
        options in the specific section.
        """
        _config_specific_override_file = '/tmp/override_config.ini'

        _f = open(_config_specific_override_file, "w")
        _f.write("[GENERAL]\n")
        _f.write(
            "mqtt_local_host = {}\n".
            format(self._test.mqtt_local_host))
        _f.write(
            "mqtt_local_port = {}\n".
            format(self._test.mqtt_local_port))
        _f.write(
            "influxdb_host = {}\n".
            format(self._test.influxdb_host))
        _f.write(
            "influxdb_port = {}\n".
            format(self._test.influxdb_port))
        _f.write(
            "logging_level = {}\n".
            format(self._test.logging_level))
        _f.write(
            "gps_location  = {}\n".
            format(self._test.gps_location))
        _f.write("[{:s}]\n".format(APPLICATION_NAME))
        _f.write(
            "mqtt_local_host = {}\n".
            format(self._override.mqtt_local_host))
        _f.write(
            "mqtt_local_port = {}\n".
            format(self._override.mqtt_local_port))
        _f.write(
            "influxdb_host = {}\n".
            format(self._override.influxdb_host))
        _f.write(
            "influxdb_port = {}\n".
            format(self._override.influxdb_port))
        _f.write(
            "logging_level = {}\n".
            format(self._override.logging_level))
        _f.write(
            "gps_location  = {}\n".
            format(self._override.gps_location))
        _f.close()

        _cmd_line = ['-c', _config_specific_override_file]
        _args = configuration_parser(_cmd_line)

        self.assertEqual(self._override.mqtt_local_host, _args.mqtt_local_host)
        self.assertEqual(self._override.mqtt_local_port, _args.mqtt_local_port)
        self.assertEqual(self._override.logging_level, _args.logging_level)
        self.assertEqual(self._override.influxdb_host, _args.influxdb_host)
        self.assertEqual(self._override.influxdb_port, _args.influxdb_port)
        self.assertEqual(self._override.gps_location, _args.gps_location)

        os.remove(_config_specific_override_file)

    def tearDown(self):
        os.remove(self._config_file)


if __name__ == '__main__':
    unittest.main()
