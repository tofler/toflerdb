'''ToflerDB Configuration Management Module

This purpose of this module is to provide easy access to Tofler's
configuration file located either at the default location or at
a location as specified in a call.

This also provides a mechanism to alter an existing configuration

By default, the Configuration class will look for configuration
files in the following order:
    1. the location file (if) specified in the constructor
    2. file specified in the TOFLERDB_CONF environment variable
    3. $HOME/.tofler.conf

The configuration file must be a json encoded dictionary
with each "section" being a top-level key in the dictionary
'''

import os
import json


def get_base_config():
    return {}


class ConfigFileNotFoundError(ValueError):
    '''Raised when the specified configuration file is not found'''


class ConfigurationParseError(ValueError):
    '''Raised when the configuration could not be parsed correctly'''


class ConfigurationAlreadyLoadedError(ValueError):
    '''Raised when there is an attempt to re-load a configuration'''


class SectionNotFoundError(ValueError):
    '''Raised when a requested section is not found in the configuration'''


class Configuration(object):
    '''This class contains a single instance of a configuration'''

    def __init__(self, location=None):
        self._config = {}
        self._config_file = None
        if location is None:
            location = os.getenv('TOFLERDB_CONF')
            if location is None or location.strip() == '':
                home = os.getenv('HOME')
                location = os.path.join(home, '.toflerdb.conf')
        if not os.path.exists(location):
            # raise ConfigFileNotFoundError('%s was not found' % location)
            pass
        else:
            self._config_file = location
        self._load_config()

    def _load_config(self):
        '''Private method to load configuration file'''
        self._config = get_base_config()

        if self._config_file:
            config_file = open(self._config_file, 'r')
            text = config_file.read()
            config_file.close()
            try:
                self._config.update(json.loads(text))
            except ValueError:
                raise ConfigurationParseError('Error parsing configuration')

    def get_config(self, section):
        '''Public method to access configuration for a section'''

        if section not in self._config:
            raise SectionNotFoundError(
                '%s not found in configuration' % section)
        return self._config[section]
