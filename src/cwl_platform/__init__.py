'''
CWL Execution Platform Implementations
'''
import logging
import os

from .arvados_platform import ArvadosPlatform
from .sevenbridges_platform import SevenBridgesPlatform
from .ngs360_platform import NGS360Platform

# Move this for a config file
SUPPORTED_PLATFORMS = {
    'Arvados': ArvadosPlatform,
    'SevenBridges': SevenBridgesPlatform,
    'NGS360': NGS360Platform
}

# Substrings that mark an environment variable as carrying a credential. The
# environment dump below goes to whatever collects the container's logs, which
# is a wider audience than whoever provisioned the credential.
_SECRET_NAME_MARKERS = ('TOKEN', 'KEY', 'SECRET', 'PASSWORD')


def _redact(name, value):
    '''
    Return an environment variable's value, masked if the name marks it secret

    :param name: Environment variable name
    :param value: Environment variable value
    :return: The value, or a mask of it
    '''
    if any(marker in name.upper() for marker in _SECRET_NAME_MARKERS):
        # Length is kept because "set but empty" and "set to a truncated value"
        # are both real misconfigurations worth telling apart in a log.
        return f"<redacted, {len(value)} chars>"
    return value


class PlatformFactory():
    ''' PlatformFactory '''

    def __init__(self):
        self._creators = {}
        for platform, creator in SUPPORTED_PLATFORMS.items():
            self._creators[platform] = creator

    def detect_platform(self):
        '''
        Detect what platform we are running on
        '''
        for platform, creator in SUPPORTED_PLATFORMS.items():
            if creator.detect():
                return platform

        # If we can't detect the platform,
        # print out environment variables and raise an error
        logging.info("Environment Variables:")
        for name, value in os.environ.items():
            logging.info("%s: %s", name, _redact(name, value))

        raise ValueError("Unable to detect platform")

    def get_platform(self, platform):
        '''
        Create a project type
        '''
        creator = self._creators.get(platform)
        if creator:
            return creator(platform)
        raise ValueError(f"Unknown platform: {platform}")

    def register_platform_type(self, platform, creator):
        '''
        Register a platform with the factory
        '''
        self._creators[platform] = creator
