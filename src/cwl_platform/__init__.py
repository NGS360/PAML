'''
CWL Execution Platform Implementations
'''
import logging
import os
from importlib.metadata import PackageNotFoundError, version

from .arvados_platform import ArvadosPlatform
from .sevenbridges_platform import SevenBridgesPlatform
from .ngs360_platform import NGS360Platform

try:
    __version__ = version("cwl_platform")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "unknown"

# Move this for a config file
SUPPORTED_PLATFORMS = {
    'Arvados': ArvadosPlatform,
    'SevenBridges': SevenBridgesPlatform,
    'NGS360': NGS360Platform
}


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

        # If we can't detect the platform, log which environment variables
        # are set to help diagnose it, and raise an error. Names only: values
        # routinely hold credentials and must not reach the logs.
        logging.debug("Unable to detect platform. Environment variables set: %s",
                      ", ".join(sorted(os.environ)))

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
