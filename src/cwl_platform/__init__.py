'''
CWL Execution Platform Implementations
'''
import logging
import os

from .arvados_platform import ArvadosPlatform
from .sevenbridges_platform import SevenBridgesPlatformUS, \
    SevenBridgesPlatformCN
#from .omics_platform import OmicsPlatform

# Move this for a config file
SUPPORTED_PLATFORMS = {
    'Arvados': ArvadosPlatform,
#    'Omics': OmicsPlatform,
    'SevenBridges_US': SevenBridgesPlatformUS,
    'SevenBridges_CN': SevenBridgesPlatformCN
}


class PlatformFactory():
    ''' PlatformFactory '''

    def __init__(self):
        self._creators = {}
        for platform, creator in SUPPORTED_PLATFORMS.items():
            self._creators[platform] = creator

    def detect_platform(self, credentials):
        '''
        Detect what platform we are running on
        '''
        for platform, creator in SUPPORTED_PLATFORMS.items():
            if creator.detect(credentials):
                return platform

        # If we can't detect the platform, print out environment variables and raise an error
        logging.info("Environment Variables:")
        for name, value in os.environ.items():
            logging.info("%s: %s", name, value)

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
