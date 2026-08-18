'''
Test PlatformFactory
'''
import unittest
from unittest.mock import patch

from cwl_platform import (
    ArvadosPlatform,
    NGS360Platform,
    PlatformFactory,
    SevenBridgesPlatform,
    _redact,
)


class TestRedact(unittest.TestCase):
    '''
    Test the environment variable redaction helper
    '''

    def test_secret_names_are_masked(self):
        ''' Test values of credential-looking variables are masked '''
        for name in (
                'NGS360_AUTH_TOKEN',
                'WES_SERVICE_KEY',
                'SB_AUTH_TOKEN',
                'AWS_SECRET_ACCESS_KEY',
                'DB_PASSWORD',
                'api_key'
        ):
            with self.subTest(name=name):
                self.assertEqual(_redact(name, 'supersecret'), '<redacted, 11 chars>')

    def test_other_names_are_left_alone(self):
        ''' Test values that are not credentials are reported as-is '''
        self.assertEqual(
            _redact('WES_API_ENDPOINT', 'https://wes.example.com'),
            'https://wes.example.com'
        )


class TestDetectPlatform(unittest.TestCase):
    '''
    Test PlatformFactory.detect_platform
    '''

    def test_undetected_platform_dump_is_redacted(self):
        '''
        Test the environment dump on failure does not log credentials
        '''
        env = {
            'WES_API_ENDPOINT': 'https://wes.example.com',
            'WES_SERVICE_KEY': 'supersecret',
        }
        with patch.dict('os.environ', env), \
                patch.object(ArvadosPlatform, 'detect', return_value=False), \
                patch.object(SevenBridgesPlatform, 'detect', return_value=False), \
                patch.object(NGS360Platform, 'detect', return_value=False):
            with self.assertLogs(level='INFO') as logs:
                with self.assertRaises(ValueError):
                    PlatformFactory().detect_platform()

        output = '\n'.join(logs.output)
        self.assertNotIn('supersecret', output)
        self.assertIn('WES_SERVICE_KEY: <redacted, 11 chars>', output)
        # Non-credential variables are still there to diagnose the failure with
        self.assertIn('WES_API_ENDPOINT: https://wes.example.com', output)


if __name__ == '__main__':
    unittest.main()
