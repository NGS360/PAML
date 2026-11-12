'''
Tests for PlatformFactory.detect_platform
'''
import os
import unittest
from unittest import mock

from cwl_platform import PlatformFactory

SECRET_VALUE = 'super-secret-token-value'


class TestDetectPlatformLogging(unittest.TestCase):
    ''' detect_platform must not write environment variable values to the logs '''

    def test_detect_platform_failure_raises(self):
        ''' With no platform variables set, detection raises ValueError '''
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                PlatformFactory().detect_platform()

    def test_detect_platform_failure_does_not_log_values(self):
        '''
        On detection failure, variable names may be logged but no value may
        appear in any log record at any level
        '''
        env = {
            'NGS360_AUTH_TOKEN': SECRET_VALUE,
            'HARMLESS_SETTING': 'visible-value',
        }
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertLogs(level='DEBUG') as logs:
                with self.assertRaises(ValueError):
                    PlatformFactory().detect_platform()

        combined = '\n'.join(logs.output)
        self.assertNotIn(SECRET_VALUE, combined)
        self.assertNotIn('visible-value', combined)

    def test_detect_platform_failure_logs_variable_names(self):
        ''' Variable names are still logged, so a failed detection can be diagnosed '''
        env = {'NGS360_AUTH_TOKEN': SECRET_VALUE}
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertLogs(level='DEBUG') as logs:
                with self.assertRaises(ValueError):
                    PlatformFactory().detect_platform()

        self.assertTrue(any('NGS360_AUTH_TOKEN' in line for line in logs.output))


if __name__ == '__main__':
    unittest.main()
