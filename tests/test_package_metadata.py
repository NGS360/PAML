'''
Test Module for package-level metadata
'''
import unittest

import cwl_platform


class TestPackageMetadata(unittest.TestCase):
    ''' Tests for the metadata cwl_platform exposes at import time '''

    def test_version_is_a_non_empty_string(self):
        '''
        __version__ is derived from the installed distribution metadata, which
        hatch-vcs populates from the git tag. This guards the import-time
        lookup rather than any particular version number, since the value
        legitimately differs between a tagged release and a working tree.
        '''
        self.assertIsInstance(cwl_platform.__version__, str)
        self.assertTrue(cwl_platform.__version__)


if __name__ == '__main__':
    unittest.main()
