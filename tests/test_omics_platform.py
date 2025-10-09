'''
Test Module for AWS Omics Platform
'''
import unittest
import os
from unittest.mock import MagicMock, patch
import json

from cwl_platform.omics_platform import OmicsPlatform

class TestOmicsPlatform(unittest.TestCase):
    '''
    Test Class for Omics Platform
    '''
    def setUp(self) -> None:
        self.platform = OmicsPlatform('Omics')
        self.platform.logger = MagicMock()
        self.platform.output_bucket = "test-bucket"
        self.platform.role_arn = "arn:aws:iam::123456789012:role/test-role"
        self.platform.s3_client = MagicMock()
        self.platform.api = MagicMock()
        return super().setUp()

    def test_copy_folder_returns_source(self):
        self.assertEqual(
            self.platform.copy_folder("proj1", "s3://bucket/folder/", "proj2"),
            "s3://bucket/folder/"
        )

    def test_get_folder_id_trailing_slash(self):
        self.assertEqual(
            self.platform.get_folder_id("proj", "s3://bucket/folder/"),
            "s3://bucket/folder/"
        )
        self.assertEqual(
            self.platform.get_folder_id("proj", "s3://bucket/folder"),
            "s3://bucket/folder/"
        )

    def test_get_file_id(self):
        self.assertEqual(
            self.platform.get_file_id("proj", "s3://bucket/file.txt"),
            "s3://bucket/file.txt"
        )

    def test_get_project_by_id(self):
        project_id = "abc123"
        result = self.platform.get_project_by_id(project_id)
        self.assertEqual(result, {"RunGroupId": project_id})

    def test_get_project_by_name_found(self):
        self.platform.api.list_run_groups.return_value = {
            "items": [{"id": "group1"}]
        }
        result = self.platform.get_project_by_name("testproj")
        self.assertEqual(result, {"RunGroupId": "group1"})

    def test_get_project_by_name_not_found(self):
        self.platform.api.list_run_groups.return_value = {"items": []}
        with patch("cwl_platform.omics_platform.logger") as mock_logger:
            result = self.platform.get_project_by_name("notfound")
            self.assertEqual(result, {})
            mock_logger.error.assert_called()

    def test_get_task_state(self):
        self.platform.api.get_run.return_value = {"status": "COMPLETED"}
        self.assertEqual(
            self.platform.get_task_state({"id": "runid"}), "Complete"
        )
        self.platform.api.get_run.return_value = {"status": "FAILED"}
        self.assertEqual(
            self.platform.get_task_state({"id": "runid"}), "Failed"
        )
        for status in ["STARTING", "RUNNING", "STOPPING"]:
            self.platform.api.get_run.return_value = {"status": status}
            self.assertEqual(
                self.platform.get_task_state({"id": "runid"}), "Running"
            )
        for status in ["CANCELLED", "DELETED"]:
            self.platform.api.get_run.return_value = {"status": status}
            self.assertEqual(
                self.platform.get_task_state({"id": "runid"}), "Cancelled"
            )
        self.platform.api.get_run.return_value = {"status": "PENDING"}
        self.assertEqual(
            self.platform.get_task_state({"id": "runid"}), "Queued"
        )
        self.platform.api.get_run.return_value = {"status": "UNKNOWN"}
        with self.assertRaises(ValueError):
            self.platform.get_task_state({"id": "runid"})

    def test_get_task_output(self):
        # Simulate S3 and API responses
        self.platform.api.get_run.return_value = {
            "outputUri": "s3://test-bucket/path/"
        }
        outputs_json = {
            "output1": [{"location": "s3://bucket/file1.txt"}, {"location": "s3://bucket/file2.txt"}],
            "output2": {"location": "s3://bucket/file3.txt"}
        }
        s3_response = {
            "Body": MagicMock()
        }
        s3_response["Body"].read.return_value = json.dumps(outputs_json).encode("utf-8")
        self.platform.s3_client.get_object.return_value = s3_response

        # List output
        result = self.platform.get_task_output({"id": "runid"}, "output1")
        self.assertEqual(result, ["s3://bucket/file1.txt", "s3://bucket/file2.txt"])
        # Single output
        result = self.platform.get_task_output({"id": "runid"}, "output2")
        self.assertEqual(result, "s3://bucket/file3.txt")
        # Missing output
        with self.assertRaises(KeyError):
            self.platform.get_task_output({"id": "runid"}, "missing")

    def test_get_task_output_filename(self):
        with patch.object(self.platform, "get_task_output") as mock_get_task_output:
            mock_get_task_output.return_value = ["s3://bucket/file1.txt", "s3://bucket/file2.txt"]
            result = self.platform.get_task_output_filename({"id": "runid"}, "output1")
            self.assertEqual(result, ["file1.txt", "file2.txt"])
            mock_get_task_output.return_value = "s3://bucket/file3.txt"
            result = self.platform.get_task_output_filename({"id": "runid"}, "output2")
            self.assertEqual(result, "file3.txt")

    def test_get_project_raises(self):
        with self.assertRaises(ValueError):
            self.platform.get_project()

    def test_get_user_raises(self):
        with self.assertRaises(ValueError):
            self.platform.get_user("user")

    def test_rename_file_raises(self):
        with self.assertRaises(ValueError):
            self.platform.rename_file("fileid", "newname")

    def test_roll_file_raises(self):
        with self.assertRaises(ValueError):
            self.platform.roll_file("proj", "file.txt")

    def test_submit_task_success(self):
        self.platform.output_bucket = "bucket"
        self.platform.role_arn = "arn:aws:iam::123456789012:role/test-role"
        self.platform.api.start_run.return_value = {"id": "runid"}
        project = {"RunGroupId": "groupid"}
        workflow = "workflowid"
        parameters = {"input": "value"}
        result = self.platform.submit_task("sample", project, workflow, parameters)
        self.assertEqual(result, {"id": "runid"})

    def test_submit_task_failure(self):
        self.platform.output_bucket = "bucket"
        self.platform.role_arn = "arn:aws:iam::123456789012:role/test-role"
        self.platform.api.start_run.side_effect = Exception("fail")
        project = {"RunGroupId": "groupid"}
        workflow = "workflowid"
        parameters = {"input": "value"}
        with self.assertRaises(Exception):
            self.platform.submit_task("sample", project, workflow, parameters)

    def test_upload_file_success(self):
        self.platform.output_bucket = "bucket"
        self.platform.s3_client.upload_file = MagicMock()
        project = {"RunGroupId": "groupid"}
        filename = "/tmp/file.txt"
        dest_folder = "/dest/"
        destination_filename = "newfile.txt"
        file_id = self.platform.upload_file(filename, project, dest_folder, destination_filename)
        self.assertTrue(file_id.startswith("s3://bucket/Project/groupid/dest/newfile.txt"))

    def test_upload_file_failure(self):
        self.platform.output_bucket = "bucket"
        self.platform.s3_client.upload_file.side_effect = Exception("fail")
        project = {"RunGroupId": "groupid"}
        filename = "/tmp/file.txt"
        dest_folder = "/dest/"
        destination_filename = "newfile.txt"
        file_id = self.platform.upload_file(filename, project, dest_folder, destination_filename)
        self.assertIsNone(file_id)

    def test_detect_classmethod(self):
        self.assertFalse(OmicsPlatform.detect())

if __name__ == '__main__':
    unittest.main()
