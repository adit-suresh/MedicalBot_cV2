import unittest
import os
import shutil
from unittest.mock import patch, mock_open
import base64

from src.email_handler.attachment_handler import AttachmentHandler
from src.utils.exceptions import AttachmentError
from config.settings import RAW_DATA_DIR

class TestAttachmentHandler(unittest.TestCase):
    def setUp(self):
        self.handler = AttachmentHandler()
        self.test_email_id = "test_email_123"
        self.test_attachment = {
            "name": "test.pdf",
            "contentBytes": base64.b64encode(b"test content").decode()
        }

        # Create temporary test directory
        os.makedirs(RAW_DATA_DIR, exist_ok=True)

    def tearDown(self):
        # Clean up test directory
        if os.path.exists(RAW_DATA_DIR):
            shutil.rmtree(RAW_DATA_DIR)

    def test_valid_attachment(self):
        # Test valid attachment types
        valid_attachments = [
            {"name": "test.pdf"},
            {"name": "test.xlsx"},
            {"name": "test.jpg"}
        ]
        for attachment in valid_attachments:
            self.assertTrue(self.handler.is_valid_attachment(attachment))

        # Test invalid attachment types
        invalid_attachments = [
            {"name": "test.exe"},
            {"name": "test.zip"},
            {"name": "test.doc"}
        ]
        for attachment in invalid_attachments:
            self.assertFalse(self.handler.is_valid_attachment(attachment))

    def test_save_attachment(self):
        with patch('builtins.open', mock_open()) as mock_file:
            path = self.handler.save_attachment(self.test_attachment, self.test_email_id)
            self.assertTrue(path.endswith(self.test_attachment["name"]))
            mock_file.assert_called_once()

    def test_save_attachment_error(self):
        with patch('builtins.open', side_effect=Exception("Write error")):
            with self.assertRaises(AttachmentError):
                self.handler.save_attachment(self.test_attachment, self.test_email_id)

    def test_process_attachments(self):
        attachments = [
            {"name": "valid.pdf", "contentBytes": base64.b64encode(b"test").decode()},
            {"name": "invalid.exe", "contentBytes": base64.b64encode(b"test").decode()},
            {"name": "valid.xlsx", "contentBytes": base64.b64encode(b"test").decode()}
        ]

        with patch('builtins.open', mock_open()):
            paths = self.handler.process_attachments(attachments, self.test_email_id)
            self.assertEqual(len(paths), 2)  # Only valid attachments should be processed

if __name__ == '__main__':
    unittest.main()