import pytest

from easylink.utilities.data_utils import download_image

TEST_FILE_RECORD_ID = 15611084
TEST_FILE_NAME = "dummy_file"
TEST_FILE_MD5_CHECKSUM = "7f6be7a98d2ce04e8f7b473926c0bab0"


def test_download_image(tmp_path):
    download_image(tmp_path, TEST_FILE_RECORD_ID, TEST_FILE_NAME, TEST_FILE_MD5_CHECKSUM)

    assert (tmp_path / TEST_FILE_NAME).exists()
    with open(tmp_path / TEST_FILE_NAME, "r") as f:
        content = f.read()
    assert content.strip() == "This is a test file; do not delete."


def test_download_image_raises_incorrect_md5_checksum(tmp_path):
    with pytest.raises(ValueError, match="MD5 checksum does not match"):
        download_image(tmp_path, TEST_FILE_RECORD_ID, TEST_FILE_NAME, "invalid_checksum")
