import pytest
from moto import mock_aws
import boto3

import sys
import os

# Add the src directory to PYTHONPATH
ROOT = os.path.dirname(os.path.dirname(__file__))
SRC = os.path.join(ROOT, "src")

sys.path.insert(0, SRC)


@pytest.fixture
def s3():
    """
    Creates a mocked AWS environment with moto.
    Returns a boto3 client (you can create buckets/keys inside each test).
    """
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        yield client
