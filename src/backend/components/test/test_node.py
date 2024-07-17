import pytest
from pydantic import ValidationError, AnyUrl
from uuid import uuid4, uuid1

from backend.components.node import Node

local_uri = 'c:/Users/Documents/mynode.json'
http_remote_uri = 'http://example.com/mynode.json'
s3_remote_uri = 's3://my-bucket/folder/subfolder/mynode.json'

def test_node_uri():
    node = Node(uuid=uuid4(), node_uri=local_uri)
    assert node.node_uri == AnyUrl(local_uri)

    node = Node(uuid=uuid4(), node_uri=http_remote_uri)
    assert node.node_uri == AnyUrl(http_remote_uri)

    node = Node(uuid=uuid4(), node_uri=s3_remote_uri)
    assert node.node_uri == AnyUrl(s3_remote_uri)
    return

def test_node_with_no_uri():
    with pytest.raises(ValidationError):
        Node(uuid=uuid4())

def test_node_with_invalid_uri():
    wrong_uri = 's3sdf/fd/mynode.json'
    with pytest.raises(ValidationError):
        Node(uuid=uuid4(), node_uri=wrong_uri)

def test_node_without_uuid4():
    with pytest.raises(ValidationError):
        Node(node_uri=local_uri)

def test_node_with_invalid_uuid4():
    with pytest.raises(ValidationError):
        Node(uuid=uuid1(), node_uri=local_uri)

