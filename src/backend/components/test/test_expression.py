import pytest
from pydantic import ValidationError, AnyUrl
from uuid import uuid4, uuid1

from backend.components.expression import Expression

local_uri = 'c:/Users/Documents/myexpression.json'
http_remote_uri = 'http://example.com/myexpression.json'
s3_remote_uri = 's3://my-bucket/folder/subfolder/myexpression.json'

def test_expression_uri():
    expression = Expression(uuid=uuid4(), expression_uri=local_uri)
    assert expression.expression_uri == AnyUrl(local_uri)

    expression = Expression(uuid=uuid4(), expression_uri=http_remote_uri)
    assert expression.expression_uri == AnyUrl(http_remote_uri)

    expression = Expression(uuid=uuid4(), expression_uri=s3_remote_uri)
    assert expression.expression_uri == AnyUrl(s3_remote_uri)
    return

def test_expression_with_no_uri():
    with pytest.raises(ValidationError):
        Expression(uuid=uuid4())

def test_expression_with_invalid_uri():
    wrong_uri = 's3sdf/fd/myexpression.json'
    with pytest.raises(ValidationError):
        Expression(uuid=uuid4(), expression_uri=wrong_uri)

def test_expression_without_uuid4():
    with pytest.raises(ValidationError):
        Expression(expression_uri=local_uri)

def test_expression_with_invalid_uuid4():
    with pytest.raises(ValidationError):
        Expression(uuid=uuid1(), expression_uri=local_uri)

