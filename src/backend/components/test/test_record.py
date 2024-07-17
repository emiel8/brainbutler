import pytest
from pydantic import ValidationError, AnyUrl
from uuid import uuid4, uuid1

from backend.components.record import Record


local_text_uri = 'c:/Users/Documents/myfile.txt'
http_remote_text_uri = 'http://example.com/myfile.txt'
s3_remote_text_uri = 's3://my-bucket/folder/subfolder/myfile.txt'


local_sound_uri = 'c:/Users/Documents/sound.mp3'
http_remote_sound_uri = 'http://example.com/sound.mp3'
s3_remote_sound_uri = 's3://my-bucket/folder/subfolder/sound.mp3'

local_image_uri = 'c:/Users/Documents/image.jpeg'
http_remote_image_uri = 'http://example.com/image.jpeg'
s3_remote_image_uri = 's3://my-bucket/folder/subfolder/image.jpeg'

# Test cases
def test_record_with_text_uri():
    record = Record(uuid=uuid4(), text_uri=local_text_uri)
    assert record.text_uri == AnyUrl(local_text_uri)

    record = Record(uuid=uuid4(), text_uri=http_remote_text_uri)
    assert record.text_uri == AnyUrl(http_remote_text_uri)

    record = Record(uuid=uuid4(), text_uri=s3_remote_text_uri)
    assert record.text_uri == AnyUrl(s3_remote_text_uri)    

def test_record_with_image_uri():
    record = Record(uuid=uuid4(), image_uri=local_image_uri)
    assert record.image_uri == AnyUrl(local_image_uri)

    record = Record(uuid=uuid4(), image_uri=http_remote_image_uri)
    assert record.image_uri == AnyUrl(http_remote_image_uri)

    record = Record(uuid=uuid4(), image_uri=s3_remote_image_uri)
    assert record.image_uri == AnyUrl(s3_remote_image_uri)

def test_record_with_sound_uri():
    record = Record(uuid=uuid4(), sound_uri=local_sound_uri)
    assert record.sound_uri == AnyUrl(local_sound_uri)

    record = Record(uuid=uuid4(), sound_uri=http_remote_sound_uri)
    assert record.sound_uri == AnyUrl(http_remote_sound_uri)

    record = Record(uuid=uuid4(), sound_uri=s3_remote_sound_uri)
    assert record.sound_uri == AnyUrl(s3_remote_sound_uri)

def test_record_with_multiple_uris():
    record = Record(
        uuid=uuid4(),
        text_uri=local_text_uri,
        image_uri=http_remote_image_uri,
        sound_uri=s3_remote_sound_uri,
    )
    assert record.text_uri == AnyUrl(local_text_uri)
    assert record.image_uri == AnyUrl(http_remote_image_uri)
    assert record.sound_uri == AnyUrl(s3_remote_sound_uri)
    assert record.reference == ''

def test_record_with_no_uris():
    with pytest.raises(ValidationError):
        Record(uuid=uuid4())

def test_record_with_invalid_uri():
    email = 'myname.myothername@provider.com'
    with pytest.raises(ValidationError):
        Record(uuid=uuid4(), text_uri=email)

def test_record_with_no_uuid4():
    with pytest.raises(ValidationError):
        Record(text_uri=local_text_uri)

def test_record_with_invalid_uuid4():
    with pytest.raises(ValidationError):
        Record(uuid=uuid1(), text_uri=local_text_uri)

def test_record_with_reference():
    record = Record(
        uuid=uuid4(),
        text_uri=local_text_uri,
        image_uri=http_remote_image_uri,
        sound_uri=s3_remote_sound_uri,
        reference='myreference'
    )
    assert record.text_uri == AnyUrl(local_text_uri)
    assert record.image_uri == AnyUrl(http_remote_image_uri)
    assert record.sound_uri == AnyUrl(s3_remote_sound_uri)
    assert record.reference == 'myreference'
