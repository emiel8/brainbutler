from pydantic import ValidationError
import pytest

from backend.components.tag import Tag

def test_tag_name_lowercase():
    tag = Tag(name='EXAMPLE')
    assert tag.name == 'example'

def test_tag_name_already_lowercase():
    tag = Tag(name='example')
    assert tag.name == 'example'

def test_tag_name_mixed_case():
    tag = Tag(name='ExAmPlE')
    assert tag.name == 'example'

def test_tag_empty_name():
    with pytest.raises(ValidationError):
        Tag(name='')

def test_tag_non_string_name():
    with pytest.raises(ValidationError):
        Tag(name=123)