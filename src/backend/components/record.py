import copy
import typing as t

from pydantic import BaseModel, model_validator, field_validator, UUID4, AnyUrl

from backend.components.tag import Tag


class Record(BaseModel):

    uuid: UUID4
    text_uri: t.Optional[AnyUrl] = None
    image_uri: t.Optional[AnyUrl] = None
    sound_uri: t.Optional[AnyUrl] = None
    reference: t.Optional[str] = ''

    
    @field_validator('text_uri', 'image_uri', 'sound_uri', mode='before')
    @classmethod
    def empty_uri(cls, v: str) -> t.Union[str, None]:
        if v == '':
            return None
        return v

    @model_validator(mode='after')
    def check_at_least_one_uri_given(self) -> t.Self:
        if (self.text_uri is None and self.image_uri is None and self.sound_uri is None):
            raise ValueError('At least one URI should be specified')
        return self
    
    def __hash__(self):
        return hash(self.uuid)

    def __eq__(self, other):
        if not isinstance(other, Record):
            return False
        return self.uuid == other.uuid

