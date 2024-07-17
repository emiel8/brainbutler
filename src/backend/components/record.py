import copy
import typing as t

from pydantic import BaseModel, model_validator, UUID4, AnyUrl

from backend.components.tag import Tag


class Record(BaseModel):

    uuid: UUID4
    text_uri: t.Optional[AnyUrl] = None
    image_uri: t.Optional[AnyUrl] = None
    sound_uri: t.Optional[AnyUrl] = None
    reference: t.Optional[str] = ''

    @model_validator(mode='after')
    def check_at_least_one_uri_given(self) -> t.Self:
        if (self.text_uri is None and self.image_uri is None and self.sound_uri is None):
            raise ValueError('At least one URI should be specified')
        return self

