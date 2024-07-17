from pydantic import BaseModel, field_validator

class Tag(BaseModel):
    """
    """

    name: str

    @field_validator('name')
    @classmethod
    def set_name_to_lowercase(cls, v: str) -> str:
        return v.lower()

    @field_validator('name')
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        assert v != ''
        return v