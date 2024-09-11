from pydantic import BaseModel, UUID4, AnyUrl, field_validator

class Expression(BaseModel):

    uuid: UUID4
    expression_uri: AnyUrl

    @field_validator('expression_uri')
    @classmethod
    def check_is_json(cls, v: AnyUrl) -> AnyUrl:
        if not str(v).endswith('.json'):
            raise ValueError('The expression URI should point to a json file!')
        return v
    
    def __hash__(self):
        return hash(self.uuid)

if __name__ == '__main__':
    # Examples of how the expressions formulas json will look

    # Allowed operands: IMPLIES, EQUIVALENT

    # In the examples below, capital letters will be replaced by node IDs

    # A IMPLIES B
    ex1 = {'type': 'implies', 'antecedent': 'A', 'consequent': 'B'}
    
    # A EQUIVALENT B
    ex2 = {'type': 'equivalent', 'equivalents': ['A', 'B']}