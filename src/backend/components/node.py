import typing as t

from pydantic import BaseModel, UUID4, AnyUrl, field_validator

class Node(BaseModel):

    uuid: UUID4
    node_uri: AnyUrl

    @field_validator('node_uri')
    @classmethod
    def check_is_json(cls, v: AnyUrl) -> AnyUrl:
        if not str(v).endswith('.json'):
            raise ValueError('The node URI should point to a json file!')
        return v
    
    @staticmethod
    def read_node_json(fpath: str) -> t.Dict:
        raise NotImplementedError
    
    @staticmethod
    def parse_node_formula(formula: str) -> t.Dict:
        # Maybe the frontend should just provide the node formula in the json format ?
        raise NotImplementedError
    
    def __hash__(self):
        return hash(self.uuid)

if __name__ == '__main__':
    # Examples of how the node formula jsons will look

    # Allowed operands: AND, OR, NOT

    # In the examples below, capital letters will be replaced by record IDs

    # A AND B
    ex1 = {'type': 'and', 'ops': ['A', 'B']}

    # NOT C
    ex2 = {'type': 'not', 'op': 'C'}

    # A OR D OR E
    ex3 = {'type': 'or', 'ops': ['A', 'D', 'E']}

    # NOT (A OR D OR E)
    ex4 = {'type': 'not', 'op': ex3}

    # (A AND B) AND (NOT (A OR D OR E))
    ex5 = {'type': 'and', 'ops': [ex1, ex4]}

    import os
    import json
    example_dir = 'src/backend/components/example_jsons/nodes'
    for idx, ex in enumerate([ex1, ex2, ex3, ex4, ex5]):
        fpath_ex = os.path.join(example_dir, f'ex{idx}.json')
        with open(fpath_ex, 'w') as f:
            json.dump(ex, fp=f, indent=4)