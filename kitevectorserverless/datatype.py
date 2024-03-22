from dataclasses import dataclass, field, asdict
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class SchemaField:
    name: str
    type: str
    is_primary: bool=False
    is_anns: bool=False

@dataclass_json
@dataclass
class Schema:
    fields: list[SchemaField]

    def get_primary(self):
        for f in self.fields:
            if f.is_primary:
                return f
        return None
    
    def get_vector(self):
        for f in self.fields:
            if f.is_anns or f.type == 'vector':
                return f
        return None
    
@dataclass_json
@dataclass
class Params:
    max_elements: int
    ef_construction: int
    M: int

@dataclass_json
@dataclass
class IndexConfig:
    name: str
    dimension: int
    metric_type: str
    schema: Schema
    params: Params

