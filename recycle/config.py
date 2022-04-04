from dataclasses import dataclass
from dataclasses import field
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from dateutil.relativedelta import relativedelta


@dataclass
class Transform:
    keep: Optional[relativedelta]
    drop: Optional[bool]
    anonymize: Dict[str, Union[str, List[str]]] = field(default_factory=dict)


Transforms = Dict[str, Dict[str, Transform]]


@dataclass
class Ops:
    transforms: Transforms
