from dataclasses import dataclass, field, asdict
from typing import Literal, Optional

SEPARATOR = "."


@dataclass
class Indicator:
    code: str
    name: str
    description: str = field(default=str)
    sources: list[str] = field(default_factory=list)
    type: Literal["DAC", "ONE"] = field(default="DAC")
    filters: dict = field(default_factory=dict)
    operations: list = field(default_factory=list)
    custom_functions: Optional[list] = field(default_factory=list)

    @property
    def to_dict(self) -> dict:
        return asdict(self)
