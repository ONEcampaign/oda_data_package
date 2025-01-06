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
    custom_function: Optional[str] = field(default_factory=str)
    # group_excluding: Optional[list[str]] = field(default_factory=list)

    @property
    def to_dict(self) -> dict:
        return asdict(self)
