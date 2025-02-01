from dataclasses import dataclass, field, asdict
from typing import Literal, Optional

SEPARATOR = "."

IndicatorType = Literal["DAC", "ONE"]


@dataclass
class Indicator:
    code: str
    name: str
    description: str = field(default=str)
    sources: list[str] = field(default_factory=list)
    type: IndicatorType = field(default="DAC")
    filters: dict = field(default_factory=dict)
    custom_function: Optional[str] = field(default_factory=str)

    @property
    def to_dict(self) -> dict:
        return asdict(self)
