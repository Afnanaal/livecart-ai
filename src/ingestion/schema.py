from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class ProductEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    product_name: str = Field(min_length=1)
    price: float = Field(gt=0)
    quantity: int = Field(ge=0)
    region: str = Field(min_length=1)
    event_time: datetime