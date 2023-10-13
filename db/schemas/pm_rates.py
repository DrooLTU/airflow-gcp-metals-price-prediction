from pydantic import BaseModel, Field
from datetime import datetime

class PMratesBase(BaseModel):
    XAUUSD: float = Field(..., example=1854.58)


class PMratesCreate(PMratesBase):
    pass


class PMrates(PMratesBase):
    id: int
    created_at: datetime
    class Config:
        from_attributes = True