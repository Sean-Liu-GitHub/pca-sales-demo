"""Pydantic schema for the policy_sold event."""

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


class ProductType(str, Enum):
    TERM_LIFE = "term_life"
    WHOLE_LIFE = "whole_life"
    ENDOWMENT = "endowment"
    INVESTMENT_LINKED = "investment_linked"
    HEALTH = "health"
    ACCIDENT = "accident"


class Channel(str, Enum):
    AGENT = "agent"
    ONLINE = "online"
    BANCASSURANCE = "bancassurance"
    BROKER = "broker"


class PaymentFrequency(str, Enum):
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    SEMI_ANNUAL = "semi_annual"
    ANNUAL = "annual"


class PolicySoldEvent(BaseModel):
    """Schema for a single policy_sold event published to Kafka."""

    event_id: UUID
    event_type: str = Field(default="policy_sold", frozen=True)
    event_ts: datetime
    policy_id: UUID
    agent_id: str
    product_code: str
    product_type: ProductType
    region: str
    channel: Channel
    premium_amount: int = Field(gt=0)
    sum_assured: int = Field(gt=0)
    customer_age_band: str
    payment_frequency: PaymentFrequency
