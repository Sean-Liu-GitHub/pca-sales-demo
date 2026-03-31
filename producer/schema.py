"""Pydantic schema for the policy_sold event."""

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


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
    event_ts: datetime
    policy_id: UUID
    agent_id: str
    product_id: str
    region_id: str
    channel: Channel
    premium_amount: int = Field(gt=0)
    sum_assured: int = Field(gt=0)
    payment_frequency: PaymentFrequency