"""Generate realistic policy_sold events using Faker."""

import random
from datetime import datetime, timezone
from uuid import uuid4



from schema import PolicySoldEvent



# --- Reference data (must match dbt seed CSVs) ---

AGENTS = [f"A-{str(i).zfill(3)}" for i in range(1, 21)]  # 20 agents

# product_id → product_type (used for premium/sum_assured ranges)
PRODUCTS = {
    "P-001": "term_life",
    "P-002": "term_life",
    "P-003": "whole_life",
    "P-004": "endowment",
    "P-005": "endowment",
    "P-006": "investment_linked",
    "P-007": "investment_linked",
    "P-008": "health",
    "P-009": "health",
    "P-010": "accident",
}

REGION_IDS = ["R-001", "R-002", "R-003", "R-004"]

CHANNELS = ["agent", "online", "bancassurance", "broker"]
CHANNEL_WEIGHTS = [0.50, 0.25, 0.15, 0.10]

PAYMENT_FREQUENCIES = ["monthly", "quarterly", "semi_annual", "annual"]
PAYMENT_FREQ_WEIGHTS = [0.45, 0.20, 0.15, 0.20]

# Annual premium ranges by product type (TWD)
PREMIUM_RANGES = {
    "term_life": (6_000, 30_000),
    "whole_life": (30_000, 120_000),
    "endowment": (24_000, 80_000),
    "investment_linked": (36_000, 200_000),
    "health": (8_000, 40_000),
    "accident": (3_000, 15_000),
}

# Sum assured ranges by product type (TWD)
SUM_ASSURED_RANGES = {
    "term_life": (3_000_000, 10_000_000),
    "whole_life": (1_000_000, 5_000_000),
    "endowment": (1_000_000, 3_000_000),
    "investment_linked": (1_500_000, 5_000_000),
    "health": (500_000, 3_000_000),
    "accident": (1_000_000, 5_000_000),
}


def generate_event() -> PolicySoldEvent:
    """Generate a single randomized policy_sold event."""
    product_id = random.choice(list(PRODUCTS.keys()))
    product_type = PRODUCTS[product_id]

    premium_lo, premium_hi = PREMIUM_RANGES[product_type]
    premium = random.randint(premium_lo, premium_hi)

    sa_lo, sa_hi = SUM_ASSURED_RANGES[product_type]
    sum_assured = random.randint(sa_lo, sa_hi)

    return PolicySoldEvent(
        event_id=uuid4(),
        event_ts=datetime.now(timezone.utc),
        policy_id=uuid4(),
        agent_id=random.choice(AGENTS),
        product_id=product_id,
        region_id=random.choice(REGION_IDS),
        channel=random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
        premium_amount=premium,
        sum_assured=sum_assured,
        payment_frequency=random.choices(
            PAYMENT_FREQUENCIES, weights=PAYMENT_FREQ_WEIGHTS, k=1
        )[0],
    )


if __name__ == "__main__":
    event = generate_event()
    print(event.model_dump_json(indent=2))