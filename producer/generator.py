"""Generate realistic policy_sold events using Faker."""
 
import random
from datetime import datetime, timezone
from uuid import uuid4
 
from faker import Faker
 
from schema import PolicySoldEvent
 
fake = Faker()

# 20 agents
AGENTS = [f"A-{str(i).zfill(3)}" for i in range(1, 21)]

# 10 products
PRODUCTS = [
    ("LIFE-TERM-10", "term_life"),
    ("LIFE-TERM-20", "term_life"),
    ("LIFE-WHOLE-99", "whole_life"),
    ("LIFE-ENDOW-15", "endowment"),
    ("LIFE-ENDOW-20", "endowment"),
    ("LIFE-ILP-GROWTH", "investment_linked"),
    ("LIFE-ILP-BALANCED", "investment_linked"),
    ("HEALTH-BASIC", "health"),
    ("HEALTH-PREMIUM", "health"),
    ("ACC-PERSONAL", "accident"),
]

# 4 regions 
REGIONS = ["Northern", "Central", "Southern", "Eastern"]

# 4 channels 
CHANNELS = ["agent", "online", "bancassurance", "broker"]
CHANNEL_WEIGHTS = [0.50, 0.25, 0.15, 0.10]

# 6 age bands
AGE_BANDS = ["18-24", "25-29", "30-39", "40-49", "50-59", "60+"]
AGE_BAND_WEIGHTS = [0.05, 0.15, 0.25, 0.25, 0.20, 0.10]

# 4 payment frequencies
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
    product_code, product_type = random.choice(PRODUCTS)
 
    premium_lo, premium_hi = PREMIUM_RANGES[product_type]
    premium = random.randint(premium_lo, premium_hi)
 
    sa_lo, sa_hi = SUM_ASSURED_RANGES[product_type]
    sum_assured = random.randint(sa_lo, sa_hi)
 
    return PolicySoldEvent(
        event_id=uuid4(),
        event_type="policy_sold",
        event_ts=datetime.now(timezone.utc),
        policy_id=uuid4(),
        agent_id=random.choice(AGENTS),
        product_code=product_code,
        product_type=product_type,
        region=random.choice(REGIONS),
        channel=random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
        premium_amount=premium,
        sum_assured=sum_assured,
        customer_age_band=random.choices(AGE_BANDS, weights=AGE_BAND_WEIGHTS, k=1)[0],
        payment_frequency=random.choices(
            PAYMENT_FREQUENCIES, weights=PAYMENT_FREQ_WEIGHTS, k=1
        )[0],
    )
 
 
if __name__ == "__main__":
    # test: print a sample event as JSON
    event = generate_event()
    print(event.model_dump_json(indent=2))
