import json
import os
import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = "data/raw"
ROWS_TARGET = 10_000_000
BATCH_SIZE = 50_000
NUM_USERS = 10_000
NUM_MERCHANTS = 500
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 6, 30)
DATE_RANGE = (END_DATE - START_DATE).days
CURRENCIES = ["USD", "EUR", "CLP", "BRL", "MXN"]
CURRENCY_WEIGHTS = [0.5, 0.2, 0.15, 0.1, 0.05]


users = [str(uuid.uuid4()) for _ in range(NUM_USERS)]
merchants = [fake.company() for _ in range(NUM_MERCHANTS)]
cards = [fake.credit_card_number(card_type="visa") for _ in range(2000)]

AMOUNT_DIST = [round(random.lognormvariate(3, 1.2), 2) for _ in range(10000)]


def generate_batch(batch_id, size):
    rows = []
    for i in range(size):
        row_id = batch_id * BATCH_SIZE + i
        user = random.choice(users)
        card = random.choice(cards)
        amount = random.choice(AMOUNT_DIST)
        if row_id % 50 == 0:
            amount = random.choice([-abs(amount), 0.0, 0])
        tx_date = START_DATE + timedelta(days=random.randint(0, DATE_RANGE))
        rows.append({
            "transaction_id": f"tx-{row_id:08d}",
            "user_id": user,
            "card": card,
            "amount": amount,
            "transaction_date": tx_date.strftime("%Y-%m-%d"),
            "timestamp": tx_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "merchant": random.choice(merchants),
            "currency": random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0],
        })
    return rows


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    num_batches = (ROWS_TARGET + BATCH_SIZE - 1) // BATCH_SIZE
    written = 0

    for batch_id in range(num_batches):
        size = min(BATCH_SIZE, ROWS_TARGET - written)
        rows = generate_batch(batch_id, size)
        fname = os.path.join(OUTPUT_DIR, f"transactions_{batch_id:04d}.json")
        with open(fname, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        written += len(rows)
        print(f"  batch {batch_id + 1}/{num_batches} — {len(rows)} rows → {fname}  ({written:,} / {ROWS_TARGET:,})")

    print(f"\nDone. {written:,} rows written to {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
