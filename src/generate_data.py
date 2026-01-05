from faker import Faker
import pandas as pd

fake = Faker()

def generate_customers(n: int = 50) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "customer_id": range(1, n + 1),
            "customer_name": [fake.company() for _ in range(n)],
            "country": [fake.country_code() for _ in range(n)],
        }
    )

if __name__ == "__main__":
    print(generate_customers(5))
