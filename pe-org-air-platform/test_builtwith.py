import httpx
import time

# BuiltWith Free API Key
KEY = "a8cb116e-14df-498c-89df-e3e8e8157bc7"

# Companies to test
DOMAINS = [
    "jpmorganchase.com",
    "jpmorgan.com",
    "adp.com",
    "deere.com",
    "target.com",
    "paychex.com",
    "unitedhealthgroup.com",
    "hcahealthcare.com"
]


def fetch_builtwith(domain: str):
    """Fetch technology stack information from BuiltWith"""
    try:
        response = httpx.get(
            "https://api.builtwith.com/free1/api.json",
            params={"KEY": KEY, "LOOKUP": domain},
            timeout=20
        )

        if response.status_code != 200:
            print(f"{domain}: API error {response.status_code}")
            return

        data = response.json()
        groups = data.get("groups", [])

        total_live = sum(g.get("live", 0) for g in groups)
        total_dead = sum(g.get("dead", 0) for g in groups)

        print(f"\n===== {domain} =====")
        print(f"Groups: {len(groups)}")
        print(f"Live technologies: {total_live}")
        print(f"Dead technologies: {total_dead}")

        # Show first few technology categories
        for g in groups[:5]:
            print(f"  - {g.get('name')} (live={g.get('live',0)}, dead={g.get('dead',0)})")

    except Exception as e:
        print(f"{domain}: failed -> {e}")


def main():
    for domain in DOMAINS:
        fetch_builtwith(domain)
        time.sleep(1.2)  # prevent rate limiting


if __name__ == "__main__":
    main()