"""Generate local-only development secrets without printing them."""

import secrets
from pathlib import Path

target = Path(".env")
if not target.exists():
    target.write_text(
        "API_KEY="
        + secrets.token_hex(24)
        + "\nPOSTGRES_PASSWORD="
        + secrets.token_hex(24)
        + "\nKAFKA_PARTITIONS=1\nWORKER_CONCURRENCY=4\nRATE_LIMIT=10000\n"
    )
    target.chmod(0o600)
    print("Created .env with random local development credentials.")
else:
    print("Existing .env preserved.")
