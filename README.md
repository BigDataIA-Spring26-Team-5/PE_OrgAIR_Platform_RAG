# 🏢 PE Org-AI-R Platform : CS4

## Setting up Docker

1. Run 
```bash
docker compose build --no-cache
```

2. Make sure to clear all Docker cache
```bash
docker compose up
```
## Error Codes

**/HEALTH Error code design:**

| Code | Dep | Rationale |
|---|---|---|
| `200` | all healthy | standard |
| `207` | mixed | partial degradation  |
| `502` | S3 | "bad gateway" — upstream storage unreachable |
| `503` | Snowflake | "service unavailable" — primary DB down |
| `504` | Redis | "gateway timeout" — cache layer timeout |