"""
Health Check Router - PE Org-AI-R Platform
app/routers/health.py

- /healthz: lightweight health check for platform (always 200) -> use for Render
- /health: deep dependency checks (Snowflake, Redis, S3) -> returns 200 or 503
"""

from __future__ import annotations

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, Optional, List
from datetime import datetime, timezone
import os
import time
from pathlib import Path

from dotenv import load_dotenv

# Load .env only in local dev (Render will use Environment Variables)
project_root = Path(__file__).resolve().parent.parent.parent
env_path = project_root / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path)

router = APIRouter(tags=["Health"])


# -------------------------
# Schemas
# -------------------------

class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    dependencies: Dict[str, str]


class CacheStatsResponse(BaseModel):
    redis_connected: bool
    redis_host: Optional[str] = None
    redis_port: Optional[int] = None
    keys_count: Optional[int] = None
    memory_used: Optional[str] = None
    uptime_seconds: Optional[int] = None
    error: Optional[str] = None


class CacheTestResponse(BaseModel):
    write_success: bool
    read_success: bool
    delete_success: bool
    value_match: bool
    latency_ms: Optional[float] = None
    error: Optional[str] = None


class CacheKeysResponse(BaseModel):
    pattern: str
    count: int
    keys: List[str]
    error: Optional[str] = None


# -------------------------
# Dependency Health Checks
# -------------------------

async def check_snowflake() -> str:
    """Check Snowflake connection health."""
    try:
        import snowflake.connector

        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        role = os.getenv("SNOWFLAKE_ROLE")

        missing = [k for k, v in {
            "SNOWFLAKE_ACCOUNT": account,
            "SNOWFLAKE_USER": user,
            "SNOWFLAKE_PASSWORD": password,
        }.items() if not v]

        if missing:
            return f"unhealthy: Missing env vars: {', '.join(missing)}"

        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
        )

        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        return f"healthy (User: {result[0]}, Role: {result[1]})"

    except ImportError:
        return "unhealthy: snowflake-connector-python not installed"
    except Exception as e:
        msg = str(e)
        msg = (msg[:160] + "...") if len(msg) > 160 else msg
        return f"unhealthy: {msg}"


async def check_redis() -> str:
    """Check Redis connection health."""
    try:
        import redis

        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

        client = redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        client.ping()
        client.close()

        return "healthy"

    except ImportError:
        return "unhealthy: redis package not installed"
    except Exception as e:
        msg = str(e)
        msg = (msg[:160] + "...") if len(msg) > 160 else msg
        return f"unhealthy: {msg}"


async def check_s3() -> str:
    """Check AWS S3 connection health."""
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError

        bucket = os.getenv("S3_BUCKET", "pe-orgair-platform")
        region = os.getenv("AWS_REGION")

        if not region:
            return "unhealthy: Missing AWS_REGION"

        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not access_key or not secret_key:
            return "unhealthy: Missing AWS credentials"

        s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

        s3_client.head_bucket(Bucket=bucket)
        return f"healthy (Bucket: {bucket}, Region: {region})"

    except NoCredentialsError:
        return "unhealthy: AWS credentials not configured"
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        return f"unhealthy: AWS error - {code}"
    except Exception as e:
        msg = str(e)
        msg = (msg[:160] + "...") if len(msg) > 160 else msg
        return f"unhealthy: {msg}"


# -------------------------
# Routes
# -------------------------

@router.get("/healthz", summary="Lightweight health check (Render)")
def healthz():
    """Always returns 200. Use this for Render health check."""
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@router.get(
    "/health",
    response_model=HealthResponse,
    responses={
        200: {"description": "All dependencies healthy"},
        503: {"description": "One or more dependencies unhealthy"},
    },
    summary="Deep health check",
    description="Checks Snowflake, Redis, and S3 connectivity.",
)
async def health_check():
    dependencies = {
        "snowflake": await check_snowflake(),
        "redis": await check_redis(),
        "s3": await check_s3(),
    }

    all_healthy = all(v.startswith("healthy") for v in dependencies.values())

    response = HealthResponse(
        status="healthy" if all_healthy else "degraded",
        timestamp=datetime.now(timezone.utc),
        version="1.0.0",
        dependencies=dependencies,
    )

    if all_healthy:
        return response

    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=response.model_dump(mode="json"),
    )


@router.get("/health/snowflake", summary="Check Snowflake connection")
async def health_snowflake():
    result = await check_snowflake()
    return {
        "service": "snowflake",
        "status": result,
        "is_healthy": result.startswith("healthy"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/health/redis", summary="Check Redis connection")
async def health_redis():
    result = await check_redis()
    return {
        "service": "redis",
        "status": result,
        "is_healthy": result.startswith("healthy"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/health/s3", summary="Check S3 connection")
async def health_s3():
    result = await check_s3()
    return {
        "service": "s3",
        "status": result,
        "is_healthy": result.startswith("healthy"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# -------------------------
# Redis Cache Testing Endpoints
# -------------------------

@router.get(
    "/health/cache/stats",
    response_model=CacheStatsResponse,
    summary="Redis cache statistics",
    description="Returns detailed Redis cache statistics and connection info.",
)
async def cache_stats() -> CacheStatsResponse:
    try:
        from app.services.cache import get_cache

        cache = get_cache()
        if not cache:
            return CacheStatsResponse(redis_connected=False, error="Redis not configured or unreachable")

        cache.client.ping()
        info = cache.client.info()
        db_info = cache.client.info("keyspace")

        keys_count = db_info.get("db0", {}).get("keys", 0) if "db0" in db_info else 0

        return CacheStatsResponse(
            redis_connected=True,
            redis_host=cache.client.connection_pool.connection_kwargs.get("host"),
            redis_port=cache.client.connection_pool.connection_kwargs.get("port"),
            keys_count=keys_count,
            memory_used=info.get("used_memory_human"),
            uptime_seconds=info.get("uptime_in_seconds"),
        )
    except Exception as e:
        return CacheStatsResponse(redis_connected=False, error=str(e))


@router.get(
    "/health/cache/test",
    response_model=CacheTestResponse,
    summary="Test Redis cache operations",
    description="Performs write/read/delete test to verify caching works.",
)
async def cache_test() -> CacheTestResponse:
    try:
        from app.services.cache import get_cache
        from pydantic import BaseModel as _BaseModel

        class TestModel(_BaseModel):
            test_value: str

        cache = get_cache()
        if not cache:
            return CacheTestResponse(
                write_success=False,
                read_success=False,
                delete_success=False,
                value_match=False,
                error="Redis not available",
            )

        test_key = "health:cache:test"
        test_value = TestModel(test_value="redis_test_123")

        start_time = time.time()

        cache.set(test_key, test_value, ttl_seconds=60)
        write_success = True

        cached = cache.get(test_key, TestModel)
        read_success = cached is not None
        value_match = cached.test_value == test_value.test_value if cached else False

        cache.delete(test_key)
        deleted = cache.get(test_key, TestModel)
        delete_success = deleted is None

        latency_ms = (time.time() - start_time) * 1000

        return CacheTestResponse(
            write_success=write_success,
            read_success=read_success,
            delete_success=delete_success,
            value_match=value_match,
            latency_ms=round(latency_ms, 2),
        )
    except Exception as e:
        return CacheTestResponse(
            write_success=False,
            read_success=False,
            delete_success=False,
            value_match=False,
            error=str(e),
        )


@router.delete(
    "/health/cache/flush",
    summary="Flush all cache",
    description="Clears all cached data. Use with caution!",
)
async def cache_flush() -> dict:
    try:
        from app.services.cache import get_cache

        cache = get_cache()
        if not cache:
            return {"success": False, "error": "Redis not available"}

        cache.client.flushdb()
        return {"success": True, "message": "Cache flushed successfully"}
    except Exception as e:
        return {"success": False, "error": str(e)}


# -------------------------
# Environment Check
# -------------------------

@router.get("/health/env-check", summary="Check environment variables")
async def health_env_check():
    """Check if environment variables are loaded (doesn't expose values)."""
    return {
        "env_file_path": str(env_path),
        "env_file_exists": env_path.exists(),
        "variables": {
            "SNOWFLAKE_ACCOUNT": "✅ Set" if os.getenv("SNOWFLAKE_ACCOUNT") else "❌ Missing",
            "SNOWFLAKE_USER": "✅ Set" if os.getenv("SNOWFLAKE_USER") else "❌ Missing",
            "SNOWFLAKE_PASSWORD": "✅ Set" if os.getenv("SNOWFLAKE_PASSWORD") else "❌ Missing",
            "SNOWFLAKE_WAREHOUSE": "✅ Set" if os.getenv("SNOWFLAKE_WAREHOUSE") else "❌ Missing",
            "SNOWFLAKE_DATABASE": "✅ Set" if os.getenv("SNOWFLAKE_DATABASE") else "❌ Missing",
            "SNOWFLAKE_SCHEMA": "✅ Set" if os.getenv("SNOWFLAKE_SCHEMA") else "❌ Missing",
            "SNOWFLAKE_ROLE": "✅ Set" if os.getenv("SNOWFLAKE_ROLE") else "⚪ Optional",
            "AWS_ACCESS_KEY_ID": "✅ Set" if os.getenv("AWS_ACCESS_KEY_ID") else "❌ Missing",
            "AWS_SECRET_ACCESS_KEY": "✅ Set" if os.getenv("AWS_SECRET_ACCESS_KEY") else "❌ Missing",
            "AWS_REGION": "✅ Set" if os.getenv("AWS_REGION") else "❌ Missing",
            "S3_BUCKET": "✅ Set" if os.getenv("S3_BUCKET") else "⚪ Using default",
            "REDIS_URL": "✅ Set" if os.getenv("REDIS_URL") else "⚪ Using default (redis://localhost:6379/0)",
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
