"""
Confirm which storage backend VectorStore is actually using.
Run: python check_backend.py
"""
import os, sys
from dotenv import load_dotenv

sys.path.insert(0, ".")
load_dotenv()

# Check env vars as Python sees them
print("=== ENV VARS ===")
api_key = os.getenv("CHROMA_API_KEY", "")
tenant  = os.getenv("CHROMA_TENANT", "")
db      = os.getenv("CHROMA_DATABASE", "pe_org-air-platform")
print(f"CHROMA_API_KEY  : {'SET (len=' + str(len(api_key)) + ')' if api_key else 'NOT SET'}")
print(f"CHROMA_TENANT   : {'SET = ' + tenant if tenant else 'NOT SET'}")
print(f"CHROMA_DATABASE : {db}")
print(f"_use_cloud would be: {bool(api_key and tenant)}")

print()
print("=== VECTOR STORE BACKEND ===")
from app.services.search.vector_store import VectorStore
vs = VectorStore()
print(f"_use_cloud      : {vs._use_cloud}")
print(f"_collection_id  : {vs._collection_id}")
print(f"_local_collection: {vs._local_collection}")
print(f"_encoder        : {vs._encoder is not None}")
print(f"count()         : {vs.count()}")

print()
print("=== .env FILE CHECK ===")
env_path = ".env"
if os.path.exists(env_path):
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("CHROMA"):
                # Show key name but mask value
                if "=" in line:
                    k, v = line.split("=", 1)
                    masked = v[:4] + "..." + v[-4:] if len(v) > 8 else "***"
                    print(f"  {k} = {masked}")
                else:
                    print(f"  {line}")
else:
    print("  .env file not found")
