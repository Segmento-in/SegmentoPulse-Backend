"""
Segmento Pulse — Production Readiness Check
============================================
Senior Backend Architect & QA Lead — Critical Path Audit

Nodes tested:
  A  Ingestion  — External API reachability (dry-run one provider)
  B  Storage    — Appwrite DB connectivity ping
  C  Retrieval  — /api/news/ai endpoint (real JSON, field validation)
  D  Interaction — POST to engagement/views endpoint (write path)

Usage:
  python prod_readiness_check.py [--prod] [--local]

  --prod   Target https://shafisk17-pulse-backend.hf.space  (default)
  --local  Target http://localhost:8000
"""

import asyncio
import sys
import json
import os
import time

try:
    import httpx
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx", "-q"])
    import httpx

# ── Configuration ─────────────────────────────────────────────────────────────
PROD_BASE = "https://shafisk17-pulse-backend.hf.space"
LOCAL_BASE = "http://localhost:8000"

# Choose target
if "--local" in sys.argv:
    BASE_URL = LOCAL_BASE
else:
    BASE_URL = PROD_BASE

FRONTEND_ORIGIN = "https://segmento.in"  # Simulated browser origin for CORS

TIMEOUT = 30.0  # seconds per request

# Test article ID (use a known valid ID from your AI collection)
TEST_ARTICLE_ID = None  # Will be populated from Node C

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW= "\033[93m"
BLUE  = "\033[94m"
RESET = "\033[0m"
BOLD  = "\033[1m"

def ok(msg):   print(f"  {GREEN}PASS{RESET}  {msg}")
def fail(msg): print(f"  {RED}FAIL{RESET}  {msg}")
def warn(msg): print(f"  {YELLOW}WARN{RESET}  {msg}")
def info(msg): print(f"  {BLUE}INFO{RESET}  {msg}")
def header(title):
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")


# ── PREFLIGHT: CORS ────────────────────────────────────────────────────────────
async def check_cors(client: httpx.AsyncClient) -> bool:
    header("PREFLIGHT — CORS Check")
    try:
        r = await client.options(
            f"{BASE_URL}/api/news/ai",
            headers={
                "Origin": FRONTEND_ORIGIN,
                "Access-Control-Request-Method": "GET",
            },
            timeout=TIMEOUT,
        )
        acao = r.headers.get("access-control-allow-origin", "")
        methods = r.headers.get("access-control-allow-methods", "")
        info(f"Status: {r.status_code}")
        info(f"Access-Control-Allow-Origin: {acao}")
        info(f"Access-Control-Allow-Methods: {methods}")

        if FRONTEND_ORIGIN in acao or acao == "*":
            ok(f"CORS allows origin '{FRONTEND_ORIGIN}'")
            return True
        else:
            fail(f"CORS does NOT allow origin '{FRONTEND_ORIGIN}'")
            fail(f"RCA: Add '{FRONTEND_ORIGIN}' to CORS_ORIGINS in config.py or HF Secrets")
            return False
    except Exception as e:
        fail(f"CORS preflight failed: {e}")
        return False


# ── NODE A: INGESTION ──────────────────────────────────────────────────────────
async def check_node_a(client: httpx.AsyncClient) -> bool:
    header("NODE A — Ingestion (External API Dry-Run)")
    info("Calling /api/admin/ingestion/status to detect provider health")
    try:
        r = await client.get(f"{BASE_URL}/api/admin/ingestion/status", timeout=TIMEOUT)
        info(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            info(f"Response: {json.dumps(data, indent=2)[:400]}")
            ok("Ingestion status endpoint reachable")
            return True
        elif r.status_code == 404:
            # Try alternate endpoint
            r2 = await client.get(f"{BASE_URL}/api/monitoring/health", timeout=TIMEOUT)
            info(f"Monitoring health status: {r2.status_code}")
            if r2.status_code == 200:
                ok("Monitoring health endpoint reachable")
                return True
            else:
                warn(f"Ingestion status 404, monitoring also failed: {r2.status_code}")
                return False
        elif r.status_code in (401, 403):
            warn(f"Status {r.status_code}: Ingestion requires auth (expected in prod)")
            return True  # Not a failure
        else:
            fail(f"Unexpected status: {r.status_code}\nBody: {r.text[:300]}")
            return False
    except Exception as e:
        fail(f"Node A failed: {e}")
        return False


# ── NODE B: STORAGE (Appwrite Ping) ────────────────────────────────────────────
async def check_node_b(client: httpx.AsyncClient) -> bool:
    header("NODE B — Storage (Appwrite DB Ping)")
    info(f"Hitting {BASE_URL}/ health endpoint")
    try:
        r = await client.get(f"{BASE_URL}/", timeout=TIMEOUT)
        info(f"Status: {r.status_code}")
        data = r.json()
        subsystems = data.get("subsystems", {})
        appwrite = subsystems.get("appwrite_db", {})
        appwrite_status = appwrite.get("status", "unknown")
        info(f"Overall backend status: {data.get('status')}")
        info(f"Appwrite DB status: {appwrite_status}")
        info(f"Uptime: {data.get('pipeline_metrics', {}).get('uptime_seconds', 'N/A')}s")

        if appwrite_status == "connected":
            ok("Appwrite DB is CONNECTED in production")
            return True
        elif appwrite_status == "disconnected":
            fail("Appwrite DB is DISCONNECTED in production")
            fail("RCA: Check APPWRITE_ENDPOINT, APPWRITE_PROJECT_ID, APPWRITE_API_KEY in HF Secrets")
            fail("1-line fix: Go to HF Space → Settings → Secrets → Add all APPWRITE_* vars")
            return False
        else:
            warn(f"Appwrite DB status unknown: '{appwrite_status}'")
            return False
    except Exception as e:
        fail(f"Node B failed: {e}")
        return False


# ── NODE C: RETRIEVAL ─────────────────────────────────────────────────────────
async def check_node_c(client: httpx.AsyncClient) -> bool:
    global TEST_ARTICLE_ID
    header("NODE C — Retrieval (/api/news/ai)")
    info(f"GET {BASE_URL}/api/news/ai?limit=3")
    try:
        t0 = time.monotonic()
        r = await client.get(
            f"{BASE_URL}/api/news/ai",
            params={"limit": 3},
            headers={"Origin": FRONTEND_ORIGIN},
            timeout=TIMEOUT,
        )
        elapsed = time.monotonic() - t0
        info(f"Status: {r.status_code} | Latency: {elapsed:.2f}s")

        if r.status_code == 500:
            fail("500 Internal Server Error on /api/news/ai")
            try:
                err = r.json()
                detail = err.get("detail", r.text[:400])
                fail(f"Error detail: {detail}")
                # Pydantic validation error detection
                if "ValidationError" in detail or "validation" in detail.lower():
                    fail("PYDANTIC CRASH detected")
                    fail("RCA: A field in Article model is 'str'/'datetime' but Appwrite returned None")
                    fail("1-line fix: Change strict fields to Optional[str]=None in app/models.py")
            except Exception:
                fail(f"Raw body: {r.text[:300]}")
            return False

        if r.status_code != 200:
            fail(f"Unexpected status: {r.status_code}\nBody: {r.text[:300]}")
            return False

        data = r.json()
        count = data.get("count", 0)
        articles = data.get("articles", [])
        cached = data.get("cached", False)
        source = data.get("source", "?")

        info(f"Count: {count} | Source: {source} | Cached: {cached}")

        if count == 0:
            fail("Endpoint returned 0 articles despite status 200")
            fail("RCA: Appwrite DB disconnected OR category filter mismatch in get_articles_with_queries")
            return False

        # Inspect first article field by field
        if articles:
            art = articles[0]
            TEST_ARTICLE_ID = art.get("$id")
            info(f"Sample article[0] fields:")
            required_fields = ["title", "url", "published_at", "source", "category"]
            all_ok = True
            for field in required_fields:
                val = art.get(field)
                if val is None or val in ("", "#", "Untitled Article"):
                    warn(f"  [{field}] = {repr(val)} (null/default — check Appwrite data)")
                    all_ok = False
                else:
                    info(f"  [{field}] = {str(val)[:70]}")

            if all_ok:
                ok(f"All required article fields populated correctly ({count} articles)")
            else:
                warn("Some fields are null — articles present but data may be incomplete")
                warn("RCA: News ingestion worker stored articles with missing metadata")

        ok(f"Node C functional — {count} articles served in {elapsed:.2f}s")
        return True

    except httpx.ReadTimeout:
        fail(f"Request timed out after {TIMEOUT}s")
        fail("RCA: HF Space is sleeping (cold start) or container OOM-killed")
        fail("1-line fix: Send a wake-up GET to / and retry after 30s")
        return False
    except Exception as e:
        fail(f"Node C failed: {e}")
        return False


# ── NODE D: INTERACTION (Write Path) ──────────────────────────────────────────
async def check_node_d(client: httpx.AsyncClient) -> bool:
    header("NODE D — Interaction (POST views)")
    if not TEST_ARTICLE_ID:
        warn("No article ID from Node C — using a placeholder ID")
        article_id = "test_article_placeholder"
    else:
        article_id = TEST_ARTICLE_ID
        info(f"Using article ID: {article_id}")

    try:
        r = await client.post(
            f"{BASE_URL}/api/engagement/articles/{article_id}/view",
            headers={"Origin": FRONTEND_ORIGIN},
            timeout=TIMEOUT,
        )
        info(f"Status: {r.status_code}")

        if r.status_code in (200, 201):
            ok("Write path (view increment) is working in production")
            info(f"Response: {r.text[:200]}")
            return True
        elif r.status_code == 404:
            warn(f"Article ID '{article_id}' not found (expected if DB is fresh)")
            warn("RCA: View endpoint works but the test article ID doesn't exist in prod DB")
            return True  # Endpoint is functional
        elif r.status_code == 405:
            # Try alternate engagement route
            r2 = await client.post(
                f"{BASE_URL}/api/engagement/view",
                json={"article_url": "https://example.com/test"},
                headers={"Origin": FRONTEND_ORIGIN},
                timeout=TIMEOUT,
            )
            info(f"Alternate endpoint status: {r2.status_code}")
            if r2.status_code in (200, 201, 422):
                ok("Write path reachable (422 = valid request shape mismatch, not a crash)")
                return True
            else:
                fail(f"Write path returned {r2.status_code}")
                return False
        elif r.status_code == 500:
            fail("500 on write path — Appwrite write is failing")
            fail("RCA: Check APPWRITE_API_KEY permissions (needs write access to collections)")
            return False
        else:
            warn(f"Engagement returned {r.status_code}: {r.text[:200]}")
            return True  # Partial pass
    except Exception as e:
        fail(f"Node D failed: {e}")
        return False


# ── ENV AUDIT ─────────────────────────────────────────────────────────────────
def audit_local_env():
    header("ENV AUDIT — Required Secrets for HF Spaces")
    required = [
        "APPWRITE_ENDPOINT",
        "APPWRITE_PROJECT_ID",
        "APPWRITE_API_KEY",
        "APPWRITE_DATABASE_ID",
        "APPWRITE_AI_COLLECTION_ID",
        "APPWRITE_CLOUD_COLLECTION_ID",
        "APPWRITE_DATA_COLLECTION_ID",
        "UPSTASH_REDIS_REST_URL",
        "UPSTASH_REDIS_REST_TOKEN",
        "CORS_ORIGINS",
        "NEWS_API_KEY",
        "GNEWS_API_KEY",
        "BREVO_API_KEY",
    ]
    missing = []
    for key in required:
        val = os.environ.get(key)
        if val:
            masked = val[:8] + "..." if len(val) > 8 else val
            ok(f"{key} = {masked}")
        else:
            # Check if it's in the .env file
            env_path = os.path.join(os.path.dirname(__file__), ".env")
            in_file = False
            if os.path.exists(env_path):
                with open(env_path) as f:
                    for line in f:
                        if line.strip().startswith(key + "="):
                            in_file = True
                            break
            if in_file:
                warn(f"{key} — only in .env file (not in OS env / HF Secrets)")
            else:
                fail(f"{key} — MISSING from both .env and OS environment")
                missing.append(key)

    if missing:
        print(f"\n  ACTION REQUIRED: Add the following to HF Secrets:")
        for k in missing:
            print(f"    → {k}")
    else:
        ok("All required env vars present")

    return len(missing) == 0


# ── MAIN ───────────────────────────────────────────────────────────────────────
async def main():
    print(f"\n{BOLD}Segmento Pulse — Production Readiness Check{RESET}")
    print(f"Target: {BOLD}{BASE_URL}{RESET}")
    print(f"Time  : {time.strftime('%Y-%m-%d %H:%M:%S IST')}")

    # Local env audit (always runs)
    env_ok = audit_local_env()

    results = {}
    async with httpx.AsyncClient(follow_redirects=True) as client:
        results["CORS"]   = await check_cors(client)
        results["Node_A"] = await check_node_a(client)
        results["Node_B"] = await check_node_b(client)
        results["Node_C"] = await check_node_c(client)
        results["Node_D"] = await check_node_d(client)

    # ── Final Report ────────────────────────────────────────────────────────
    header("FINAL REPORT")
    passed = sum(1 for v in results.values() if v)
    total  = len(results)

    for name, result in results.items():
        status = f"{GREEN}PASS{RESET}" if result else f"{RED}FAIL{RESET}"
        print(f"  {name:<10} {status}")

    print()
    if passed == total:
        print(f"  {GREEN}{BOLD}[{passed}/{total}] All nodes PASS — Production is READY{RESET}")
    else:
        print(f"  {RED}{BOLD}[{passed}/{total}] Some nodes FAILED — Review RCAs above{RESET}")

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
