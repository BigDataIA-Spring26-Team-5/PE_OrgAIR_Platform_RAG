import signal
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

# IMPORT ROUTERS
from app.routers.companies import router as companies_router
from app.core.exceptions import validation_exception_handler
from app.routers.industries import router as industries_router
from app.routers.health import router as health_router
from app.routers.assessments import router as assessments_router
from app.routers.dimensionScores import router as dimension_scores_router
from app.routers.documents import router as documents_router
from app.routers.signals import router as signals_router
from app.routers.evidence import router as evidence_router
from app.routers.scoring import router as scoring_router
from app.routers.board_governance import router as board_governance_router
from app.routers.glassdoor_signals import router as glassdoor_signals_router
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

from app.shutdown import set_shutdown, is_shutting_down


# SWAGGER UI — tag display order
_OPENAPI_TAGS = [
    {"name": "Root"},
    {"name": "Health"},
    {"name": "Industries"},
    {"name": "Companies"},
    {"name": "Assessments"},
    {"name": "Dimension Scores"},
    {"name": "1. Collection"},
    {"name": "2. Parsing"},
    {"name": "3. Chunking"},
    {"name": "5. Management"},
    {"name": "6. Reset (Demo)"},
    {"name": "Signals"},
    {"name": "Evidence"},
    {"name": "Glassdoor Culture Signals"},
    {"name": "Board Governance"},
    {"name": "CS3 Dimensions Scoring"},
]

# FASTAPI APPLICATION CONFIGURATION
app = FastAPI(
    title="PE Org-AI-R Platform Foundation API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    openapi_tags=_OPENAPI_TAGS,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# REGISTER EXCEPTION HANDLERS
app.add_exception_handler(RequestValidationError, validation_exception_handler)

# REGISTER ROUTERS
app.include_router(health_router)            # Health
app.include_router(industries_router)        # Industries
app.include_router(companies_router)         # Companies       ← CS1 client target
app.include_router(assessments_router)       # Assessments     ← CS3 client target
app.include_router(dimension_scores_router)  # Dimension Scores← CS3 client target
app.include_router(documents_router)         # CS2 doc pipeline
app.include_router(signals_router)           # CS2 signal pipeline
app.include_router(evidence_router)          # Evidence mapping
app.include_router(board_governance_router)  # Board governance signal ← CS4 source
app.include_router(glassdoor_signals_router) # Culture signal         ← CS4 source
app.include_router(scoring_router)           # CS3 dimension scoring  ← CS4 justification

# REMOVED (not needed for CS4):
# tc_vr_router, pf_router, hr_router, orgair_router, property_tests_router


# ROOT ENDPOINT
@app.get("/", tags=["Root"], summary="Root endpoint")
async def root():
    return {
        "service": "PE Org-AI-R Platform Foundation API",
        "version": "1.0.0",
        "docs": {
            "swagger": "/docs",
            "redoc": "/redoc"
        },
        "status": "running"
    }


# STARTUP EVENT
@app.on_event("startup")
async def startup_event():
    print("Starting PE Org-AI-R Platform Foundation API...")
    print("Swagger UI available at: http://localhost:8000/docs")

    loop = asyncio.get_running_loop()

    def _signal_handler(sig):
        print(f"\n⚠️  Received {sig.name} — shutting down gracefully...")
        set_shutdown()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler, sig)
    except NotImplementedError:
        print("⚠️  Signal handlers not supported on Windows, using fallback...")
        _register_windows_signal_handlers()


# SHUTDOWN EVENT
@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down PE Org-AI-R Platform Foundation API...")
    set_shutdown()


def _register_windows_signal_handlers():
    original_sigint = signal.getsignal(signal.SIGINT)

    def _windows_handler(signum, frame):
        print(f"\n⚠️  Received Ctrl+C — shutting down gracefully...")
        set_shutdown()
        if callable(original_sigint):
            original_sigint(signum, frame)

    signal.signal(signal.SIGINT, _windows_handler)


# RUN WITH UVICORN
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )