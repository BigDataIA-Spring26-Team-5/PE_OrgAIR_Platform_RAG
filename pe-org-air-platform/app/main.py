import signal
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError

# IMPORT ROUTERS
from app.routers.companies import router as companies_router
from app.routers.companies import validation_exception_handler
from app.routers.industries import router as industries_router
from app.routers.health import router as health_router
from app.routers.assessments import router as assessments_router
from app.routers.dimensionScores import router as dimension_scores_router
from app.routers.documents import router as documents_router
# from app.routers.pdf_parser import router as pdf_parser_router
from app.routers.signals import router as signals_router
from app.routers.evidence import router as evidence_router
from app.routers.scoring import router as scoring_router
# from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.cors import CORSMiddleware
from app.routers.board_governance import router as board_governance_router
from app.routers.glassdoor_signals import router as glassdoor_signals_router
# from app.routers.tc_vr_scoring import router as tc_vr_scoring_router
from app.routers.tc_vr_scoring import router as tc_vr_router
from app.routers.position_factor import router as pf_router
from app.routers.hr_scoring import router as hr_router
from app.routers.orgair_scoring import router as orgair_router
from app.routers.property_tests import router as property_tests_router
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
    {"name": "Reports"},
    {"name": "Glassdoor Culture Signals"},
    {"name": "Board Governance"},
    {"name": "CS3 Dimensions Scoring"},
    {"name": "CS3 TC + V^R Scoring"},
    {"name": "CS3 Position Factor"},
    {"name": "CS3 H^R (Human Readiness)"},
    {"name": "CS3 Org-AI-R"},
    {"name": "Property-Based Tests"},
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

# app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# REGISTER EXCEPTION HANDLERS
app.add_exception_handler(RequestValidationError, validation_exception_handler)

# REGISTER ROUTERS (order matches _OPENAPI_TAGS / Swagger UI display order)
# app.include_router(tc_vr_scoring_router)
app.include_router(health_router)           # Health
app.include_router(industries_router)       # Industries
app.include_router(companies_router)        # Companies
app.include_router(assessments_router)      # Assessments
app.include_router(dimension_scores_router) # Dimension Scores
app.include_router(documents_router)        # 1.Collection / 2.Parsing / 3.Chunking / 5.Management / 6.Reset(Demo)
# app.include_router(pdf_parser_router)
app.include_router(signals_router)          # Signals / Reset(Demo) / Signal Scoring
app.include_router(evidence_router)         # Evidence
app.include_router(board_governance_router) # Board Governance
app.include_router(glassdoor_signals_router)# Glassdoor Culture Signals
app.include_router(scoring_router)          # CS3 Scoring
app.include_router(tc_vr_router)            # CS3 TC + V^R Scoring
app.include_router(pf_router)               # CS3 Position Factor
app.include_router(hr_router)               # CS3 H^R (Human Readiness)
app.include_router(orgair_router)           # CS3 Org-AI-R
app.include_router(property_tests_router)  # Property-Based Tests


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

    # Register signal handlers for graceful shutdown (Ctrl+C / kill)
    loop = asyncio.get_running_loop()

    def _signal_handler(sig):
        print(f"\n⚠️  Received {sig.name} — shutting down gracefully...")
        set_shutdown()

    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _signal_handler, sig)
    except NotImplementedError:
        # Windows doesn't support add_signal_handler — use fallback
        print("⚠️  Signal handlers not supported on Windows, using fallback...")
        _register_windows_signal_handlers()


# SHUTDOWN EVENT
@app.on_event("shutdown")
async def shutdown_event():
    print("Shutting down PE Org-AI-R Platform Foundation API...")
    set_shutdown()  # Ensure flag is set even if signal handler didn't fire


def _register_windows_signal_handlers():
    """Fallback for Windows where loop.add_signal_handler is not supported."""
    import threading

    def _ctrl_c_watcher():
        """Watch for KeyboardInterrupt in a background thread."""
        import time
        while not _shutdown_event.is_set():
            time.sleep(0.5)

    original_sigint = signal.getsignal(signal.SIGINT)

    def _windows_handler(signum, frame):
        print(f"\n⚠️  Received Ctrl+C — shutting down gracefully...")
        set_shutdown()
        # Call original handler to let uvicorn shut down too
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