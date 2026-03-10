"""Unit-test configuration — runs before any test module is imported."""

import os
import sys

# The import chain (routers → database → settings) requires API_KEY.
# Unit tests never call the real API, so a dummy value is sufficient.
# Set this BEFORE any imports to ensure settings picks it up.
os.environ["API_KEY"] = "test"

# Also set other required env vars that settings might need
os.environ.setdefault("NAME", "Learning Management Service")
os.environ.setdefault("DEBUG", "false")
os.environ.setdefault("ADDRESS", "127.0.0.1")
os.environ.setdefault("PORT", "8000")
os.environ.setdefault("RELOAD", "false")
os.environ.setdefault("CORS_ORIGINS", "[]")
os.environ.setdefault("APP_ENABLE_INTERACTIONS", "true")
os.environ.setdefault("APP_ENABLE_LEARNERS", "true")
os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_NAME", "test")
os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("AUTOCHECKER_API_URL", "https://auche.namaz.live")
os.environ.setdefault("AUTOCHECKER_EMAIL", "")
os.environ.setdefault("AUTOCHECKER_PASSWORD", "")
