import os
import pytest

# Устанавливаем переменные окружения ДО импорта app
os.environ["API_KEY"] = "test"

from fastapi.testclient import TestClient
from app.main import app
from app.settings import settings

# Убедимся, что settings тоже получил значение
settings.API_KEY = "test"

@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    with TestClient(app) as client:
        yield client
