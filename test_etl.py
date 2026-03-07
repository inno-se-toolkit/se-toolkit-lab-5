#!/usr/bin/env python3
"""Test script for fetch_logs function."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from app.etl import fetch_logs, fetch_items
from app.settings import settings


async def test_fetch_items():
    """Test fetching items from the API."""
    print("=" * 60)
    print("Testing fetch_items()...")
    print("=" * 60)
    print(f"API URL: {settings.autochecker_api_url}")
    print(f"Email: {settings.autochecker_email}")
    print()

    try:
        items = await fetch_items()
        print(f"✓ Successfully fetched {len(items)} items")
        if items:
            print(f"  First item: {items[0]}")
        return items
    except Exception as e:
        print(f"✗ Error fetching items: {e}")
        return None


async def test_fetch_logs():
    """Test fetching logs from the API."""
    print("=" * 60)
    print("Testing fetch_logs()...")
    print("=" * 60)
    print(f"API URL: {settings.autochecker_api_url}")
    print(f"Email: {settings.autochecker_email}")
    print()

    try:
        logs = await fetch_logs()
        print(f"✓ Successfully fetched {len(logs)} logs")
        if logs:
            print(f"  First log: {logs[0]}")
        return logs
    except Exception as e:
        print(f"✗ Error fetching logs: {e}")
        return None


async def main():
    """Run all tests."""
    print("\n=== ETL Function Tests ===\n")

    # Test fetch_items first
    items = await test_fetch_items()
    print()

    # Test fetch_logs
    logs = await test_fetch_logs()
    print()

    if items and logs:
        print("✓ All fetch functions working correctly!")
    else:
        print("✗ Some tests failed. Check your credentials and network.")


if __name__ == "__main__":
    asyncio.run(main())
