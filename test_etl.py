"""Test script for ETL functions."""

import asyncio
from backend.app.etl import fetch_items, fetch_logs


async def test_fetch_items():
    """Test fetch_items function."""
    print("Testing fetch_items()...")
    try:
        items = await fetch_items()
        print(f"✓ Success! Fetched {len(items)} items")
        if items:
            print(f"  First item: {items[0]}")
        return items
    except Exception as e:
        print(f"✗ Error: {e}")
        raise


async def test_fetch_logs():
    """Test fetch_logs function."""
    print("\nTesting fetch_logs()...")
    try:
        logs = await fetch_logs()
        print(f"✓ Success! Fetched {len(logs)} logs")
        if logs:
            print(f"  First log: {logs[0]}")
        return logs
    except Exception as e:
        print(f"✗ Error: {e}")
        raise


async def main():
    """Run all tests."""
    print("=" * 50)
    print("ETL Function Tests")
    print("=" * 50)
    
    items = await test_fetch_items()
    logs = await test_fetch_logs()
    
    print("\n" + "=" * 50)
    print("All tests completed!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
