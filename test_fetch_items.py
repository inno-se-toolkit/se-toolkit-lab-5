"""Test script for fetch_items() function."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from app.etl import fetch_items


async def test_fetch_items():
    """Test the fetch_items function."""
    print("Testing fetch_items()...")
    print("-" * 50)

    try:
        items = await fetch_items()
        print(f"[OK] Successfully fetched {len(items)} items")
        print()

        if items:
            print("Sample items (first 3):")
            for i, item in enumerate(items[:3]):
                print(f"  {i + 1}. {item}")
            print()

            # Count by type
            labs = [i for i in items if i.get("type") == "lab"]
            tasks = [i for i in items if i.get("type") == "task"]
            print(f"Labs: {len(labs)}, Tasks: {len(tasks)}")
        else:
            print("[WARN] No items returned (empty catalog)")

        print()
        print("=" * 50)
        print("[OK] fetch_items() test PASSED")
        return True

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        print()
        print("=" * 50)
        print("[FAIL] fetch_items() test FAILED")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_fetch_items())
    sys.exit(0 if success else 1)
