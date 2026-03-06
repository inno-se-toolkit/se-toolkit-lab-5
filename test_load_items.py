"""Test script for load_items() function."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from sqlmodel import select

from app.database import engine, get_session
from app.etl import fetch_items, load_items
from app.models.item import ItemRecord


async def test_load_items():
    """Test the load_items function."""
    print("Testing load_items()...")
    print("-" * 50)

    try:
        # Step 1: Create tables if they don't exist
        print("\n[Step 1] Creating database tables...")
        async with engine.begin() as conn:
            from app.models.item import ItemRecord
            from app.models.learner import Learner
            from app.models.interaction import InteractionLog

            await conn.run_sync(ItemRecord.metadata.create_all)
        print("[OK] Tables created/verified")

        # Step 2: Fetch items from API
        print("\n[Step 2] Fetching items from API...")
        items = await fetch_items()
        print(f"[OK] Fetched {len(items)} items")

        # Step 3: Load items into database (first run)
        print("\n[Step 3] Loading items into database (first run)...")
        async for session in get_session():
            new_count = await load_items(items, session)
            print(f"[OK] Created {new_count} new items")

            # Verify: count items in database
            result = await session.exec(select(ItemRecord))
            db_items = list(result.all())
            print(f"[OK] Database now has {len(db_items)} items")

            # Count by type
            labs = [i for i in db_items if i.type == "lab"]
            tasks = [i for i in db_items if i.type == "task"]
            print(f"  - Labs: {len(labs)}")
            print(f"  - Tasks: {len(tasks)}")

            # Step 4: Load items again (idempotency test)
            print("\n[Step 4] Loading items again (idempotency test)...")
            new_count_2 = await load_items(items, session)
            print(f"[OK] Created {new_count_2} new items (should be 0)")

            if new_count_2 == 0:
                print("[OK] Idempotency check PASSED - no duplicates created")
            else:
                print("[FAIL] Idempotency check FAILED - duplicates created!")

            # Verify final count
            result = await session.exec(select(ItemRecord))
            db_items_final = list(result.all())
            print(f"[OK] Database still has {len(db_items_final)} items")

            await session.close()
            break

        # Verify counts match
        if len(db_items) == len(db_items_final):
            print("[OK] Item count stable after second load")
        else:
            print("[FAIL] Item count changed after second load!")

        print("\n" + "=" * 50)
        if new_count_2 == 0:
            print("[OK] load_items() test PASSED")
            return True
        else:
            print("[FAIL] load_items() test FAILED (idempotency issue)")
            return False

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        print("\n" + "=" * 50)
        print("[FAIL] load_items() test FAILED")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_load_items())
    sys.exit(0 if success else 1)
