"""Test script for fetch_logs() function."""

import asyncio
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

from app.etl import fetch_logs


async def test_fetch_logs():
    """Test the fetch_logs function."""
    print("Testing fetch_logs()...")
    print("-" * 50)

    try:
        # Test 1: Fetch all logs (no since parameter)
        print("\n[Test 1] Fetching ALL logs (since=None)...")
        all_logs = await fetch_logs(since=None)
        print(f"[OK] Fetched {len(all_logs)} total logs")

        if all_logs:
            print("\nSample log (first one):")
            print(f"  {all_logs[0]}")
            print("\nSample log (last one):")
            print(f"  {all_logs[-1]}")

        # Test 2: Fetch logs with a since parameter (incremental sync)
        print("\n" + "=" * 50)
        print("\n[Test 2] Testing incremental sync (since=last_log_time)...")
        if all_logs:
            # Get the timestamp of the last log
            last_log_time_str = all_logs[-1].get("submitted_at")
            print(f"Last log timestamp: {last_log_time_str}")

            from datetime import datetime
            last_log_time = datetime.fromisoformat(last_log_time_str)

            # Fetch logs since that time (should return few or none)
            incremental_logs = await fetch_logs(since=last_log_time)
            print(f"[OK] Fetched {len(incremental_logs)} logs since last timestamp")

            # Verify that incremental fetch returns fewer logs
            if len(incremental_logs) <= len(all_logs):
                print("[OK] Incremental fetch returned fewer/equal logs (as expected)")
            else:
                print("[WARN] Incremental fetch returned more logs than expected")

        print("\n" + "=" * 50)
        print("[OK] fetch_logs() test PASSED")
        return True

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        print("\n" + "=" * 50)
        print("[FAIL] fetch_logs() test FAILED")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_fetch_logs())
    sys.exit(0 if success else 1)
