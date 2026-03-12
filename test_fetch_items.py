#!/usr/bin/env python
"""Тест для функции fetch_items."""

import asyncio
import sys
from pathlib import Path

src_dir = Path(__file__).resolve().parent / "backend"
sys.path.insert(0, str(src_dir))

from app.etl import fetch_items


async def main():
    try:
        print("Вызов fetch_items()...")
        items = await fetch_items()
        print(f"✓ Получено {len(items)} элементов")
        if items:
            print(f"  Пример: {items[0]}")
    except httpx.HTTPStatusError as e:
        print(f"✗ HTTP ошибка: {e.response.status_code}")
        print(f"  {e.response.text[:200]}")
    except Exception as e:
        print(f"✗ Ошибка: {type(e).__name__}: {e}")


if __name__ == "__main__":
    import httpx
    asyncio.run(main())
