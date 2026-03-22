"""Allow running with: python -m scraper.engine ..."""
from .engine import main
import asyncio

asyncio.run(main())
