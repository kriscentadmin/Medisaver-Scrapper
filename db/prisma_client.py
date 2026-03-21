from prisma import Prisma
import os
from dotenv import load_dotenv

# Load environment variables from .env (if it exists)
load_dotenv()

# Get DATABASE_URL safely
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Please define it in your environment or .env file.")

db = Prisma(
    datasource={
        "db": {
            "url": DATABASE_URL
        } 
    }
)

async def connect():
    if not db.is_connected():
        await db.connect()

async def disconnect():
    if db.is_connected():
        await db.disconnect()
