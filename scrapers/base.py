import random
import asyncio

USER_AGENTS = [

"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/118.0 Safari/537.36",
"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/118.0",
]

async def human_delay(min_s=2, max_s=6):

    await asyncio.sleep(random.uniform(min_s, max_s))

def random_ua():

    return random.choice(USER_AGENTS)

