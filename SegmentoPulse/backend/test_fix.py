import asyncio, sys, os
sys.path.append(os.getcwd())

async def test():
    from app.services.appwrite_db import get_appwrite_db
    db = get_appwrite_db()
    articles = await db.get_articles('ai', limit=3)
    print(f'Count: {len(articles)}')
    for a in articles:
        title = a.get('title', 'MISSING')
        source = a.get('source', 'MISSING')
        pub = a.get('published_at', 'MISSING')
        print(f'  title={title[:60] if title else None} | source={source} | published_at={pub}')

asyncio.run(test())
