import asyncio
import os
import sys

# Add current directory to python path
sys.path.append(os.getcwd())

from app.services.newsletter_service import send_scheduled_newsletter
from app.config import settings

async def main():
    print("🚀 Manually triggering 'Evening' newsletter...")
    
    try:
        result = await send_scheduled_newsletter("Evening")
        print("\n✅ Result:", result)
    except Exception as e:
        print("\n❌ Error:", e)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        # Manual trigger
        asyncio.run(main())
            
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Execution Error: {e}")
