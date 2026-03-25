import asyncio
import weakref
import sys

class _DummyTask:
    def get_loop(self): return asyncio.get_running_loop()
    def get_name(self): return "dummy-task"
    def set_name(self, name): pass

_GLOBAL_DUMMY_TASK = _DummyTask()

def test_patch():
    try:
        w = weakref.ref(_GLOBAL_DUMMY_TASK)
        print(f"Weakref created: {w}")
        print(f"Weakref target: {w()}")
        
        # Test basic methods
        loop = _GLOBAL_DUMMY_TASK.get_loop()
        print(f"Loop from dummy: {loop}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(asyncio.to_thread(test_patch))
