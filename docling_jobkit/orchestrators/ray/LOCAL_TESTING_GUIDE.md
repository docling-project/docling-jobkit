# Local Testing Guide for Ray Orchestrator

This guide walks you through testing the Ray Orchestrator on your local machine.

## Quick Start

### 1. Install Dependencies

```bash
pip install "ray[data]>=2.0.0" "redis[asyncio]>=5.0.0" msgpack
```

### 2. Start Redis with Docker

```bash
docker run -d --name redis-test -p 6379:6379 redis:7-alpine
```

### 3. Ray Auto-Starts

**You don't need to manually start Ray!** The orchestrator automatically starts a local Ray instance when you set `ray_address=None` in your config:

```python
config = RayOrchestratorConfig(
    redis_url="redis://localhost:6379/",
    ray_address=None,  # Ray auto-starts
)
# Dashboard: http://127.0.0.1:8265
```

Then configure to use it:
```python
config = RayOrchestratorConfig(
    redis_url="redis://localhost:6379/",
    ray_address="auto",  # Connect to running Ray
)
```

**Stop Ray when done:**
```bash
ray stop
```

## Quick Test Script

Create a file `test_ray_local.py`:

```python
"""Quick local test of Ray Orchestrator."""

import asyncio
from pathlib import Path

from docling_jobkit.convert.manager import (
    DoclingConverterManager,
    DoclingConverterManagerConfig,
)
from docling_jobkit.datamodel.http_inputs import FileSource
from docling_jobkit.datamodel.task_targets import InBodyTarget
from docling_jobkit.orchestrators.ray import (
    RayOrchestrator,
    RayOrchestratorConfig,
)


async def main():
    print("🚀 Starting Ray Orchestrator local test...")
    
    # Configure orchestrator
    config = RayOrchestratorConfig(
        redis_url="redis://localhost:6379/",
        dispatcher_interval=1.0,  # Check every 1 second for testing
        max_concurrent_tasks=2,
        ray_address=None,  # Auto-start local Ray
    )
    
    # Create converter manager
    cm_config = DoclingConverterManagerConfig(
        enable_remote_services=False,  # Use local models
    )
    cm = DoclingConverterManager(config=cm_config)
    
    # Create orchestrator
    print("📦 Creating orchestrator...")
    orchestrator = RayOrchestrator(config=config, converter_manager=cm)
    
    # Check connections
    print("🔍 Checking connections...")
    try:
        await orchestrator.check_connection()
        print("✅ Connections healthy!")
    except Exception as e:
        print(f"❌ Connection check failed: {e}")
        return
    
    # Start processing queue in background
    print("🎬 Starting queue processing...")
    queue_task = asyncio.create_task(orchestrator.process_queue())
    
    # Give it a moment to start
    await asyncio.sleep(2)
    
    # Enqueue a test task
    print("\n📝 Enqueueing test task...")
    
    # Create a simple test document
    test_content = b"%PDF-1.4\nTest PDF content"
    sources = [
        FileSource(
            filename="test_document.pdf",
            content=test_content,
        )
    ]
    
    # Import convert options
    from docling_jobkit.datamodel.convert import ConvertDocumentsOptions
    
    task = await orchestrator.enqueue(
        sources=sources,
        target=InBodyTarget(),
        convert_options=ConvertDocumentsOptions(),  # Use default options
    )
    
    # Set user metadata
    task.metadata["user_id"] = "test_user_123"
    
    print(f"✅ Task enqueued: {task.task_id}")
    print(f"   User: {task.metadata.get('user_id')}")
    print(f"   Status: {task.task_status}")
    print(f"   Sources: {len(task.sources)}")
    
    # Get queue stats
    print("\n📊 Queue statistics:")
    stats = await orchestrator.get_stats()
    print(f"   Queue size: {stats['queue_size']}")
    print(f"   Total tasks: {stats['total_tasks']}")
    print(f"   Users with tasks: {stats['users_with_tasks']}")
    
    # Monitor task progress
    print("\n⏳ Monitoring task progress...")
    max_wait = 30  # seconds
    waited = 0
    
    while not task.is_completed() and waited < max_wait:
        await asyncio.sleep(2)
        waited += 2
        task = await orchestrator.task_status(task.task_id)
        print(f"   Status: {task.task_status} (waited {waited}s)")
    
    if task.is_completed():
        print(f"\n✅ Task completed with status: {task.task_status}")
        
        # Try to get result
        result = await orchestrator.task_result(task.task_id)
        if result:
            print(f"   Processing time: {result.processing_time:.2f}s")
            print(f"   Documents converted: {result.num_converted}")
            print(f"   Succeeded: {result.num_succeeded}")
            print(f"   Failed: {result.num_failed}")
            print(f"   Result type: {result.result.kind}")
    else:
        print(f"\n⚠️  Task did not complete within {max_wait}s")
    
    # Cleanup
    print("\n🧹 Cleaning up...")
    await orchestrator.shutdown()
    queue_task.cancel()
    try:
        await queue_task
    except asyncio.CancelledError:
        pass
    
    print("✅ Test complete!")


if __name__ == "__main__":
    asyncio.run(main())
```

## Run the Test

```bash
# Make sure Redis is running
redis-cli ping

# Run the test
python test_ray_local.py
```

**Expected output:**
```
🚀 Starting Ray Orchestrator local test...
📦 Creating orchestrator...
🔍 Checking connections...
✅ Connections healthy!
🎬 Starting queue processing...

📝 Enqueueing test task...
✅ Task enqueued: abc-123-def-456
   User: test_user_123
   Status: TaskStatus.PENDING
   Sources: 1

📊 Queue statistics:
   Queue size: 1
   Total tasks: 1
   Users with tasks: 1

⏳ Monitoring task progress...
   Status: TaskStatus.STARTED (waited 2s)
   Status: TaskStatus.SUCCESS (waited 4s)

✅ Task completed with status: TaskStatus.SUCCESS
   Documents processed: 1
   Succeeded: 1
   Failed: 0

🧹 Cleaning up...
✅ Test complete!
```

## Running Pytest Tests

```bash
# Run all Ray tests
pytest tests/test_ray_orchestrator.py -v

# Run specific test
pytest tests/test_ray_orchestrator.py::test_enqueue_task -v

# Run with output
pytest tests/test_ray_orchestrator.py -v -s
```

**Note:** Tests will be skipped if Ray or Redis are not available.

## Troubleshooting

### Redis Connection Issues

**Problem:** `redis.exceptions.ConnectionError: Error connecting to Redis`

**Solutions:**
```bash
# Check if Redis is running
redis-cli ping

# Check Redis port
netstat -an | grep 6379

# Try connecting manually
redis-cli
> ping
> exit

# Check Redis logs (if using Docker)
docker logs redis-test
```

### Ray Issues

**Problem:** Ray fails to start or connect

**Solutions:**
```bash
# Check if Ray is already running
ray status

# Stop any existing Ray instances
ray stop

# Clear Ray temp files
rm -rf /tmp/ray

# Let orchestrator auto-start Ray
# (use ray_address=None in config)
```

### Import Errors

**Problem:** `ModuleNotFoundError: No module named 'ray'`

**Solution:**
```bash
# Install Ray with data support
pip install "ray[data]>=2.0.0"

# Verify installation
python -c "import ray; print(ray.__version__)"
```

### Memory Issues

**Problem:** Ray workers running out of memory

**Solutions:**
```python
# Reduce concurrent tasks
config = RayOrchestratorConfig(
    max_concurrent_tasks=1,  # Lower limit
    ray_memory_limit_per_worker="2GB",  # Set memory limit
)
```

## Monitoring

### Ray Dashboard

If you manually started Ray, access the dashboard:
```
http://localhost:8265
```

Features:
- View active tasks and workers
- Monitor resource usage (CPU, memory)
- See task execution timeline
- Debug failed tasks

### Redis Monitoring

```bash
# Monitor Redis commands in real-time
redis-cli monitor

# Check memory usage
redis-cli info memory

# List all keys
redis-cli keys '*'

# Check specific user's queue
redis-cli llen "user:test_user_123:tasks"

# View task metadata
redis-cli hgetall "task:abc-123-def-456"
```

## Clean Up After Testing

```bash
# Stop Redis (if using Docker)
docker stop redis-test
docker rm redis-test

# Stop Ray (if manually started)
ray stop

# Clear Redis data (if needed)
redis-cli FLUSHALL

# Clear Ray temp files
rm -rf /tmp/ray
```

## Next Steps

Once local testing works:

1. **Test with real PDFs**: Replace test content with actual PDF files
2. **Test multi-user scenarios**: Enqueue tasks with different user_ids
3. **Test queue limits**: Configure limits and verify rejection
4. **Load testing**: Enqueue many tasks to test scalability
5. **Integration testing**: Integrate with docling-serve

## Common Test Scenarios

### Test Multi-User Fair Scheduling

```python
# Enqueue tasks for multiple users
for user_id in ["alice", "bob", "charlie"]:
    for i in range(5):
        task = await orchestrator.enqueue(sources=[...], target=InBodyTarget())
        task.metadata["user_id"] = user_id

# Watch fair scheduling in action - each user gets one task per round
```

### Test Queue Limits

```python
config = RayOrchestratorConfig(
    max_queued_tasks=3,
    enable_queue_limit_rejection=True,
)

# Try to enqueue more than limit
try:
    for i in range(5):
        await orchestrator.enqueue(sources=[...], target=InBodyTarget())
except QueueLimitExceededError:
    print("Queue limit reached!")
```

### Test with Real Documents

```python
# Load a real PDF
with open("path/to/document.pdf", "rb") as f:
    pdf_content = f.read()

sources = [FileSource(filename="document.pdf", content=pdf_content)]
task = await orchestrator.enqueue(sources=sources, target=InBodyTarget())
```

## Getting Help

If you encounter issues:

1. Check the logs for error messages
2. Verify Redis and Ray are running
3. Review the troubleshooting section above
4. Check the main README.md for configuration details
5. Open an issue with logs and error messages