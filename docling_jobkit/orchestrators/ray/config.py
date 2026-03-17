"""Configuration for Ray orchestrator."""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RayOrchestratorConfig(BaseSettings):
    """Configuration for Ray + Redis orchestrator.

    This orchestrator uses Ray Serve for autoscaling document processing,
    with Redis for state management and fair task scheduling.

    All settings can be overridden via environment variables with DOCLING_ prefix.
    Example: DOCLING_MIN_WORKERS=5 DOCLING_MAX_WORKERS=20
    """

    model_config = SettingsConfigDict(
        env_prefix="DOCLING_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Redis Configuration
    # Supports standard Redis, Redis Sentinel, and Redis Cluster via URL:
    # - Standard: "redis://localhost:6379/"
    # - Sentinel: "redis+sentinel://sentinel-host:26379/mymaster/0"
    # - Cluster: "redis://cluster-node:6379/?cluster=true"
    redis_url: str = Field(
        default="redis://localhost:6379/",
        description="Redis connection URL (supports standard, sentinel, cluster)",
    )
    redis_max_connections: int = Field(
        default=50, description="Maximum connections in Redis pool"
    )
    redis_socket_timeout: Optional[float] = Field(
        default=None, description="Socket timeout for Redis operations"
    )
    redis_socket_connect_timeout: Optional[float] = Field(
        default=None, description="Socket connect timeout for Redis"
    )

    # Result Storage
    results_ttl: int = Field(
        default=3600 * 4,
        description="Time-to-live for task results in seconds (4 hours)",
    )
    results_prefix: str = Field(
        default="docling:ray:results", description="Prefix for result keys in Redis"
    )

    # Pub/Sub
    sub_channel: str = Field(
        default="docling:ray:updates",
        description="Redis pub/sub channel for task updates",
    )

    # Fair Dispatcher Configuration
    dispatcher_interval: float = Field(
        default=2.0,
        description="Seconds between dispatch rounds (how often to check for new tasks)",
    )

    # Per-User Limits
    max_concurrent_tasks: int = Field(
        default=5,
        description="Max tasks being processed simultaneously per user",
    )
    max_queued_tasks: Optional[int] = Field(
        default=None,
        description="Max tasks in queue per user (None = unlimited)",
    )
    enable_queue_limit_rejection: bool = Field(
        default=False,
        description="Return 429 if queue limit exceeded (requires max_queued_tasks)",
    )
    max_documents: Optional[int] = Field(
        default=None,
        description="Max documents being processed per user (None = unlimited)",
    )
    enable_document_limits: bool = Field(
        default=False, description="Enable per-user document limits"
    )

    # Ray Configuration (no Ray Serve needed - using Ray Core + Ray Data)
    ray_address: Optional[str] = Field(
        default=None,
        description="Ray cluster address (None = auto-detect or start local)",
    )
    ray_namespace: str = Field(
        default="docling", description="Ray namespace for isolation"
    )
    ray_runtime_env: Optional[dict] = Field(
        default=None, description="Optional Ray runtime environment configuration"
    )

    # Ray mTLS Configuration
    enable_mtls: bool = Field(
        default=False,
        description="Enable mTLS authentication for Ray cluster connection",
    )
    ray_cluster_name: Optional[str] = Field(
        default=None,
        description="Ray cluster name for mTLS certificate generation (required when enable_mtls=True)",
    )

    # Ray Serve Autoscaling Configuration
    min_actors: int = Field(
        default=1,
        description="Minimum number of Ray Serve replicas (autoscaling lower bound)",
    )
    max_actors: int = Field(
        default=10,
        description="Maximum number of Ray Serve replicas (autoscaling upper bound)",
    )
    target_requests_per_replica: int = Field(
        default=1,
        description="Target number of concurrent requests per replica for autoscaling",
    )
    upscale_delay_s: float = Field(
        default=30.0,
        description="Seconds to wait before scaling up (prevents flapping)",
    )
    downscale_delay_s: float = Field(
        default=600.0,
        description="Seconds to wait before scaling down (10 minutes default)",
    )
    ray_num_cpus_per_actor: float = Field(
        default=1.0, description="Number of CPUs to allocate per Ray Serve replica"
    )

    # Fault Tolerance & Retry Configuration
    max_task_retries: int = Field(
        default=3, description="Maximum retries for failed tasks"
    )
    retry_delay: float = Field(
        default=5.0, description="Seconds to wait between task retries"
    )
    max_document_retries: int = Field(
        default=2, description="Maximum retries per document within a task"
    )

    # Ray Actor Configuration (for fault tolerance)
    dispatcher_max_restarts: int = Field(
        default=-1,
        description="Max dispatcher actor restarts (-1 = unlimited, for high availability)",
    )
    dispatcher_max_task_retries: int = Field(
        default=3, description="Ray-level task retries for dispatcher operations"
    )

    # Timeouts
    task_timeout: Optional[float] = Field(
        default=3600.0,
        description="Maximum seconds per task (None = no limit)",
    )
    document_timeout: Optional[float] = Field(
        default=300.0,
        description="Maximum seconds per document (None = no limit)",
    )
    redis_operation_timeout: float = Field(
        default=30.0, description="Timeout for Redis operations in seconds"
    )

    # Health Checks
    enable_heartbeat: bool = Field(
        default=True, description="Enable dispatcher heartbeat monitoring"
    )

    # Resource Management & Memory Monitoring
    ray_memory_limit_per_actor: Optional[str] = Field(
        default=None,
        description='Memory limit per Ray actor (e.g., "4GB")',
    )
    ray_object_store_memory: Optional[str] = Field(
        default=None,
        description='Ray object store memory (e.g., "10GB")',
    )
    enable_oom_protection: bool = Field(
        default=True,
        description="Enable out-of-memory detection and recovery in actors",
    )
    memory_warning_threshold: float = Field(
        default=0.9,
        description="Memory usage threshold for warnings (0.0-1.0)",
    )

    # Scratch Directory
    scratch_dir: Optional[Path] = Field(
        default=None,
        description="Directory for temporary files during processing",
    )

    # Logging
    log_level: str = Field(
        default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )
