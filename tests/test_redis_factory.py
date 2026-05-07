"""Tests for the shared Redis client factory (`_redis` module).

Covers:
  - URL classification (single / sentinel / cluster rejection)
  - Sentinel URL parsing (happy paths + edge cases)
  - Eager validation hook (`validate_url`)
  - Orchestrator-level rejection of cluster URLs at config-build time
"""

from __future__ import annotations

import pytest

from docling_jobkit.orchestrators import _redis as factory


class TestDetectMode:
    @pytest.mark.parametrize(
        "url",
        [
            "redis://localhost:6379/",
            "rediss://localhost:6379/",
            "unix:///tmp/redis.sock",
            "redis://h:6379/?ssl=true",  # query options other than cluster=
        ],
    )
    def test_single_node_schemes(self, url: str) -> None:
        assert factory.detect_mode(url) == factory.RedisMode.SINGLE

    @pytest.mark.parametrize(
        "url",
        [
            "redis+sentinel://h:26379/m",
            "rediss+sentinel://h:26379/m",
        ],
    )
    def test_sentinel_schemes(self, url: str) -> None:
        assert factory.detect_mode(url) == factory.RedisMode.SENTINEL

    @pytest.mark.parametrize(
        "url",
        [
            "redis+cluster://node:6379/0",
            "rediss+cluster://node:6379/0",
            "redis-cluster://node:6379/0",
        ],
    )
    def test_cluster_scheme_rejected(self, url: str) -> None:
        with pytest.raises(factory.UnsupportedRedisModeError, match="Cluster URL"):
            factory.detect_mode(url)

    @pytest.mark.parametrize("value", ["true", "1", "anything"])
    def test_cluster_query_param_rejected(self, value: str) -> None:
        with pytest.raises(
            factory.UnsupportedRedisModeError, match=r"\?cluster"
        ):
            factory.detect_mode(f"redis://node:6379/?cluster={value}")

    def test_unknown_scheme_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsupported redis URL scheme"):
            factory.detect_mode("memcached://x:11211/")


class TestParseSentinelUrl:
    def test_minimal(self) -> None:
        t = factory.parse_sentinel_url("redis+sentinel://h1:26379/mymaster")
        assert t.hosts == [("h1", 26379)]
        assert t.service_name == "mymaster"
        assert t.db == 0
        assert t.ssl is False
        assert t.username is None
        assert t.password is None

    def test_full_url(self) -> None:
        t = factory.parse_sentinel_url(
            "redis+sentinel://user:p%40ss@h1,h2:26380,h3:26381/mymaster/3"
            "?sentinel_username=su&sentinel_password=sp&ssl=true"
        )
        assert t.hosts == [("h1", 26379), ("h2", 26380), ("h3", 26381)]
        assert t.service_name == "mymaster"
        assert t.db == 3
        assert t.username == "user"
        assert t.password == "p@ss"  # url-decoded
        assert t.sentinel_username == "su"
        assert t.sentinel_password == "sp"
        assert t.ssl is True

    def test_rediss_sentinel_implies_ssl(self) -> None:
        t = factory.parse_sentinel_url("rediss+sentinel://h1:26379/m")
        assert t.ssl is True

    def test_default_sentinel_port(self) -> None:
        t = factory.parse_sentinel_url("redis+sentinel://h1,h2/m")
        assert t.hosts == [("h1", 26379), ("h2", 26379)]

    def test_rejects_wrong_scheme(self) -> None:
        with pytest.raises(ValueError, match="must start with"):
            factory.parse_sentinel_url("redis://h/0")

    def test_rejects_missing_master(self) -> None:
        with pytest.raises(ValueError, match="master/service name"):
            factory.parse_sentinel_url("redis+sentinel://h:26379/")

    def test_rejects_missing_hosts(self) -> None:
        with pytest.raises(ValueError, match="at least one sentinel host"):
            factory.parse_sentinel_url("redis+sentinel:///mymaster")


class TestValidateUrl:
    def test_accepts_supported(self) -> None:
        factory.validate_url("redis://localhost:6379/")
        factory.validate_url("redis+sentinel://h:26379/m")
        factory.validate_url("rediss+sentinel://h1,h2:26380/m/0?ssl=true")

    def test_rejects_cluster(self) -> None:
        with pytest.raises(factory.UnsupportedRedisModeError):
            factory.validate_url("redis+cluster://node:6379/0")

    def test_rejects_cluster_query(self) -> None:
        with pytest.raises(factory.UnsupportedRedisModeError):
            factory.validate_url("redis://node:6379/?cluster=true")

    def test_rejects_malformed_sentinel(self) -> None:
        # detect_mode passes (scheme is sentinel), but parse_sentinel_url
        # surfaces the missing master at config-build time.
        with pytest.raises(ValueError, match="master/service name"):
            factory.validate_url("redis+sentinel://h:26379/")


class TestRQOrchestratorConfigRejectsCluster:
    """End-to-end: cluster URLs raise during RQOrchestratorConfig(...)."""

    def test_cluster_scheme_rejected_at_config_build(self) -> None:
        from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestratorConfig

        with pytest.raises(Exception, match="Cluster"):
            RQOrchestratorConfig(redis_url="redis+cluster://node:6379/0")

    def test_cluster_query_rejected_at_config_build(self) -> None:
        from docling_jobkit.orchestrators.rq.orchestrator import RQOrchestratorConfig

        with pytest.raises(Exception, match=r"cluster"):
            RQOrchestratorConfig(redis_url="redis://node:6379/?cluster=true")


class TestRayOrchestratorConfigRejectsCluster:
    def test_cluster_scheme_rejected_at_config_build(self) -> None:
        ray_config = pytest.importorskip(
            "docling_jobkit.orchestrators.ray.config"
        )

        with pytest.raises(Exception, match="Cluster"):
            ray_config.RayOrchestratorConfig(redis_url="redis+cluster://node:6379/0")

    def test_cluster_query_rejected_at_config_build(self) -> None:
        ray_config = pytest.importorskip(
            "docling_jobkit.orchestrators.ray.config"
        )

        with pytest.raises(Exception, match=r"cluster"):
            ray_config.RayOrchestratorConfig(
                redis_url="redis://node:6379/?cluster=true"
            )

    def test_sentinel_url_accepted(self) -> None:
        ray_config = pytest.importorskip(
            "docling_jobkit.orchestrators.ray.config"
        )

        cfg = ray_config.RayOrchestratorConfig(
            redis_url="redis+sentinel://h1,h2:26380/mymaster/0?ssl=true",
        )
        assert cfg.redis_url.startswith("redis+sentinel://")
