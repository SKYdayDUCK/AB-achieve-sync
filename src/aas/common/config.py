"""Configurations are read from explicit local files, never downloaded from A."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import math
import yaml
from .core import ProtocolError, validate_id


@dataclass(frozen=True)
class AConfig:
    producer_id: str
    consumer_id: str
    root: Path
    input_dir: Path
    crypto_scheme: str = "none"
    recipients: tuple[str, ...] = ()
    max_files: int = 100
    max_bytes: int = 1073741824
    max_wait_seconds: float = 600.0
    min_age_seconds: float = 3.0
    max_batch_bytes: int = 4294967296
    critical_free_bytes: int = 0
    warn_free_bytes: int = 0
    skip_open_files: bool = True
    hold_back_newest: int = 1          # never pack the newest N image(s): a sequential writer may still be on them
    idle_flush_factor: float = 4.0     # no new image for factor x (typical inter-image gap) => producer is done
    idle_flush_min_seconds: float = 10.0
    idle_flush_max_seconds: float = 300.0  # also the threshold until >=3 gaps have been observed
    emergency_enabled: bool = True
    emergency_idle_seconds: int = 60
    emergency_max_seconds: int = 1800

    @classmethod
    def load(cls, path: str | Path) -> AConfig:
        data = load_yaml(path)
        node, paths, batch, crypto, cap, em = (data.get(k, {}) for k in ("node", "paths", "batch", "crypto", "capacity", "emergency"))
        root = Path(paths["root_dir"]).expanduser().resolve()
        conf = cls(
            producer_id=validate_id(node["producer_id"]),
            consumer_id=validate_id(node["consumer_id"]),
            root=root, input_dir=Path(paths["input_dir"]).expanduser().resolve(),
            crypto_scheme=crypto.get("strategy", "none"),
            recipients=tuple(crypto.get("recipients", [])),
            max_files=int(batch.get("max_files", 100)),
            max_bytes=int(batch.get("max_bytes", 1073741824)),
            max_wait_seconds=float(batch.get("max_wait_seconds", 600)),
            max_batch_bytes=int(batch.get("max_batch_bytes", 4294967296)),
            min_age_seconds=float(batch.get("min_age_seconds", 3)),
            critical_free_bytes=int(cap.get("critical_free_bytes", 0)),
            warn_free_bytes=int(cap.get("warn_free_bytes", 0)),
            skip_open_files=bool(data.get("watch", {}).get("skip_open_files", True)),
            hold_back_newest=int(data.get("watch", {}).get("hold_back_newest", 1)),
            idle_flush_factor=float(data.get("watch", {}).get("idle_flush_factor", 4.0)),
            idle_flush_min_seconds=float(data.get("watch", {}).get("idle_flush_min_seconds", 10.0)),
            idle_flush_max_seconds=float(data.get("watch", {}).get("idle_flush_max_seconds", 300.0)),
            emergency_enabled=em.get("enabled", True),
            emergency_idle_seconds=int(em.get("idle_seconds", 60)),
            emergency_max_seconds=int(em.get("max_session_seconds", 1800)),
        )
        if any((conf.max_files <= 0, conf.max_bytes <= 0, conf.max_batch_bytes <= 0, conf.max_wait_seconds < 0, conf.min_age_seconds < 0, conf.critical_free_bytes < 0, conf.warn_free_bytes < 0)):
            raise ProtocolError("invalid A numeric configuration")
        if conf.hold_back_newest < 0 or not 0 < conf.idle_flush_min_seconds <= conf.idle_flush_max_seconds or conf.idle_flush_factor < 1:
            raise ProtocolError("invalid watch.* configuration")
        if crypto.get("strategy", "none") not in ("none", "age_x25519"):
            raise ProtocolError("unsupported crypto strategy")
        if crypto.get("strategy", "none") == "age_x25519" and not conf.recipients:
            raise ProtocolError("age requires B recipient public keys")
        if data.get("archive", {}).get("strategy", "tar") != "tar":
            raise ProtocolError("MVP implements audited tar only")
        for section, field, correct in (("archive", "verify_final_archive", True), ("archive", "delete_source_after_verified_publish", True), ("ack", "delete_bundle_only_after_ack", True), ("capacity", "auto_pause_generator", False), ("capacity", "auto_delete_unacked", False)):
            if field in data.get(section, {}) and data[section][field] != correct:
                raise ProtocolError(f"unsafe override {section}.{field}")
        if data.get("ack", {}).get("ack_grace_seconds", 0) != 0:
            raise ProtocolError("this release supports ack_grace_seconds=0 only")
        return conf


@dataclass(frozen=True)
class BConfig:
    consumer_id: str
    producer_id: str
    root: Path
    remote_host: str
    remote_user: str
    remote_port: int = 22
    identity_file: str | None = None
    emergency_identity_file: str | None = None
    known_hosts_file: str | None = None
    address_family: str = "any"
    crypto_scheme: str = "none"
    identity_keys: tuple[str, ...] = ()
    interval_seconds: float = 300.0
    immediate_recheck: bool = True
    remote_query_timeout_seconds: int = 30
    reconcile_seconds: float = 1800.0
    retry_initial_seconds: float = 10.0
    retry_max_seconds: float = 300.0
    retry_jitter: bool = True
    wake_mode: str = "none"
    wake_timeout_seconds: float = 55.0
    max_workers: int = 2
    chunk_bytes: int = 8388608
    min_free_bytes: int = 0

    @classmethod
    def load(cls, path: str | Path) -> BConfig:
        data = load_yaml(path)
        scheduler = data.get("scheduler", {})
        forbidden = {"mode", "min_poll_seconds", "max_poll_seconds", "poll_factor"} & scheduler.keys()
        if forbidden:
            raise ProtocolError(f"obsolete adaptive scheduler fields: {sorted(forbidden)}")
        if scheduler.get("strategy", "fixed_interval") != "fixed_interval" or scheduler.get("experimental", {}).get("adaptive_enabled", False):
            raise ProtocolError("only production fixed_interval policy is registered")
        fixed = scheduler.get("fixed_interval", {})
        if fixed.get("immediate_scan_on_start", True) is not True or fixed.get("single_flight_discovery", True) is not True:
            raise ProtocolError("disabling safe fixed poll semantics is forbidden")
        if fixed.get("max_immediate_rechecks", 1) not in (0, 1):
            raise ProtocolError("at most one immediate recheck is permitted")
        node, remote, paths, trans, crypto = (data.get(k, {}) for k in ("node", "remote", "paths", "transfer", "crypto"))
        if remote.get("transport", "ssh_aasctl") != "ssh_aasctl" or remote.get("host_key_checking", "strict") != "strict":
            raise ProtocolError("only strict SSH host authentication supported")
        conf = cls(
            producer_id=validate_id(node["producer_id"]), consumer_id=validate_id(node["consumer_id"]),
            root=Path(paths["root_dir"]).expanduser().resolve(),
            remote_host=str(remote["host"]), remote_user=str(remote["user"]), remote_port=int(remote.get("port", 22)),
            identity_file=remote.get("identity_file"), emergency_identity_file=remote.get("emergency_identity_file"), known_hosts_file=remote.get("known_hosts_file"), address_family=str(remote.get("address_family", "any")),
            crypto_scheme=crypto.get("strategy", "none"), identity_keys=tuple(crypto.get("identity_keys", [])),
            interval_seconds=float(fixed.get("interval_seconds", 300)),
            immediate_recheck=bool(fixed.get("immediate_recheck_on_new_ready", True)) and fixed.get("max_immediate_rechecks", 1) == 1,
            remote_query_timeout_seconds=int(scheduler.get("remote_query_timeout_seconds", 30)),
            reconcile_seconds=float(scheduler.get("reconcile_interval_minutes", 30)) * 60,
            retry_initial_seconds=float(scheduler.get("network_retry", {}).get("initial_seconds", 10)),
            retry_max_seconds=float(scheduler.get("network_retry", {}).get("max_seconds", 300)),
            retry_jitter=bool(scheduler.get("network_retry", {}).get("jitter", True)),
            wake_mode=str(scheduler.get("wake", {}).get("mode", "none")),
            wake_timeout_seconds=float(scheduler.get("wake", {}).get("timeout_seconds", 55)),
            max_workers=int(trans.get("max_parallel_downloads", 2)), chunk_bytes=int(trans.get("chunk_bytes", 8388608)),
            min_free_bytes=int(data.get("capacity", {}).get("min_free_bytes", 0)),
        )
        if any((conf.interval_seconds <= 0, conf.remote_query_timeout_seconds <= 0, conf.reconcile_seconds <= 0, conf.retry_initial_seconds <= 0, conf.retry_max_seconds < conf.retry_initial_seconds, conf.max_workers < 1, conf.chunk_bytes < 1, conf.chunk_bytes > 16777216, conf.min_free_bytes < 0, conf.remote_port < 1 or conf.remote_port > 65535)):
            raise ProtocolError("invalid B numeric configuration")
        if conf.wake_mode not in ("none", "long_poll") or not 5 <= conf.wake_timeout_seconds <= 110:
            raise ProtocolError("scheduler.wake.mode must be none|long_poll with timeout_seconds in [5,110]")
        if conf.address_family not in ("any", "inet", "inet6"):
            raise ProtocolError("remote.address_family must be any|inet|inet6")
        if not conf.remote_host or not conf.remote_user or any(c.isspace() for c in conf.remote_host+conf.remote_user):
            raise ProtocolError("invalid SSH host / user")
        if conf.crypto_scheme not in ("none", "age_x25519") or (conf.crypto_scheme == "age_x25519" and not conf.identity_keys):
            raise ProtocolError("bad crypto scheme / missing private keys")
        if trans.get("durable_queue", True) is not True or trans.get("verify_full_bundle_sha256", True) is not True:
            raise ProtocolError("cannot disable durable queue or full hash")
        return conf


def load_yaml(path: str | Path) -> dict:
    with Path(path).open("rb") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ProtocolError("YAML config must be an object")
    return data
