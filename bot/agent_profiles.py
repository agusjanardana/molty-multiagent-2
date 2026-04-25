"""
Agent profile loading, bootstrap, and persistence for multi-agent runtime.
"""
from __future__ import annotations

import asyncio
import json
import re
import gzip
import base64
from pathlib import Path

from bot.api_client import APIError, MoltyAPI
from bot.config import (
    ACCOUNTS_B64_GZIP,
    ACCOUNTS_JSON,
    ACCOUNT_BOOTSTRAP_DELAY_SECONDS,
    ADVANCED_MODE,
    AGENT_BOOTSTRAP_COUNT,
    AGENT_NAME_PREFIX,
    AUTO_IDENTITY,
    AUTO_SC_WALLET,
    AUTO_WHITELIST,
    DEV_AGENT_DIR,
    ENABLE_MEMORY,
    ROOM_MODE,
    SHARED_OWNER_WALLET,
)
from bot.dashboard.state import dashboard_state
from bot.credentials import load_agent_wallet, load_credentials, load_owner_wallet
from bot.utils.logger import get_logger
from bot.utils.railway_sync import is_railway, sync_profiles_to_railway
from bot.web3.wallet_manager import generate_agent_wallet, generate_owner_wallet

log = get_logger(__name__)

ACCOUNTS_FILE = DEV_AGENT_DIR / "accounts.json"


def _slugify(value: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return base or "agent"


def _normalize_profile(raw: dict, index: int) -> dict:
    profile = dict(raw or {})
    profile["agent_name"] = profile.get("agent_name") or profile.get("name") or f"Agent {index + 1}"
    profile["agent_key"] = profile.get("agent_key") or f"{_slugify(profile['agent_name'])}-{index + 1}"
    profile["api_key"] = profile.get("api_key", "")
    profile["agent_wallet_address"] = profile.get("agent_wallet_address", "")
    profile["agent_private_key"] = profile.get("agent_private_key", "")
    profile["owner_eoa"] = profile.get("owner_eoa", "")
    profile["owner_private_key"] = profile.get("owner_private_key", "")
    profile["room_mode"] = profile.get("room_mode", ROOM_MODE)
    profile["advanced_mode"] = bool(profile.get("advanced_mode", ADVANCED_MODE))
    profile["auto_whitelist"] = bool(profile.get("auto_whitelist", AUTO_WHITELIST))
    profile["auto_sc_wallet"] = bool(profile.get("auto_sc_wallet", AUTO_SC_WALLET))
    profile["auto_identity"] = bool(profile.get("auto_identity", AUTO_IDENTITY))
    profile["enable_memory"] = bool(profile.get("enable_memory", ENABLE_MEMORY))
    profile["molty_royale_wallet"] = profile.get("molty_royale_wallet", "")
    profile["erc8004_token_id"] = profile.get("erc8004_token_id")
    return profile


def _parse_accounts_json(raw: str) -> list[dict]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("Failed to parse ACCOUNTS_JSON: %s", exc)
        return []
    if isinstance(data, dict):
        data = data.get("accounts", [])
    if not isinstance(data, list):
        return []
    return [_normalize_profile(item, idx) for idx, item in enumerate(data) if isinstance(item, dict)]


def _parse_accounts_b64_gzip(raw: str) -> list[dict]:
    if not raw:
        return []
    try:
        compressed = base64.b64decode(raw)
        payload = gzip.decompress(compressed).decode("utf-8")
    except Exception as exc:
        log.warning("Failed to decode ACCOUNTS_B64_GZIP: %s", exc)
        return []
    return _parse_accounts_json(payload)


def _read_accounts_file() -> list[dict]:
    if not ACCOUNTS_FILE.exists():
        return []
    try:
        raw = json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Failed to read %s: %s", ACCOUNTS_FILE, exc)
        return []
    if isinstance(raw, dict):
        raw = raw.get("accounts", [])
    if not isinstance(raw, list):
        return []
    return [_normalize_profile(item, idx) for idx, item in enumerate(raw) if isinstance(item, dict)]


def _load_legacy_profile() -> list[dict]:
    creds = load_credentials() or {}
    agent_wallet = load_agent_wallet() or {}
    owner_wallet = load_owner_wallet() or {}
    api_key = creds.get("api_key", "")
    if not api_key:
        return []

    profile = _normalize_profile({
        "agent_name": creds.get("agent_name", "Agent"),
        "api_key": api_key,
        "agent_wallet_address": creds.get("agent_wallet_address", "") or agent_wallet.get("address", ""),
        "agent_private_key": agent_wallet.get("privateKey", ""),
        "owner_eoa": creds.get("owner_eoa", "") or owner_wallet.get("address", ""),
        "owner_private_key": owner_wallet.get("privateKey", ""),
        "molty_royale_wallet": creds.get("molty_royale_wallet", ""),
        "erc8004_token_id": creds.get("erc8004_token_id"),
    }, 0)
    return [profile]


class AgentProfileStore:
    """Persistent profile store used by the multi-agent runtime."""

    def __init__(self, profiles: list[dict], path: Path | None):
        self.profiles = profiles
        self.path = path

    def save(self):
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps({"accounts": self.profiles}, indent=2), encoding="utf-8")

    def update_profile(self, agent_key: str, **fields):
        for profile in self.profiles:
            if profile.get("agent_key") == agent_key:
                profile.update(fields)
                self.save()
                if is_railway():
                    try:
                        loop = asyncio.get_running_loop()
                    except RuntimeError:
                        loop = None
                    if loop and not loop.is_closed():
                        loop.create_task(sync_profiles_to_railway(self.profiles))
                return


async def bootstrap_profiles_if_needed() -> AgentProfileStore:
    compressed_profiles = _parse_accounts_b64_gzip(ACCOUNTS_B64_GZIP)
    if compressed_profiles:
        log.info("Loaded %d agent profile(s) from ACCOUNTS_B64_GZIP", len(compressed_profiles))
        store = AgentProfileStore(compressed_profiles, ACCOUNTS_FILE)
        store.save()
        return store

    env_profiles = _parse_accounts_json(ACCOUNTS_JSON)
    if env_profiles:
        log.info("Loaded %d agent profile(s) from ACCOUNTS_JSON", len(env_profiles))
        store = AgentProfileStore(env_profiles, ACCOUNTS_FILE)
        store.save()
        return store

    file_profiles = _read_accounts_file()
    if file_profiles:
        log.info("Loaded %d agent profile(s) from %s", len(file_profiles), ACCOUNTS_FILE)
        if AGENT_BOOTSTRAP_COUNT > len(file_profiles):
            profiles = await _bootstrap_profiles(AGENT_BOOTSTRAP_COUNT, existing_profiles=file_profiles)
            store = AgentProfileStore(profiles, ACCOUNTS_FILE)
            store.save()
            if is_railway():
                await sync_profiles_to_railway(profiles)
            return store
        return AgentProfileStore(file_profiles, ACCOUNTS_FILE)

    if AGENT_BOOTSTRAP_COUNT > 0:
        profiles = await _bootstrap_profiles(AGENT_BOOTSTRAP_COUNT, existing_profiles=[])
        store = AgentProfileStore(profiles, ACCOUNTS_FILE)
        store.save()
        if is_railway():
            await sync_profiles_to_railway(profiles)
        return store

    legacy_profiles = _load_legacy_profile()
    if legacy_profiles:
        log.info("Using legacy single-agent credentials")
        return AgentProfileStore(legacy_profiles, None)

    log.info("No preconfigured agent profiles found")
    return AgentProfileStore([], ACCOUNTS_FILE)


async def _bootstrap_profiles(count: int, existing_profiles: list[dict] | None = None) -> list[dict]:
    log.info("Bootstrapping %d agent account(s)...", count)
    profiles: list[dict] = list(existing_profiles or [])
    dashboard_state.set_setup_status(
        active=True,
        message="Your account is being setup.",
        current=len(profiles),
        total=count,
    )
    dashboard_state.add_log(f"Bootstrap setup started: {len(profiles)}/{count}", "info")
    shared_owner_eoa = ""
    shared_owner_pk = ""
    if ADVANCED_MODE and SHARED_OWNER_WALLET and profiles:
        shared_owner_eoa = profiles[0].get("owner_eoa", "")
        shared_owner_pk = profiles[0].get("owner_private_key", "")
    elif ADVANCED_MODE and SHARED_OWNER_WALLET:
        shared_owner_eoa, shared_owner_pk = generate_owner_wallet()

    for idx in range(len(profiles), count):
        agent_name = f"{AGENT_NAME_PREFIX}-{idx + 1}"
        agent_address, agent_pk = generate_agent_wallet()

        owner_eoa = ""
        owner_pk = ""
        if ADVANCED_MODE:
            if SHARED_OWNER_WALLET:
                owner_eoa, owner_pk = shared_owner_eoa, shared_owner_pk
            else:
                owner_eoa, owner_pk = generate_owner_wallet()

        result = None
        while result is None:
            api = MoltyAPI()
            try:
                result = await api.create_account(agent_name, agent_address)
            except APIError as exc:
                await api.close()
                if exc.code == "RATE_LIMITED" or exc.status == 429:
                    wait = max(ACCOUNT_BOOTSTRAP_DELAY_SECONDS, 15)
                    msg = f"Rate limited while creating {agent_name}. Retrying in {wait}s..."
                    log.warning(msg)
                    dashboard_state.set_setup_status(
                        active=True,
                        message=msg,
                        current=len(profiles),
                        total=count,
                    )
                    dashboard_state.add_log(msg, "warning")
                    await asyncio.sleep(wait)
                    continue
                msg = f"Bootstrap failed for {agent_name}: {exc}"
                dashboard_state.set_setup_status(
                    active=True,
                    message="Your account is being setup.",
                    current=len(profiles),
                    total=count,
                    error=msg,
                )
                raise RuntimeError(msg) from exc
            else:
                await api.close()

        api_key = result.get("apiKey", "")
        if not api_key:
            raise RuntimeError(f"Bootstrap failed for {agent_name}: no apiKey returned")

        profiles.append(_normalize_profile({
            "agent_name": agent_name,
            "api_key": api_key,
            "account_id": result.get("accountId", ""),
            "public_id": result.get("publicId", ""),
            "agent_wallet_address": agent_address,
            "agent_private_key": agent_pk,
            "owner_eoa": owner_eoa,
            "owner_private_key": owner_pk,
            "room_mode": ROOM_MODE,
            "advanced_mode": ADVANCED_MODE,
            "auto_whitelist": AUTO_WHITELIST,
            "auto_sc_wallet": AUTO_SC_WALLET,
            "auto_identity": AUTO_IDENTITY,
            "enable_memory": ENABLE_MEMORY,
        }, idx))
        log.info("Created account %s (%d/%d)", agent_name, idx + 1, count)
        dashboard_state.set_setup_status(
            active=True,
            message="Your account is being setup.",
            current=len(profiles),
            total=count,
        )
        dashboard_state.add_log(f"Created account {agent_name} ({len(profiles)}/{count})", "info")
        ACCOUNTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        ACCOUNTS_FILE.write_text(json.dumps({"accounts": profiles}, indent=2), encoding="utf-8")
        if idx + 1 < count and ACCOUNT_BOOTSTRAP_DELAY_SECONDS > 0:
            await asyncio.sleep(ACCOUNT_BOOTSTRAP_DELAY_SECONDS)

    dashboard_state.set_setup_status(
        active=False,
        message="Account setup complete.",
        current=len(profiles),
        total=count,
    )
    return profiles


def serialize_profiles_compact(profiles: list[dict]) -> tuple[str, str]:
    """Return both minified JSON and gzip+base64 payloads for Railway persistence."""
    compact_profiles = []
    for profile in profiles:
        compact_profiles.append({
            "agent_name": profile.get("agent_name", ""),
            "api_key": profile.get("api_key", ""),
            "agent_wallet_address": profile.get("agent_wallet_address", ""),
            "agent_private_key": profile.get("agent_private_key", ""),
            "owner_eoa": profile.get("owner_eoa", ""),
            "owner_private_key": profile.get("owner_private_key", ""),
            "room_mode": profile.get("room_mode", ROOM_MODE),
        })
    payload = json.dumps({"accounts": compact_profiles}, separators=(",", ":"))
    encoded = base64.b64encode(gzip.compress(payload.encode("utf-8"))).decode("ascii")
    return payload, encoded
