"""
Heartbeat loop for one agent profile.
State machine: setup -> join -> play -> settle -> repeat.
"""
import asyncio

from bot.api_client import MoltyAPI, APIError
from bot.dashboard.state import dashboard_state
from bot.game.free_join import join_free_game
from bot.game.paid_join import join_paid_game
from bot.game.room_selector import select_room
from bot.game.settlement import settle_game
from bot.game.websocket_engine import WebSocketEngine
from bot.memory.agent_memory import AgentMemory
from bot.setup.identity import ensure_identity
from bot.setup.wallet_setup import ensure_molty_wallet
from bot.setup.whitelist import ensure_whitelist
from bot.state_router import IN_GAME, NO_IDENTITY, READY_FREE, READY_PAID, determine_state
from bot.config import DASHBOARD_SHOW_PRIVATE_KEYS
from bot.utils.logger import get_logger

log = get_logger(__name__)

_OWNER_SETUP_LOCKS: dict[str, asyncio.Lock] = {}


def _get_owner_setup_lock(owner_eoa: str) -> asyncio.Lock:
    key = (owner_eoa or "").lower()
    lock = _OWNER_SETUP_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _OWNER_SETUP_LOCKS[key] = lock
    return lock


def _owner_label(owner_eoa: str) -> str:
    if not owner_eoa:
        return "-"
    return f"{owner_eoa[:8]}...{owner_eoa[-6:]}"


class Heartbeat:
    """Main loop for a single configured agent profile."""

    def __init__(self, profile: dict, profile_store=None):
        self.profile = profile
        self.profile_store = profile_store
        self.api: MoltyAPI | None = None
        self.memory = AgentMemory(profile.get("agent_key", "default"))
        self.running = True
        self._agent_key = profile.get("agent_key", "agent-1")
        self._agent_name = profile.get("agent_name", "Agent")

    def _save_profile(self, **fields):
        self.profile.update(fields)
        if self.profile_store:
            self.profile_store.update_profile(self.profile["agent_key"], **fields)

    def _propagate_owner_wallet(self, owner_eoa: str, wallet_addr: str):
        if not owner_eoa or not wallet_addr:
            return
        owner_lower = owner_eoa.lower()
        if self.profile_store:
            for profile in self.profile_store.profiles:
                if (profile.get("owner_eoa", "")).lower() == owner_lower:
                    profile["molty_royale_wallet"] = wallet_addr
                    self.profile_store.update_profile(
                        profile["agent_key"],
                        molty_royale_wallet=wallet_addr,
                    )
        for agent_id, agent in dashboard_state.agents.items():
            if (agent.get("owner_eoa", "")).lower() == owner_lower:
                dashboard_state.update_agent(agent_id, {
                    "molty_royale_wallet": wallet_addr,
                })

    def _known_owner_wallet(self, owner_eoa: str) -> str:
        if not owner_eoa:
            return ""
        owner_lower = owner_eoa.lower()
        if self.profile.get("molty_royale_wallet"):
            return self.profile["molty_royale_wallet"]
        if self.profile_store:
            for profile in self.profile_store.profiles:
                if (profile.get("owner_eoa", "")).lower() == owner_lower and profile.get("molty_royale_wallet"):
                    return profile["molty_royale_wallet"]
        for agent in dashboard_state.agents.values():
            if (agent.get("owner_eoa", "")).lower() == owner_lower and agent.get("molty_royale_wallet"):
                return agent["molty_royale_wallet"]
        return ""

    def _dashboard_private_key(self) -> str:
        return self.profile.get("agent_private_key", "")

    def _set_owner_setup_state(self, owner_eoa: str, **fields):
        dashboard_state.set_owner_setup(owner_eoa, fields)

    def _clear_owner_setup_state(self, owner_eoa: str):
        dashboard_state.clear_owner_setup(owner_eoa)

    @property
    def api_key(self) -> str:
        return self.profile.get("api_key", "")

    @property
    def agent_private_key(self) -> str:
        return self.profile.get("agent_private_key", "")

    @property
    def owner_private_key(self) -> str:
        return self.profile.get("owner_private_key", "")

    async def run(self):
        if not self.api_key:
            log.error("Agent %s has no API key; skipping start", self._agent_key)
            dashboard_state.update_agent(self._agent_key, {
                "name": self._agent_name,
                "status": "error",
                "last_action": "Missing API key",
            })
            return

        self.api = MoltyAPI(self.api_key)
        dashboard_state.add_log(f"Bot started: {self._agent_name}", "info", self._agent_key)
        dashboard_state.update_agent(self._agent_key, {
            "name": self._agent_name,
            "status": "idle",
            "agent_wallet_address": self.profile.get("agent_wallet_address", ""),
            "agent_private_key": self._dashboard_private_key(),
            "owner_eoa": self.profile.get("owner_eoa", ""),
            "molty_royale_wallet": self.profile.get("molty_royale_wallet", ""),
        })

        if self.profile.get("enable_memory", True):
            await self.memory.load()
            self.memory.set_agent_name(self._agent_name)

        consecutive_errors = 0
        while self.running:
            try:
                await self._heartbeat_cycle()
                consecutive_errors = 0
            except KeyboardInterrupt:
                self.running = False
            except Exception as exc:
                consecutive_errors += 1
                wait = min(10 * (2 ** min(consecutive_errors - 1, 4)), 120)
                log.error("Heartbeat error for %s (#%d): %s. Retrying in %ds...",
                          self._agent_name, consecutive_errors, exc, wait)
                dashboard_state.update_agent(self._agent_key, {
                    "status": "error",
                    "last_action": f"Error: {exc}",
                })
                await asyncio.sleep(wait)

        if self.api:
            await self.api.close()

    async def _heartbeat_cycle(self):
        try:
            me = await self.api.get_accounts_me()
        except APIError as exc:
            if exc.status == 401:
                dashboard_state.update_agent(self._agent_key, {
                    "status": "error",
                    "last_action": "Invalid API key",
                })
                self.running = False
                return
            raise

        state, ctx = determine_state(me)
        self._agent_name = me.get("agentName", me.get("name", self._agent_name))
        balance = me.get("balance", 0)
        readiness = me.get("readiness", {}) if isinstance(me.get("readiness"), dict) else {}
        sc_wallet = readiness.get("scWallet") or self._known_owner_wallet(self.profile.get("owner_eoa", ""))
        if sc_wallet and sc_wallet != self.profile.get("molty_royale_wallet", ""):
            self._save_profile(molty_royale_wallet=sc_wallet)
            self._propagate_owner_wallet(self.profile.get("owner_eoa", ""), sc_wallet)
        whitelist_approved = bool(readiness.get("whitelistApproved", False))
        identity_registered = readiness.get("erc8004Id") is not None
        dashboard_state.update_agent(self._agent_key, {
            "name": self._agent_name,
            "status": "playing" if state == IN_GAME else "idle",
            "smoltz": balance,
            "whitelisted": whitelist_approved,
            "identity_registered": identity_registered,
            "erc8004_token_id": readiness.get("erc8004Id"),
            "remote_agent_id": me.get("agentId", ""),
            "agent_wallet_address": self.profile.get("agent_wallet_address", ""),
            "agent_private_key": self._dashboard_private_key(),
            "owner_eoa": self.profile.get("owner_eoa", ""),
            "molty_royale_wallet": sc_wallet or self.profile.get("molty_royale_wallet", ""),
        })

        if state == NO_IDENTITY:
            await self._handle_no_identity()
            return
        if state == IN_GAME:
            await self._handle_in_game(ctx)
            return
        if state in (READY_FREE, READY_PAID):
            await self._handle_ready(me)

    async def _handle_no_identity(self):
        owner_eoa = self.profile.get("owner_eoa", "")
        agent_eoa = self.profile.get("agent_wallet_address", "")
        if not owner_eoa:
            dashboard_state.update_agent(self._agent_key, {
                "status": "error",
                "last_action": "Missing owner EOA",
            })
            await asyncio.sleep(30)
            return

        owner_lock = _get_owner_setup_lock(owner_eoa)
        if owner_lock.locked():
            shared = dashboard_state.owner_setup.get(owner_eoa.lower(), {})
            holder_name = shared.get("holder_name", "another agent")
            step = shared.get("step", "setup")
            dashboard_state.add_log(
                f"Waiting for shared-owner setup: holder={holder_name} step={step}",
                "info",
                self._agent_key,
            )
            dashboard_state.update_agent(self._agent_key, {
                "status": "idle",
                "last_action": f"Queued for shared-owner setup. Waiting for {holder_name} ({step})",
                "shared_owner_waiting_for": holder_name,
                "shared_owner_step": step,
            })

        wait_after = 0
        async with owner_lock:
            dashboard_state.add_log(
                f"Acquired shared-owner setup lock for owner {_owner_label(owner_eoa)}",
                "info",
                self._agent_key,
            )
            self._set_owner_setup_state(
                owner_eoa,
                owner=_owner_label(owner_eoa),
                holder=self._agent_key,
                holder_name=self._agent_name,
                step="owner setup started",
            )
            dashboard_state.update_agent(self._agent_key, {
                "status": "idle",
                "last_action": "Running owner setup",
                "shared_owner_waiting_for": "",
                "shared_owner_step": "owner setup started",
            })

            if self.profile.get("auto_sc_wallet", True):
                known_wallet = self._known_owner_wallet(owner_eoa)
                if known_wallet:
                    self._save_profile(molty_royale_wallet=known_wallet)
                    self._propagate_owner_wallet(owner_eoa, known_wallet)
                    dashboard_state.update_agent(self._agent_key, {
                        "last_action": "Running owner setup: shared wallet already known",
                        "shared_owner_step": "shared wallet already known",
                    })
                else:
                    self._set_owner_setup_state(
                        owner_eoa,
                        step="wallet setup",
                    )
                    dashboard_state.update_agent(self._agent_key, {
                        "last_action": "Running owner setup: wallet setup",
                        "shared_owner_step": "wallet setup",
                    })
                    wallet_addr = await ensure_molty_wallet(
                        self.api,
                        owner_eoa,
                        profile=self.profile,
                        save_profile=self._save_profile,
                    )
                    if not wallet_addr:
                        wait_after = 30
                    else:
                        self.profile["molty_royale_wallet"] = wallet_addr
                        self._propagate_owner_wallet(owner_eoa, wallet_addr)

            if wait_after == 0 and self.profile.get("auto_whitelist", True):
                self._set_owner_setup_state(
                    owner_eoa,
                    step="whitelist request + approval",
                )
                dashboard_state.update_agent(self._agent_key, {
                    "last_action": "Running owner setup: whitelist request + approval",
                    "shared_owner_step": "whitelist request + approval",
                })
                ok = await ensure_whitelist(
                    self.api,
                    owner_eoa,
                    agent_eoa,
                    owner_private_key=self.owner_private_key,
                    advanced_mode=self.profile.get("advanced_mode", True),
                )
                if not ok:
                    wait_after = 120

            if wait_after == 0 and self.profile.get("auto_identity", True):
                self._set_owner_setup_state(
                    owner_eoa,
                    step="identity registration",
                )
                dashboard_state.update_agent(self._agent_key, {
                    "last_action": "Running owner setup: identity registration",
                    "shared_owner_step": "identity registration",
                })
                ok = await ensure_identity(
                    self.api,
                    owner_private_key=self.owner_private_key,
                    profile=self.profile,
                    save_profile=self._save_profile,
                    advanced_mode=self.profile.get("advanced_mode", True),
                )
                if not ok:
                    wait_after = 30

            if wait_after:
                self._set_owner_setup_state(
                    owner_eoa,
                    step=f"retry scheduled ({wait_after}s)",
                )
                dashboard_state.update_agent(self._agent_key, {
                    "last_action": f"Owner setup incomplete. Retrying in {wait_after}s",
                    "shared_owner_step": f"retry scheduled ({wait_after}s)",
                })
            else:
                self._set_owner_setup_state(
                    owner_eoa,
                    step="owner setup completed",
                )
                dashboard_state.update_agent(self._agent_key, {
                    "last_action": "Owner setup completed",
                    "shared_owner_step": "owner setup completed",
                })

        dashboard_state.add_log(
            f"Released shared-owner setup lock for owner {_owner_label(owner_eoa)}",
            "info",
            self._agent_key,
        )
        self._clear_owner_setup_state(owner_eoa)

        if wait_after:
            await asyncio.sleep(wait_after)

    async def _handle_ready(self, me: dict):
        room_type = self.profile.get("room_mode") or select_room(me)
        try:
            if room_type == "paid":
                game_id, agent_id = await join_paid_game(self.api, self.agent_private_key)
            else:
                game_id, agent_id = await join_free_game(self.api)
        except (APIError, RuntimeError) as exc:
            dashboard_state.add_log(f"Join failed: {exc}", "warning", self._agent_key)
            await asyncio.sleep(10)
            return
        await self._play_game(game_id, agent_id, room_type)

    async def _handle_in_game(self, ctx: dict):
        await self._play_game(ctx["game_id"], ctx["agent_id"], ctx.get("entry_type", "free"))

    async def _play_game(self, game_id: str, agent_id: str, entry_type: str):
        dashboard_state.update_agent(self._agent_key, {
            "status": "playing",
            "room_id": game_id,
            "room_name": f"{entry_type} room",
        })
        dashboard_state.add_log(f"Joined {entry_type} game: {game_id[:12]}", "info", self._agent_key)

        self.memory.set_temp_game(game_id)
        await self.memory.save()

        engine = WebSocketEngine(game_id, agent_id, self.api_key)
        engine.dashboard_key = self._agent_key
        engine.dashboard_name = self._agent_name
        game_result = await engine.run()
        await settle_game(game_result, entry_type, self.memory)

        result = game_result.get("result", game_result)
        rewards = result.get("rewards", {})
        dashboard_state.update_agent(self._agent_key, {
            "wins": self.memory.data["overall"]["history"]["wins"],
            "moltz": rewards.get("moltz", self.profile.get("moltz", 0)),
            "smoltz": rewards.get("sMoltz", self.profile.get("smoltz", 0)),
        })
        await asyncio.sleep(5)
