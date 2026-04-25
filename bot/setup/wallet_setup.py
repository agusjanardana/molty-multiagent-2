"""
MoltyRoyale wallet (SC wallet) setup.
Handles create/recover and persists back into the active agent profile when available.
"""
from bot.api_client import MoltyAPI, APIError
from bot.web3.whitelist_contract import get_molty_wallet_address
from bot.utils.logger import get_logger

log = get_logger(__name__)


async def ensure_molty_wallet(
    api: MoltyAPI,
    owner_eoa: str,
    profile: dict | None = None,
    save_profile=None,
) -> str:
    """Create or recover the owner SC wallet and return its address."""
    existing = (profile or {}).get("molty_royale_wallet", "")
    confirmed = bool((profile or {}).get("molty_royale_wallet_confirmed", False))
    if existing and confirmed:
        log.info("MoltyRoyale Wallet already known: %s", existing)
        return existing

    try:
        result = await api.create_wallet(owner_eoa)
        wallet_addr = result.get("walletAddress", "")
        log.info("MoltyRoyale Wallet created: %s", wallet_addr)
        if profile is not None:
            profile["molty_royale_wallet"] = wallet_addr
            profile["molty_royale_wallet_confirmed"] = True
        if save_profile:
            save_profile(molty_royale_wallet=wallet_addr, molty_royale_wallet_confirmed=True)
        return wallet_addr
    except APIError as exc:
        if exc.code in ("CONFLICT", "WALLET_ALREADY_EXISTS"):
            log.info("MoltyRoyale Wallet already exists, recovering address...")
            return await _recover_wallet_address(owner_eoa, profile, save_profile)
        if exc.code == "AGENT_EOA_EQUALS_OWNER_EOA":
            log.error("Agent EOA and Owner EOA are the same address")
            return ""
        log.error("Wallet creation failed: %s", exc)
        return ""
    except Exception as exc:
        log.error("Unexpected wallet setup error: %s", exc)
        return ""


async def _recover_wallet_address(owner_eoa: str, profile: dict | None, save_profile) -> str:
    try:
        wallet_addr = await get_molty_wallet_address(owner_eoa)
        if wallet_addr:
            log.info("Recovered MoltyRoyale Wallet: %s", wallet_addr)
            if profile is not None:
                profile["molty_royale_wallet"] = wallet_addr
                profile["molty_royale_wallet_confirmed"] = False
            if save_profile:
                save_profile(molty_royale_wallet=wallet_addr, molty_royale_wallet_confirmed=False)
            return wallet_addr
    except Exception as exc:
        log.warning("On-chain wallet recovery failed: %s", exc)

    log.warning(
        "Wallet exists but address could not be recovered. "
        "Check My Agent page at https://www.moltyroyale.com"
    )
    return ""
