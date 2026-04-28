"""Polymarket CLOB client wrapper -- initialises the V2 client with API credentials."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from py_clob_client_v2.client import ClobClient

if TYPE_CHECKING:
    import config as cfg
    from py_clob_client_v2.clob_types import ApiCreds

log = logging.getLogger(__name__)


class PolymarketClient:
    """Thin wrapper around *py-clob-client* with robust L2 credential bootstrap.

    For the current signer/funder setup we prefer deriving an existing API key
    first. The upstream V2 helper ``create_or_derive_api_key()`` does the reverse
    (create first, derive second), which produces a noisy POST /auth/api-key 400
    for wallets that should derive instead of creating a fresh key.
    """

    def __init__(self, config: "cfg") -> None:  # type: ignore[type-arg]
        self.config = config
        log.info("Initialising ClobClient (host=%s, chain=%s) ...", config.CLOB_HOST, config.CHAIN_ID)

        self.client = ClobClient(
            host=config.CLOB_HOST,
            key=config.POLYMARKET_PRIVATE_KEY,
            chain_id=config.CHAIN_ID,
            signature_type=config.POLYMARKET_SIGNATURE_TYPE,
            funder=config.POLYMARKET_FUNDER_ADDRESS,
        )

        creds = self._bootstrap_api_creds()
        self.client.set_api_creds(creds)
        log.info("ClobClient ready with L2 credentials.")

    def _bootstrap_api_creds(self) -> "ApiCreds":
        """Return authenticated API creds with derive-first fallback logic.

        Flow:
          1. Prefer ``derive_api_key()`` because this account mode commonly has
             an existing API key and Polymarket rejects redundant create attempts.
          2. Fall back to ``create_api_key()`` only when derivation fails.

        Any total failure is raised to the caller so startup can disable trading
        explicitly rather than leaving the client half-configured.
        """
        derive_error: Exception | None = None

        try:
            creds = self.client.derive_api_key()
            log.debug("Polymarket API creds derived successfully.")
            return creds
        except Exception as exc:
            derive_error = exc
            log.warning(
                "Polymarket API key derivation failed; falling back to creation.",
                exc_info=True,
            )

        try:
            creds = self.client.create_api_key()
            log.info("Polymarket API creds created successfully after derive failed.")
            return creds
        except Exception as create_error:
            log.error(
                "Polymarket API credential bootstrap failed: derive and create both failed."
            )
            if derive_error is not None:
                raise RuntimeError(
                    "Polymarket API credential bootstrap failed after derive-first fallback"
                ) from derive_error
            raise RuntimeError("Polymarket API credential bootstrap failed") from create_error
