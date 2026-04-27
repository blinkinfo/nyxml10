"""Polymarket CLOB client wrapper -- initialises the V2 client with API credentials."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import ApiCreds

if TYPE_CHECKING:
    import config as cfg

log = logging.getLogger(__name__)


class PolymarketClient:
    """Thin wrapper around *py-clob-client* that derives API creds on init."""

    def __init__(self, config: "cfg") -> None:  # type: ignore[type-arg]
        self.config = config
        log.info("Initialising ClobClient (host=%s, chain=%s) ...", config.CLOB_HOST, config.CHAIN_ID)

        # Step 1 — bare client (no creds yet)
        self.client = ClobClient(
            host=config.CLOB_HOST,
            key=config.POLYMARKET_PRIVATE_KEY,
            chain=config.CHAIN_ID,
            signature_type=config.POLYMARKET_SIGNATURE_TYPE,
            funder=config.POLYMARKET_FUNDER_ADDRESS,
        )

        # Step 2 -- derive / fetch L2 API creds
        creds = self.client.create_or_derive_api_key()
        log.debug("API creds derived successfully.")

        # Step 3 — re-instantiate with creds for authenticated L2 trading
        self.client = ClobClient(
            host=config.CLOB_HOST,
            key=config.POLYMARKET_PRIVATE_KEY,
            chain=config.CHAIN_ID,
            signature_type=config.POLYMARKET_SIGNATURE_TYPE,
            funder=config.POLYMARKET_FUNDER_ADDRESS,
            creds=ApiCreds(
                api_key=creds.api_key,
                api_secret=creds.api_secret,
                api_passphrase=creds.api_passphrase,
            ),
        )
        log.info("ClobClient ready with L2 credentials.")
