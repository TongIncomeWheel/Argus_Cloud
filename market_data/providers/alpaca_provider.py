"""
Alpaca provider — Greeks enrichment for open option positions.
Source: Alpaca free-tier indicative feed (no broker dependency, free API account).
Provides: Delta, Gamma, Theta per contract.
Gracefully degrades to None if keys are not configured or Alpaca is unreachable.
"""
import logging
from typing import List

from ..models import OptionsContract
from ..config import ALPACA_API_KEY, ALPACA_SECRET_KEY

logger = logging.getLogger(__name__)


class AlpacaProvider:
    """
    Enriches OptionsContract objects with Greeks from Alpaca option snapshots.
    Only queries contracts for the user's actual open positions — not full chains.
    """

    def __init__(self):
        self._client = None
        self._available = False
        self._init_client()

    def _init_client(self) -> None:
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            logger.info(
                "Alpaca: API keys not configured — Greeks (Δ Γ Θ) will show as None. "
                "Add ALPACA_API_KEY and ALPACA_SECRET_KEY to .env to enable."
            )
            return

        try:
            from alpaca.data.historical.option import OptionHistoricalDataClient

            self._client = OptionHistoricalDataClient(
                api_key=ALPACA_API_KEY,
                secret_key=ALPACA_SECRET_KEY,
            )
            self._available = True
            logger.info("Alpaca: client initialised — Greeks available.")
        except ImportError:
            logger.warning(
                "Alpaca: alpaca-py not installed. "
                "Run: pip install alpaca-py"
            )
        except Exception as exc:
            logger.warning(f"Alpaca: client init failed — {exc}")

    @property
    def is_available(self) -> bool:
        return self._available

    def enrich_with_greeks(
        self, contracts: List[OptionsContract]
    ) -> List[OptionsContract]:
        """
        Fetch option snapshots from Alpaca for the given contracts and
        populate delta, gamma, theta in-place. Returns the same list.
        Contracts without a matching snapshot retain None Greeks.
        """
        if not self._available or not contracts:
            return contracts

        symbols = [c.contract_symbol for c in contracts if c.contract_symbol]
        if not symbols:
            return contracts

        try:
            from alpaca.data.requests import OptionSnapshotRequest

            req = OptionSnapshotRequest(symbol_or_symbols=symbols)
            snapshots = self._client.get_option_snapshot(req)

            for contract in contracts:
                snap = snapshots.get(contract.contract_symbol)
                if snap and snap.greeks:
                    contract.delta = snap.greeks.delta
                    contract.gamma = snap.greeks.gamma
                    contract.theta = snap.greeks.theta

        except Exception as exc:
            logger.warning(f"Alpaca: Greeks enrichment failed — {exc}")

        return contracts
