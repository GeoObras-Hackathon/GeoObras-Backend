"""
infra/http_clients/tcerj_client.py
Cliente HTTP para a API de Dados Abertos do TCE-RJ.
"""

import logging
import time
from typing import Any

import httpx

from src.config.settings import get_settings

logger = logging.getLogger(__name__)
_settings = get_settings()


class TCERJClient:
    """Wrapper sobre os endpoints do TCE-RJ Dados Abertos."""

    BASE_URL = _settings.TCERJ_BASE_URL

    def __init__(self):
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            timeout=_settings.HTTP_TIMEOUT,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        for attempt in range(1, _settings.HTTP_MAX_RETRIES + 1):
            try:
                resp = self._client.get(path, params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "TCE-RJ HTTP %s em %s (tentativa %d): %s",
                    exc.response.status_code, path, attempt, exc,
                )
                if attempt == _settings.HTTP_MAX_RETRIES:
                    raise
                time.sleep(2 ** attempt)
            except httpx.RequestError as exc:
                logger.error("TCE-RJ erro de rede em %s: %s", path, exc)
                if attempt == _settings.HTTP_MAX_RETRIES:
                    raise
                time.sleep(2 ** attempt)

    # ------------------------------------------------------------------
    # Endpoints públicos
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_list(data: Any) -> list[dict]:
        """Normaliza resposta da API: aceita lista direta ou dict com chave 'Obras'."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("Obras") or data.get("obras") or data.get("data") or []
        return []

    def get_obras(self) -> list[dict]:
        """Pagina /obras_tce até receber lista vazia."""
        all_obras: list[dict] = []
        inicio = 0
        limite = _settings.TCERJ_PAGE_SIZE

        while True:
            data = self._get("/obras_tce", {"inicio": inicio, "limite": limite, "jsonfull": True})
            obras = self._extract_list(data)

            if not obras:
                logger.info("TCE-RJ obras_tce: fim no offset %d", inicio)
                break

            logger.info("TCE-RJ obras_tce: offset=%d → %d registros", inicio, len(obras))
            all_obras.extend(obras)
            inicio += limite

        return all_obras

    def get_obras_paralisadas(self, ano: int) -> list[dict]:
        """Retorna obras paralisadas de um determinado ano."""
        logger.info("TCE-RJ obras_paralisadas: buscando ano %d", ano)
        try:
            data = self._get("/obras_paralisadas", {"ano": ano, "jsonfull": True})
            obras = self._extract_list(data)
            logger.info("TCE-RJ obras_paralisadas %d: %d registros", ano, len(obras))
            return obras
        except Exception as exc:
            logger.error("TCE-RJ: falha ao buscar paralisadas de %d: %s", ano, exc)
            return []

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
