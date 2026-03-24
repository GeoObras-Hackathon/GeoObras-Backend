"""
infra/http_clients/obrasgov_client.py
Cliente HTTP para a API ObrasGov.br.
Encapsula paginação, retries e parsing básico de resposta.
"""

import logging
import time
from typing import Any, Generator

import httpx

from src.config.settings import get_settings

logger = logging.getLogger(__name__)
_settings = get_settings()


class ObrasGovClient:
    """Wrapper sobre os endpoints da API ObrasGov.br."""

    BASE_URL = _settings.OBRASGOV_BASE_URL

    def __init__(self):
        self._client = httpx.Client(
            base_url=self.BASE_URL,
            timeout=_settings.HTTP_TIMEOUT,
        )

    # ------------------------------------------------------------------
    # Helpers internos
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        """GET com retry; aguarda mais tempo em caso de HTTP 429."""
        for attempt in range(1, _settings.HTTP_MAX_RETRIES + 1):
            try:
                resp = self._client.get(path, params=params)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                logger.error(
                    "HTTP %s em %s (tentativa %d/%d): %s",
                    status, path, attempt, _settings.HTTP_MAX_RETRIES, exc,
                )
                if attempt == _settings.HTTP_MAX_RETRIES:
                    raise
                # 429 = rate limit: espera muito mais que outros erros
                wait = int(exc.response.headers.get("Retry-After", 0))
                if not wait:
                    wait = 60 if status == 429 else 2 ** attempt
                logger.info("Aguardando %ds antes de tentar novamente…", wait)
                time.sleep(wait)
            except httpx.RequestError as exc:
                logger.error("Erro de rede em %s (tentativa %d): %s", path, attempt, exc)
                if attempt == _settings.HTTP_MAX_RETRIES:
                    raise
                time.sleep(2 ** attempt)

    def _paginate(self, path: str, base_params: dict[str, Any]) -> Generator[list[dict], None, None]:
        """Itera páginas até receber lista vazia; aguarda 1s entre páginas."""
        pagina = 1
        while True:
            params = {**base_params, "pagina": pagina, "tamanhoDaPagina": _settings.OBRASGOV_PAGE_SIZE}
            data = self._get(path, params)

            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                items = data.get("content") or data.get("data") or data.get("resultado") or []
            else:
                items = []

            if not items:
                logger.info("ObrasGov %s: fim na página %d", path, pagina)
                break

            logger.info("ObrasGov %s: página %d → %d registros", path, pagina, len(items))
            yield items
            pagina += 1
            time.sleep(1)  # evita rate limit (429)

    # ------------------------------------------------------------------
    # Endpoints públicos
    # ------------------------------------------------------------------

    def get_projetos_investimento(self, uf: str = "RJ") -> Generator[list[dict], None, None]:
        """Pagina /projeto-investimento filtrando por UF."""
        logger.info("ObrasGov: iniciando ingestão de projetos (UF=%s)", uf)
        yield from self._paginate("/projeto-investimento", {"uf": uf})

    def get_execucao_fisica(self, id_unico: str) -> list[dict]:
        """Retorna a lista de execuções físicas de um projeto."""
        path = "/execucao-fisica"
        all_items: list[dict] = []
        for page in self._paginate(path, {"idUnico": id_unico}):
            all_items.extend(page)
        return all_items

    def get_execucao_financeira(self, id_projeto: str) -> list[dict]:
        """Retorna empenhos de um projeto (execução financeira)."""
        all_items: list[dict] = []
        for page in self._paginate("/execucao-financeira", {"idProjetoInvestimento": id_projeto}):
            all_items.extend(page)
        return all_items

    def get_contratos(self, id_projeto: str) -> list[dict]:
        """Retorna contratos de um projeto."""
        all_items: list[dict] = []
        for page in self._paginate("/execucao-financeira/contrato", {"idProjetoInvestimento": id_projeto}):
            all_items.extend(page)
        return all_items

    def get_geometria(self, id_unico: str) -> list[dict]:
        """Retorna dados de georreferenciamento (WKT) de um projeto."""
        try:
            data = self._get("/geometria", {"idUnico": id_unico})
            if isinstance(data, list):
                return data
            return [data] if isinstance(data, dict) else []
        except Exception as exc:
            logger.warning("ObrasGov: falha ao buscar geometria de %s: %s", id_unico, exc)
            return []

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
