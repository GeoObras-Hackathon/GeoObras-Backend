# GeoObras Backend – Mês 1

Plataforma de monitoramento de obras públicas em Macaé/RJ.

## Stack
- **Python 3.11+** · FastAPI · SQLAlchemy Core · psycopg2
- **Postgres 16** (com PostGIS opcional, mas DDL compatível com Postgres puro)
- **httpx** para chamadas HTTP · **pandas** para leitura de CSV

---

## Estrutura de pastas

```
geoobras-backend/
├── sql/
│   └── 001_create_schemas.sql      # DDL completo (raw / clean / analytics)
├── src/
│   ├── config/settings.py          # Configurações (URLs, DSN, etc.)
│   ├── infra/
│   │   ├── db.py                   # Engine + SessionLocal
│   │   ├── http_clients/
│   │   │   ├── obrasgov_client.py
│   │   │   └── tcerj_client.py
│   │   └── repositories/
│   │       ├── raw_repository.py
│   │       ├── clean_repository.py
│   │       └── analytics_repository.py
│   ├── domain/
│   │   ├── models.py               # Pydantic domain models
│   │   └── enums.py
│   ├── services/
│   │   ├── ingestion_service.py    # RAW ingestão
│   │   ├── clean_service.py        # RAW → CLEAN
│   │   ├── geometry_service.py     # WKT → lat/lon
│   │   └── analytics_service.py   # CLEAN → Analytics
│   ├── etl/run_etl.py              # Orquestrador ETL
│   └── api/
│       ├── main.py                 # FastAPI app
│       └── schemas.py              # Schemas de resposta
├── data/input/macae_convenios/     # CSVs de convênios
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## Setup rápido

### 1. Pré-requisitos
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Banco de dados
```bash
# Via Docker (inclui PostGIS):
docker compose up -d db

# Ou Postgres local – crie o banco e aplique o DDL:
psql -U postgres -c "CREATE DATABASE geoobras;"
psql -U postgres -d geoobras -f sql/001_create_schemas.sql
```

### 3. Configuração
```bash
cp .env.example .env
# Edite DATABASE_URL e demais variáveis conforme seu ambiente
```

### 4. Executar API
```bash
uvicorn src.api.main:app --reload
# Acesse: http://localhost:8000/docs
```

### 5. Executar ETL
```bash
# ETL completo:
python -m src.etl.run_etl

# Só uma fonte:
python -m src.etl.run_etl --fonte obrasgov
python -m src.etl.run_etl --fonte tcerj
python -m src.etl.run_etl --fonte convenios

# Reprocessar CLEAN + Analytics sem reingerir RAW:
python -m src.etl.run_etl --skip-raw
```

### 6. Convênios CSV
Coloque arquivos `.csv` (encoding latin-1, separador `;` ou `,`) em:
```
data/input/macae_convenios/
```

### 7. Cron (sugestão)
```cron
# Executa ETL completo todo dia às 03h
0 3 * * * cd /app && /app/.venv/bin/python -m src.etl.run_etl >> /var/log/geoobras_etl.log 2>&1
```

---

## Endpoints da API

| Método | Path | Descrição |
|--------|------|-----------|
| GET | `/health` | Health check |
| GET | `/api/v1/obras` | Lista obras (filtros opcionais) |
| GET | `/api/v1/obras/{id}` | Detalhe completo de uma obra |
| GET | `/api/v1/estatisticas` | Métricas agregadas |
| POST | `/api/v1/refresh` | Registra intenção de ETL (stub) |

Documentação interativa: `http://localhost:8000/docs`

### Filtros disponíveis em `/api/v1/obras`

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `situacao` | string | `planejada`, `em_execucao`, `concluida`, `paralisada`, `cancelada`, `inacabada` |
| `municipio` | string | Filtro textual no nome do município |
| `apenas_com_coordenadas` | bool | Retorna só obras com lat/lon preenchidos |
| `apenas_inconsistencias` | bool | Retorna só obras com flags de dados pendentes/suspeitos |
| `valor_minimo` | float | Filtro por `valor_total_contratado` mínimo (R$) |
| `page` / `page_size` | int | Paginação |
| `eficiencia_minima` | float | **Não implementado** – reservado para Mês 2 |
| `risco` | string | **Não implementado** – reservado para Mês 2 |

---

## Notas de arquitetura e suposições documentadas

1. **Filtro de município ObrasGov**: a API não suporta filtro direto por município, então ingerimos todo o estado RJ e filtramos na camada CLEAN por substring "Macaé" nos campos `endereco`, `nome` e `descricao`.

2. **Datas pendentes**: strings como "Informação Pendente" são convertidas para `NULL` e geram `flag_data_fim_pendente = true`.

3. **Matching ObrasGov ↔ TCE-RJ**: heurística de similaridade de texto (`SequenceMatcher`, threshold 0.60). Pode ser refinada com NLP ou cruzamento por CNPJ/número de contrato no Mês 2.

4. **PostGIS**: DDL usa `TEXT` para campos de geometria; funções `ST_*` são indicadas em comentários. Ativar substituindo `TEXT` por `geometry(Point, 4326)` e criando a extensão.

5. **Paginação ObrasGov**: assume retorno `{"content": [...]}` ou lista direta. Adaptar conforme resposta real da API.

6. **TCE-RJ – filtro Macaé**: obras TCE não têm campo de município no endpoint principal. O filtro é feito por matching com ObrasGov; no Mês 2, refinar com campo `Ente`.
