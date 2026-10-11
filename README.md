# fabellala-pipeline

[![Python 3.14](assets/badges/python.svg)](https://www.python.org/)
[![uv](assets/badges/uv.svg)](https://docs.astral.sh/uv/)
[![code style: ruff](assets/badges/ruff.svg)](https://docs.astral.sh/ruff/)
[![pyarrow](assets/badges/pyarrow.svg)](https://arrow.apache.org/docs/python/)

Synthetic-data foundation: Python 3.14 · uv · pyarrow · pytest · ruff — Parquet extracts now, BigQuery star schema → DQ → BI on the roadmap

## Description

End-to-end banking-analytics practice project for a fictional Chilean bank, **Fabellala**, built around its flagship card product, **Tarjeta FBL** (dataset prefix `fbl`). The current phase is a **synthetic data generator**: a Python package that mints realistic-but-fake institutional data — UF currency series, client RUTs, card transactions — and lands it as idempotent, Hive-partitioned Parquet extracts, the raw layer that later analytics layers will consume.

> fabellala-pipeline involves no real bank, no real clients, and no real personal data. Every row is synthesized in-process from a pinned seed, and every institutional name is invented. The regulatory documents referenced below shape the project's *conventions* only — synthetic data is the point: it is what allows practicing real-bank discipline (RUT validation, fixed-point money, defect reconciliation) without ever touching anyone's data.

## Motivation

Most data-engineering tutorials hand you `df.to_parquet()` and stop there — you never see why partitioning is decided inside the writer, why money must never be a float, or how a data-quality team reconciles recorded defects back to knowable ground truth. fabellala-pipeline is a from-scratch implementation of that discipline:

- A stdlib-only foundation layer — `config.py` (seed, date anchor, defect rates, compliance tags) and `rut.py` (módulo-11 RUT validation and formatting) — so the regulatory-scary parts are isolated and unit-testable.
- Registry-driven Parquet writers that refuse silently-lossy input: schemas must match *exactly* (no casts, so a float64 money column is rejected rather than silently rounded), and every write is atomic (temp file → fsync → `os.replace`), making partition rewrites crash-safe and idempotent.
- A defect ledger: a dedicated QA Parquet file recording every injected synthetic defect, so a later data-quality layer can reconcile its findings against known ground truth instead of chasing phantom bugs.

It is also a portfolio project: every module carries Google-style docstrings, every contract is enforced at the gate, and the test suite redirects all writes to `tmp_path` so no test ever touches the real `data/` tree.

## Quick Start

### Prerequisites

- [uv](https://docs.astral.sh/uv/) — manages Python 3.14 via `.python-version` and installs it if missing

### First-time setup

```bash
uv sync
```

That creates `.venv/`, installs dependencies from the committed `uv.lock`, and installs the `fabellala_gen` package in editable mode.

### Usage

```bash
uv run pytest -q             # test suite
uv run ruff check src tests  # lint
```

There is no CLI entry point yet — `generate_synthetic_data.py` is the next build item, after the per-entity generators.

## Roadmap

| Phase | Status | Deliverable |
|---|---|---|
| 0 · Bootstrap | **done** | uv project, src-layout package, ruff + pytest, hygiene |
| 1 · Synthetic sources | **in progress** | `config.py`, `rut.py`, `writers.py` done; next: `generators.py`, `generate_synthetic_data.py` |
| 2 · BigQuery raw | planned | `fbl_raw` dataset; idempotent partition loads of the Parquet extracts |
| 3 · Staging star schema | planned | fact/summary tables via MERGE; RUT masking per Ley 19.628 |
| 4 · Data quality | planned | rebalance / traffic-light reconciliation against the defect ledger |
| 5 · Orchestration | planned | scheduled runs of the full pipeline |
| 6 · BI | planned | executive dashboards over the mart layer |

## Data conventions

- **Fixed-point money.** `SALDO_PRECISION = (20, 2)` is the contract; float64 money is refused at the writer, never cast.
- **Hive partitions.** Extracts land at `data/raw/<entidad>/fecha=YYYY-MM-DD/part-0000.parquet`; the partition date travels in the path, never inside the file. zstd compression, 10 000-row row groups, deterministic Parquet v2.6.
- **Closed schema registry.** Only entities declared in `EXTRACT_SCHEMAS` (and listed in `COMPLIANCE_TAGS`) can be written; everything else is refused by name.
- **Reproducibility.** One pinned `SEED` (2026-09-30) and `END_DATE_ANCHOR`; defect injection draws from an offset child stream so defects never perturb the main stream.
- **Docstrings.** Google style on every function — params, returns, raises — enforced as the house style.

## Regulatory references

The conventions above are shaped by (and cite) the actual Chilean norms:

- [DFL N° 3, Art. 154 — secreto y reserva bancaria](https://www.bcn.cl/leychile/Navegar?idNorma=83135) — why card/client extracts stay in a controlled raw layer.
- [Ley N° 19.628 — protección de datos personales](https://www.bcn.cl/leychile/Navegar?idNorma=141599) — why RUT masking is deferred to the staging layer, never the analytical one.
- [CMF RAN 20-7/1-13 — registro de acciones negociadas](https://www.cmfchile.cl/portal/normativa/624/w4-propertyvalue-49163.html) — reference for registry-driven, schema-frozen extraction design.
- [Ley N° 18.840, Art. 35 N° 9 — definición de la UF](https://www.leychile.cl/Navegar?idNorma=30216) and the [CMF UF series](https://api.cmfchile.cl/documentacion/UF.html) — what the synthetic `uf_diaria` series stands in for.

## Caveats

- **UF values are synthetic.** The `uf_diaria` series follows the right shape and precision but is generated, not fetched; wiring the CMF UF series is a planned enhancement.
- **RUT masking is deferred by design.** The generator may inject bad-check-digit RUTs on purpose (tracked in the defect ledger); masking happens in staging, per Ley 19.628.
- **BigQuery layers are not implemented yet.** The repo currently ends at Parquet extracts; phases 2–6 are design-complete but unbuilt.
- **No CI yet.** A GitHub Actions pipeline (uv sync → ruff → pytest on push/PR) is on the roadmap.

## Workflow

- **Branch from main:** `feat/<name>`, `fix/<name>`, `docs/<name>`, `test/<name>`, `chore/<name>`.
- **Conventional (Angular-style) commits:** `type(scope): subject` in imperative mood, with a body explaining *why*; atomic commits — one functionality per commit.
- **Run everything through uv:** `uv run pytest -q`, `uv run ruff check src tests` must be green before committing.
- **Never commit:** `opencode.json`, `data/raw/` outputs, `.venv/`, caches — all gitignored.
