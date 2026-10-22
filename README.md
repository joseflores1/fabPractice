# fabellala-pipeline

[![Python 3.14](assets/badges/python.svg)](https://www.python.org/)
[![uv](assets/badges/uv.svg)](https://docs.astral.sh/uv/)
[![code style: ruff](assets/badges/ruff.svg)](https://docs.astral.sh/ruff/)
[![pyarrow](assets/badges/pyarrow.svg)](https://arrow.apache.org/docs/python/)

A bank's data engine, rebuilt in public on data that exists only in code — Python 3.14 · uv · pyarrow · Parquet · pytest · ruff

## Description

fabellala-pipeline reconstructs, piece by piece, the reporting backbone of a Chilean bank whose flagship — the **Fabellala** brand and its **Tarjeta FBL** card — exists only in configuration files (every generated artifact carries the `fbl` prefix). Phase 1 is underway: a deterministic generator that mints the bank's frozen raw layer — daily UF series, client RUTs, card movements — and lands each entity as idempotent Hive-partitioned Parquet under `data/raw/`, in exactly the shape a BigQuery load expects downstream.

> Nothing on these pages is real: no institution, no clients, no records. **Fabellala** and **Tarjeta FBL** are inventions, and every datum is synthesized in-process from the pinned `SEED` 20260930. The Chilean regulations cited below exist to set *conventions* — they outline what a real bank owes its data stack, which is precisely the discipline this project practices on values that belong to no one.

## Motivation

The lessons that make banking data trustworthy are exactly the ones tutorials skip: why a monetary column must be exactly round rather than float-fast, how a RUT's check digit actually validates, what it takes for "rewrite yesterday's partition" to be safe, and how a data-quality team reconciles defects it planted on purpose. fabellala-pipeline builds each of those bare, module by module:

- **A compliance chokepoint.** `config.py` is the only surface for constants — the seed, the `END_DATE_ANCHOR` (2026-09-30), the defect rates, the `COMPLIANCE_TAGS` gate — so branding and parameters never hide inside literals.
- **Chilean identity as a module.** `rut.py` implements módulo 11 end to end — compute, canonical format, parse, validate — and doubles as the seed for fabricating fake clients and, in staging, masking them (Ley 19.628).
- **Write contracts, not just files.** `writers.py` compares schemas by exact equality, refusing a float64 money column instead of rounding it; partition dates travel in paths, never payloads; every write goes temp-file → fsync → `os.replace`, so re-runs overwrite cleanly and crashes never leave half a file.
- **Ground truth for planted dirt.** Every injected defect is recorded in a QA ledger keyed by entity and `llave` with its configured rate — so Phase-4 reconciliation scores the DQ layer against evidence, rather than a memory of past adventures.

It is also a portfolio build: Google-style docstrings on every callable, a hermetic test suite whose writes are monkeypatched into `tmp_path`, and `ruff` clean as the standing bar.

## Quick Start

### Prerequisites

- [uv](https://docs.astral.sh/uv/) — it reads `.python-version` and installs CPython 3.14 if missing.

### First-time setup

```bash
uv sync
```

This creates the `.venv`, materializes the committed `uv.lock`, and installs `fabellala_gen` in editable mode.

### Usage

```bash
uv run pytest -q             # the test suite
uv run ruff check src tests  # linting
```

No CLI ships yet — `generators.py` and `generate_synthetic_data.py` are the in-progress phase-1 deliverables, followed by `validate_outputs.py`.

## Roadmap

| Phase | Status | Scope |
|---|---|---|
| 0 · Bootstrap | **done** | uv project, src-layout package, ruff + pytest hygiene |
| 1 · Synthetic sources | **in progress** | `config.py` / `rut.py` / `writers.py` shipped; `generators.py` and the CLI in flight |
| 2 · BigQuery raw | planned | `fbl_raw` dataset; per-partition idempotent loads of the extracts |
| 3 · Staging star schema | planned | fact/summary MERGEs; RUT masking per Ley 19.628 |
| 4 · Data quality | planned | traffic-light reconciliation against the defect ledger |
| 5 · Orchestration | planned | scheduled runs of the full chain |
| 6 · BI | planned | executive dashboards over the mart layer |

## Data conventions

- **Money is exactly round.** `SALDO_PRECISION (20, 2)` is the wire contract; float64 amounts are refused at the schema gate, never cast.
- **Dates live in paths, not rows.** Canonical `fecha=YYYY-MM-DD` Hive directories with a fixed `part-0000.parquet` name; zstd compression, 10 000-row row groups and Parquet v2.6 keep re-runs byte-identical.
- **The registry is closed.** A write requires the entity to be both in `COMPLIANCE_TAGS` and frozen in `EXTRACT_SCHEMAS` — names, types and nullability must match exactly.
- **Randomness is auditable.** A single `SEED` anchors all streams; defect injection draws from an offset child stream and can never perturb the pristine one.
- **Docstrings are house style.** Google format — Args, Returns, Raises — on every function, tests included.

## Regulatory references

The conventions above trace back to actual norms, cited from their official hosts:

- [DFL N° 3, Art. 154 — secreto y reserva bancaria](https://www.bcn.cl/leychile/Navegar?idNorma=83135) — the reason card/client extracts live in a controlled raw layer and never leave it raw.
- [Ley N° 19.628 — protección de datos personales](https://www.bcn.cl/leychile/Navegar?idNorma=141599) — the reason RUT masking is deferred to the staging layer rather than the extractor.
- [CMF RAN 20-7/1-13 — registro de acciones negociadas](https://www.cmfchile.cl/portal/normativa/624/w4-propertyvalue-49163.html) — the reference pattern for registry-driven, schema-frozen extraction contracts.
- [Ley N° 18.840, Art. 35 N° 9 — definición de la UF](https://www.leychile.cl/Navegar?idNorma=30216) and the [CMF daily UF series](https://api.cmfchile.cl/documentacion/UF.html) — the reality the synthetic `uf_diaria` stand-in imitates.

## Caveats

- **UF values are invented.** The series has the right precision and cadence but is generated, not fetched. Two future upgrades are on the table: swapping in the real CMF feed, or — cheaper and fully deterministic — emulating the official period mechanics (one IPC per 10th-to-9th period, daily factor `Rd = (1 + IPC/100)^(1/d) − 1` per Ley 18.840 Art. 35 N° 9), which would add flat and negative periods and the day-10 kink at the cost of strict monotonicity.
- **Bad RUTs are deliberate.** A rate-controlled batch fails the check digit on purpose, each one noted in the ledger; masking is a staging concern, per Ley 19.628.
- **The BigQuery layers are design-only.** Phases 2–6 are specified but unbuilt — the executable story today ends at Parquet.
- **No CI workflow yet.** A GitHub Actions pipeline (uv sync → ruff → pytest on push and PR) remains on the roadmap.

## Workflow

- **Branch off main:** `feat/<name>`, `fix/<name>`, `docs/<name>`, `test/<name>`, `chore/<name>`.
- **Angular-style commits:** `type(scope): subject`, imperative mood, one functionality per commit, a body that says why.
- **Green before push:** `uv run pytest -q` and `uv run ruff check src tests` must pass on every commit.
- **Never commit:** `opencode.json`, `data/raw/` outputs, `.venv/`, caches — all gitignored.
