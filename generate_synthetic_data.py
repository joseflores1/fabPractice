"""Command-line entry point for the Fabellala synthetic-data generator.

Orchestrates one complete, FK-ordered run — clientes, comercios,
cuentas, tarjetas, transacciones_fbl, saldos_diarios, uf_diaria —
writing each partition through write_extract the moment it is
produced, then finishing with a single write_defect_ledger call that
carries every defect recorded along the way. Deterministic by
construction: the scale preset, SEED and frozen calendar in config
fully determine the output bytes, so re-running overwrites every
partition byte-identically. This script owns no domain logic and no
I/O policy of its own — orchestration only. Usage:

    uv run python generate_synthetic_data.py --scale small
"""

import argparse
import time
from collections import Counter

import pyarrow as pa

from fabellala_gen import writers
from fabellala_gen.config import (
    END_DATE_ANCHOR,
    SCALE_PRESETS,
    SEED,
    START_DATE,
    Scale,
)
from fabellala_gen.generators import (
    Chunk,
    generate_clientes,
    generate_comercios,
    generate_cuentas,
    generate_tarjetas,
    iter_saldos_diarios,
    iter_transacciones_fbl,
    iter_uf_diaria,
)
from fabellala_gen.writers import (
    DEFECT_LEDGER_SCHEMA,
    write_defect_ledger,
    write_extract,
)


def _run(scale: Scale) -> None:
    """Execute one full FK-ordered run and print the operator receipt.

    Extracts each upstream entity's FK columns and feeds them to the
    next generator — the calling contract from generators.py — so the
    only referential ambiguity left on disk is the deliberately
    injected orphan set. All writes go through the writers' gates;
    nothing here formats Parquet, names files or decides paths.

    Args:
        scale: Row-count preset, one of the SCALE_PRESETS values.
    """
    started = time.perf_counter()
    ledger: list[dict[str, str | float]] = []
    files = 0
    rows_by_entity: Counter[str] = Counter()

    def _write(chunk: Chunk) -> None:
        """Write one chunk's partition and absorb its ledger rows.

        Args:
            chunk: The ready-to-write partition from any generator.
        """
        nonlocal files
        ledger.extend(chunk.ledger_rows)
        write_extract(chunk.table, chunk.entidad, chunk.fecha)
        files += 1
        rows_by_entity[chunk.entidad] += chunk.table.num_rows

    clientes = generate_clientes(scale)
    comercios = generate_comercios(scale)
    ruts = clientes.table.column("rut").to_pylist()
    accounts = generate_cuentas(scale, ruts)
    cuenta_ids = accounts.table.column("id").to_pylist()
    cards = generate_tarjetas(
        scale, list(zip(cuenta_ids, accounts.table.column("rut").to_pylist()))
    )
    card_ids = cards.table.column("id").to_pylist()

    for chunk in (clientes, comercios, accounts, cards):
        _write(chunk)
    for chunk in iter_transacciones_fbl(scale, card_ids):
        _write(chunk)
    for chunk in iter_saldos_diarios(
        list(
            zip(
                cuenta_ids,
                accounts.table.column("moneda").to_pylist(),
                accounts.table.column("saldo").to_pylist(),
                accounts.table.column("fecha_apertura").to_pylist(),
            )
        )
    ):
        _write(chunk)
    for chunk in iter_uf_diaria():
        _write(chunk)

    write_defect_ledger(
        pa.Table.from_pylist(ledger, schema=DEFECT_LEDGER_SCHEMA)
    )
    _print_receipt(scale, files, rows_by_entity, ledger, started)


def _print_receipt(
    scale: Scale,
    files: int,
    rows_by_entity: Counter[str],
    ledger: list[dict[str, str | float]],
    started: float,
) -> None:
    """Print the end-of-run operator receipt.

    Args:
        scale: The preset that ran (echoed for verification).
        files: Total partitions written, ledger file excluded.
        rows_by_entity: Rows written per entity, defects included.
        ledger: Every recorded defect row, exactly as written.
        started: perf_counter value captured at run start.
    """
    elapsed = time.perf_counter() - started
    types = Counter(row["tipo_defecto"] for row in ledger)
    detail = ", ".join(f"{name} {count}" for name, count in sorted(types.items()))
    rows = ", ".join(f"{name} {count}" for name, count in sorted(rows_by_entity.items()))
    print()
    print("Fabellala synthetic run complete")
    print(f"  preset : {scale!r}")
    print(f"  seed   : {SEED} | window: {START_DATE} -> {END_DATE_ANCHOR}")
    print(f"  files  : {files}")
    print(f"  rows   : {rows}")
    print(f"  ledger : {len(ledger)} rows — {detail}")
    print(f"  raw    : {writers.DATA_RAW}")
    print(f"  qa file: {writers.DEFECT_LEDGER_PATH}")
    print(f"  elapsed: {elapsed:.1f}s")


def main() -> int:
    """Parse CLI arguments and execute one full run.

    Returns:
        Process exit code: 0 on success. Writer-gate ValueErrors and
        OSError failures propagate uncaught, so failures surface with
        full context and a nonzero interpreter exit — the orchestrator
        never swallows what the gates refuse.
    """
    parser = argparse.ArgumentParser(
        prog="generate_synthetic_data.py",
        description="Generate the Fabellala synthetic dataset (deterministic).",
    )
    parser.add_argument(
        "--scale",
        choices=sorted(SCALE_PRESETS),
        default="small",
        help=(
            "Row-count preset from config.SCALE_PRESETS; small writes "
            "~14 MB, full ~220 MB (2195 files)."
        ),
    )
    args = parser.parse_args()
    _run(SCALE_PRESETS[args.scale])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
