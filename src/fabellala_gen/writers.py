"""Disk writers for the Fabellala synthetic-data generator.

The single chokepoint for all pipeline I/O: every byte written to
data/raw/ and data/qa/ is produced here, so any file in the project has
exactly one possible origin. Two artifact families exist:

- Entity extracts under DATA_RAW, Hive-partitioned by canonical
  "fecha=YYYY-MM-DD" directories with one "part-0000.parquet" file per
  partition. The partition date travels in the path only, never as a
  column inside the file, so BigQuery hive_partitioning recovers it
  unambiguously at load time — and the config.py hard rule holds: dirt
  never touches partition keys.
- The defect ledger at DEFECT_LEDGER_PATH, the ground-truth record of
  every dirty-data defect the generator injects (rates in config.py),
  which the Phase-4 DQ layer scores against like a classifier.

Schemas are explicit, never inferred: money is decimal128(20, 2) (UF and
saldos) or int64 (CLP pesos), RUTs are canonical dot-less strings, and
uf_diaria.valor is the only string column carrying the Chilean
comma-decimal legacy convention. Writers refuse silently-lossy input by
requiring exact schema equality — no casts — so a float64 money column
cannot sneak through and round itself.

Byte reproducibility: row-group size, compression and Parquet format
version are pinned, and callers pass rows in deterministic order
(ordering belongs to generators.py), so regenerating under the pinned
uv.lock environment yields byte-identical files. No CLI lives here —
the entry point belongs to generate_synthetic_data.py — and no domain
content is invented: writers store exactly what callers hand them.
"""

import os
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from fabellala_gen.config import COMPLIANCE_TAGS, DATA_RAW, DEFECT_LEDGER_PATH

_ROW_GROUP_SIZE = 10_000
_COMPRESSION = "zstd"
_PARQUET_VERSION = "2.6"
_PART_FILE_NAME = "part-0000.parquet"
_PARTITION_COLUMN = "fecha"

# Registry of frozen extract contracts. uf_diaria is the only schema
# config.py fully pins today (the valor string column); the remaining
# entities are added here as generators.py freezes each contract, and
# write_extract refuses to write an entity until then.
EXTRACT_SCHEMAS: dict[str, pa.Schema] = {
    "uf_diaria": pa.schema([("valor", pa.string())]),
}

DEFECT_LEDGER_SCHEMA = pa.schema(
    [
        ("entidad", pa.string()),
        ("llave", pa.string()),
        ("tipo_defecto", pa.string()),
        ("valor_original", pa.string()),
        ("valor_inyectado", pa.string()),
        ("tasa_configurada", pa.float64()),
    ]
)


def _hive_path(entidad: str, fecha: date) -> Path:
    """Return the partition directory DATA_RAW/<entidad>/fecha=<iso date>.

    The date is rendered with date.isoformat(), which is always the
    canonical zero-padded YYYY-MM-DD form, so partition paths stay
    loader-safe regardless of caller formatting habits.

    Args:
        entidad: Entity name, one of the COMPLIANCE_TAGS keys.
        fecha: Partition date.

    Returns:
        The directory that holds that partition's Parquet file.
    """
    return DATA_RAW.joinpath(entidad, f"fecha={fecha.isoformat()}")


def write_extract(table: pa.Table, entidad: str, fecha: date) -> Path:
    """Write one entity partition to DATA_RAW as a Parquet file.

    Validates the entity, the declared schema and the partition
    discipline, then writes data/raw/<entidad>/fecha=<YYYY-MM-DD>/
    part-0000.parquet atomically (temp file plus os.replace), so a
    crash never leaves a partial partition and re-runs cleanly
    overwrite.

    The table must match the entity's schema in EXTRACT_SCHEMAS
    exactly — same column names (as a set) and same Arrow types and
    nullability. No cast is attempted: a float64 money column is
    rejected rather than silently rounded. Metadata differences are
    ignored.

    Args:
        table: The partition's rows, in the caller's deterministic
            order (ordering is generators.py's concern). Must not
            contain the partition column ("fecha") as a column: the
            date travels in the path only.
        entidad: Entity name, one of the COMPLIANCE_TAGS keys. Its
            schema must already be declared in EXTRACT_SCHEMAS.
        fecha: The partition date.

    Returns:
        The path of the written Parquet file.

    Raises:
        ValueError: If entidad is not in COMPLIANCE_TAGS, its schema
            is not yet declared in EXTRACT_SCHEMAS, the table carries
            a "fecha" column, or the table's schema does not exactly
            match the declared one.
    """
    if entidad not in COMPLIANCE_TAGS:
        raise ValueError(f"unknown entidad {entidad!r}: not a COMPLIANCE_TAGS key")
    if entidad not in EXTRACT_SCHEMAS:
        raise ValueError(
            f"no Parquet schema declared yet for {entidad!r}: add it to "
            "EXTRACT_SCHEMAS in writers.py when generators.py freezes the contract"
        )
    if _PARTITION_COLUMN in table.column_names:
        raise ValueError(
            f"partition column {_PARTITION_COLUMN!r} must travel in the "
            "path, not inside the file"
        )
    schema = EXTRACT_SCHEMAS[entidad]
    if set(table.column_names) != set(schema.names):
        raise ValueError(
            f"{entidad!r} columns {sorted(table.column_names)} do not match "
            f"the declared schema columns {sorted(schema.names)}"
        )
    if table.schema != schema:
        raise ValueError(
            f"{entidad!r} table schema {table.schema} does not match the "
            f"declared schema {schema}: types and nullability must match exactly"
        )
    target = _hive_path(entidad, fecha).joinpath(_PART_FILE_NAME)
    _atomic_write(table, target)
    return target


def write_defect_ledger(table: pa.Table) -> Path:
    """Write the defect ledger Parquet at DEFECT_LEDGER_PATH.

    The ledger is the ground-truth label set for the injected
    dirty-data defects, with six columns: entidad, llave, tipo_defecto,
    valor_original, valor_inyectado and tasa_configurada. The value
    columns are strings for uniformity — callers stringify non-string
    values (numbers, Decimals) before handing rows over;
    tasa_configurada is the configured defect rate, a plain float64
    because a rate is not money.

    The write is atomic (temp file plus os.replace) and overwrites any
    previous ledger, matching the generator's single end-of-run write.

    Args:
        table: The ledger rows. Must match DEFECT_LEDGER_SCHEMA
            exactly (same columns, types and nullability; metadata
            ignored).

    Returns:
        The path of the written ledger (DEFECT_LEDGER_PATH).

    Raises:
        ValueError: If the table's schema does not exactly match
            DEFECT_LEDGER_SCHEMA.
    """
    if set(table.column_names) != set(DEFECT_LEDGER_SCHEMA.names):
        raise ValueError(
            f"ledger columns {sorted(table.column_names)} do not match the "
            f"declared columns {sorted(DEFECT_LEDGER_SCHEMA.names)}"
        )
    if table.schema != DEFECT_LEDGER_SCHEMA:
        raise ValueError(
            f"ledger table schema {table.schema} does not match the declared "
            f"schema {DEFECT_LEDGER_SCHEMA}: types and nullability must match exactly"
        )
    _atomic_write(table, DEFECT_LEDGER_PATH)
    return DEFECT_LEDGER_PATH


def _atomic_write(table: pa.Table, target: Path) -> None:
    """Write a Parquet table to target atomically via temp + os.replace.

    Serializes to a pid-suffixed sibling of the target (same
    filesystem, so os.replace is atomic on POSIX), fsyncs the file
    before the rename, and removes the temp file on any failure. The
    final file name is fixed, so artifact determinism is unaffected by
    the temp name.

    Args:
        table: The table to serialize.
        target: Final file path; parent directories are created.

    Raises:
        OSError: If the write, fsync or replace fails; the target is
            left untouched and the temp file is removed.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_name(f"{target.name}.tmp-{os.getpid()}")
    try:
        pq.write_table(
            table,
            temp_path,
            row_group_size=_ROW_GROUP_SIZE,
            compression=_COMPRESSION,
            version=_PARQUET_VERSION,
        )
        with open(temp_path, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
    finally:
        if temp_path.exists():
            temp_path.unlink()
