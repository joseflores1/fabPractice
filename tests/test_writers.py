"""Tests for fabellala_gen.writers: layout, roundtrip, determinism, refusal.

All writes are redirected to pytest tmp_path via monkeypatch, so no test
ever touches the real data/ tree.
"""

import hashlib
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fabellala_gen import writers
from fabellala_gen.writers import write_defect_ledger, write_extract

# ---------------------------------------------------------------------------
# Fixtures: redirect the writers' global paths to throwaway directories.
# ---------------------------------------------------------------------------

@pytest.fixture
def raw_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point DATA_RAW at a temporary directory for the test."""
    root = tmp_path.joinpath("raw")
    monkeypatch.setattr(writers, "DATA_RAW", root)
    return root


@pytest.fixture
def ledger_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point DEFECT_LEDGER_PATH at a temporary file for the test."""
    target = tmp_path / "qa" / "defect_ledger.parquet"
    monkeypatch.setattr(writers, "DEFECT_LEDGER_PATH", target)
    return target


# ---------------------------------------------------------------------------
# write_extract — independent contract 1 of 2: one Hive partition per call.
# Gates: COMPLIANCE_TAGS entity, frozen registry schema, no fecha column,
# exact schema match. Tests: layout, roundtrip, determinism, refusals.
# ---------------------------------------------------------------------------

def test_partition_layout_and_roundtrip(raw_root: Path) -> None:
    """A uf_diaria partition lands at the canonical Hive path and reads back intact."""
    target = write_extract(
        pa.table({"valor": pa.array(["20.939,49"])}),
        "uf_diaria",
        date(2025, 3, 1),
    )
    assert target == raw_root.joinpath("uf_diaria", "fecha=2025-03-01", "part-0000.parquet")
    back = pq.read_table(target)
    assert back.column("valor").to_pylist() == ["20.939,49"]
    assert back.schema == writers.EXTRACT_SCHEMAS["uf_diaria"]


def test_rewrite_is_byte_identical(raw_root: Path) -> None:
    """Rewriting the same partition yields byte-identical files."""
    table = pa.table({"valor": pa.array(["20.939,49", "21.100,00"])})
    first = write_extract(table, "uf_diaria", date(2025, 3, 1))
    first_digest = hashlib.sha256(first.read_bytes()).hexdigest()
    second = write_extract(table, "uf_diaria", date(2025, 3, 1))
    assert first == second
    assert hashlib.sha256(second.read_bytes()).hexdigest() == first_digest


def test_rejects_undeclared_schema(raw_root: Path) -> None:
    """An entity in COMPLIANCE_TAGS without a declared schema is refused."""
    with pytest.raises(ValueError, match="no Parquet schema declared"):
        write_extract(pa.table({"valor": pa.array(["1"])}), "clientes", date(2025, 3, 1))


def test_rejects_unknown_entidad(raw_root: Path) -> None:
    """An entity outside COMPLIANCE_TAGS is refused outright."""
    with pytest.raises(ValueError, match="unknown entidad"):
        write_extract(
            pa.table({"valor": pa.array(["1"])}), "inventos", date(2025, 3, 1)
        )


def test_rejects_float_money(raw_root: Path) -> None:
    """A float64 money column is refused instead of silently cast."""
    with pytest.raises(ValueError, match="does not match the declared schema"):
        write_extract(
            pa.table({"valor": pa.array([20939.49])}), "uf_diaria", date(2025, 3, 1)
        )


def test_rejects_fecha_in_payload(raw_root: Path) -> None:
    """The partition date must travel in the path, not inside the file."""
    with pytest.raises(ValueError, match="must travel in the path"):
        write_extract(
            pa.table(
                {
                    "valor": pa.array(["20.939,49"]),
                    "fecha": pa.array(["2025-03-01"]),
                }
            ),
            "uf_diaria",
            date(2025, 3, 1),
        )


# ---------------------------------------------------------------------------
# write_defect_ledger — independent contract 2 of 2: one fixed file per run.
# Gates: exact schema match against DEFECT_LEDGER_SCHEMA (no entity
# registry, no partitioning). Tests: roundtrip, determinism, refusals.
# ---------------------------------------------------------------------------

def test_defect_ledger_roundtrip(ledger_target: Path) -> None:
    """The ledger is written to DEFECT_LEDGER_PATH and reads back intact."""
    table = pa.table(
        {
            "entidad": pa.array(["clientes"]),
            "llave": pa.array(["rut=20000517"]),
            "tipo_defecto": pa.array(["bad_dv_rut"]),
            "valor_original": pa.array(["20000517-1"]),
            "valor_inyectado": pa.array(["20000517-4"]),
            "tasa_configurada": pa.array([0.003]),
        }
    )
    target = write_defect_ledger(table)
    assert target == ledger_target
    back = pq.read_table(target)
    assert back.column("valor_inyectado").to_pylist() == ["20000517-4"]
    assert back.column("tasa_configurada").to_pylist() == [0.003]
