"""Single source of truth for the Fabellala synthetic-data generator.

Holds identity constants, the reproducibility anchor (a fixed SEED and a FIXED
date range — anchored to a constant, never date.today(), so regeneration is
byte-reproducible), scale presets, dirty-data defect rates (with a separate
RNG offset and a defect-ledger output path), monetary-precision and
decimal-separator declarations (canonical dot-decimal storage vs. the
Chilean comma-decimal legacy feed convention, mimicked only by the STRING
column uf_diaria.valor), and COMPLIANCE_TAGS, which classifies each extract
per Chilean banking norms (see docs/compliance.md, forthcoming).
"""

import calendar
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

BANK_NAME = "Fabellala"
CARD_PRODUCT = "Tarjeta FBL"
DATASET_PREFIX = "fbl"
CLP = "CLP"
UF = "UF"

SEED = 20260930
END_DATE_ANCHOR = date(2026, 9, 30)  # fixed anchor, NOT today(): reproducibility
HISTORY_MONTHS = 24

UF_SCALE = 2
SALDO_PRECISION = (20, 2)  # (precision, scale) for decimal128 / BigQuery NUMERIC

# Canonical numeric convention: dot as decimal separator, no thousands
# separators — what Decimal / Parquet decimal128 / BigQuery NUMERIC speak.
# CLP has no decimals at all (int64 pesos); UF carries exactly UF_SCALE
# decimals. The Chilean human/legacy convention (comma decimal, dot
# thousands, e.g. "20.939,49" — the literal format returned by the CMF/SBIF
# API) is reproduced ONLY by the string column uf_diaria.valor (see
# UF_FORMAT_DIRT_RATE); every other column stays canonically typed.
# docs/README.md has the CMF API reference behind this convention.


def _shift_months(anchor: date, months: int) -> date:
    """Shift a date by a whole number of months.

    The day is clamped to the last day of the target month whenever it would
    not exist there (e.g. 2025-03-31 shifted back one month yields
    2025-02-28). Round-trips are not guaranteed to restore the original
    date: shifting 2024-02-29 forward and back yields 2024-02-28.

    Args:
        anchor: The date to shift from.
        months: Months to shift the date. Positive values shift forward;
            negative values shift backward.

    Returns:
        The shifted date.
    """
    total_months = anchor.year * 12 + (anchor.month - 1) + months
    year, zero_idx_month = divmod(total_months, 12)
    actual_month = zero_idx_month + 1
    clamped_last_day = min(anchor.day, calendar.monthrange(year, actual_month)[1])
    return date(year, actual_month, clamped_last_day)


START_DATE = _shift_months(END_DATE_ANCHOR, -HISTORY_MONTHS) + timedelta(days=1)


@dataclass(frozen=True)
class Scale:
    clientes: int
    cuentas: int
    tarjetas: int
    comercios: int
    transacciones: int


SMALL = Scale(
    clientes=1_000,
    cuentas=1_400,
    tarjetas=1_200,
    comercios=200,
    transacciones=200_000,
)
FULL = Scale(
    clientes=10_000,
    cuentas=14_000,
    tarjetas=12_000,
    comercios=800,
    transacciones=2_000_000,
)
SCALE_PRESETS = {"small": SMALL, "full": FULL}

DUP_TRANSACCION_RATE = 0.0005
LATE_ARRIVAL_RATE = 0.001
N_ORPHAN_COMERCIO_REFS = 50
N_CONFLICTING_CLIENTES = 3

# ---------------------------------------------------------------------------
# Dirty-data defect rates. Every defect is injected deliberately,
# deterministically, and recorded in the defect ledger (DEFECT_LEDGER_PATH)
# so the Phase-4 DQ layer can be evaluated against ground truth like a
# classifier (recall/precision), not just eyeballed. Each rate carries the
# real-world cause it mimics.
# ---------------------------------------------------------------------------
# Separate stream, offset from SEED so tuning these rates never reshuffles
# the clean data (clean rows stay byte-identical across regenerations).
DEFECT_SEED_OFFSET = 1

BAD_DV_RUT_RATE = 0.003  # digitación manual en sucursal
CAT_VARIANT_RATE = 0.05  # free-text: "Renta Alta"/"Renca Alta"/"RENTA ALTA"
SENTINEL_NULL_RATE = 0.01  # "N/A"/"SIN DATO"/"-"/"" cargados como "valor"
UF_FORMAT_DIRT_RATE = 0.02  # legacy feed: 98% "20.939,49", 2% variantes
N_UF_PRECISION_VIOLATIONS = 4  # UF con 3 decimales; la oficial tiene 2

# Hard rule: dirt never touches Hive partition keys (fecha=YYYY-MM-DD
# paths stay canonical) — impossible dates live inside the row, or the
# loader itself breaks, which is a generator bug, not a data defect.

COMPLIANCE_TAGS: dict[str, str] = {
    "clientes": "datos_personales",
    "cuentas": "secreto_bancario",
    "saldos_diarios": "secreto_bancario",
    "tarjetas": "reserva_bancaria",
    "transacciones_fbl": "reserva_bancaria",
    "comercios": "publico",
    "uf_diaria": "publico",
}

PROJECT_ROOT = Path(__file__).resolve(strict=True).parents[2]
DATA_RAW = PROJECT_ROOT.joinpath("data", "raw")
# Ground-truth labels for injected dirty-data defects (entidad, llave,
# tipo_defecto, valor_original, valor_inyectado, tasa_configurada). Lives
# under data/qa/ so it is gitignored along with data/raw/ outputs.
DEFECT_LEDGER_PATH = DATA_RAW.parent.joinpath( "qa", "defect_ledger.parquet")
