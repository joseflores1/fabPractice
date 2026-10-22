"""Domain row generators for the Fabellala synthetic-data pipeline.

This module owns WHAT rows exist; writers.py owns HOW they land on disk.
Every generator is a pure function of two things: the reference Scale
preset from config and labeled RNG streams hashed from SEED, so the same
call sequence produces byte-identical Arrow tables on any machine —
clean-row draws and defect draws never share a stream (the defect stream
is offset by DEFECT_SEED_OFFSET, so tuning a defect rate never reshuffles
the clean data).

Importing this module performs one and only one side effect: the six
generator-owned frozen schemas (clientes, comercios, cuentas, tarjetas,
transacciones_fbl, saldos_diarios) are registered into
writers.EXTRACT_SCHEMAS. uf_diaria stays writers-pinned. After
registration, write_extract accepts every COMPLIANCE_TAGS entity.

Foreign-key discipline is a calling contract, never inferred: generate
in source order — clientes, comercios, cuentas(ruts), tarjetas(cuentas),
transacciones_fbl(tarjetas), saldos_diarios(cuentas) — and the daily
entities stream deterministic per-day chunks oldest-to-newest.

Name/provider randomness comes from Faker (es_CL locale); each generator
call seeds its own instance via seed_instance() with a stream-derived
int, which replays exactly. Generator calls must therefore run
sequentially — as generate_synthetic_data.py does — never interleaved.

Clean-row counts follow the Scale preset exactly; defect records arrive
as extra rows (duplicates and orphan references), so a full run exceeds
the preset by however many the configured rates produced. Every defect
family injects deliberately, at the configured rate or count, and emits
ledger rows (entidad, llave, tipo_defecto, valor_original,
valor_inyectado, tasa_configurada) that the CLI concatenates and writes
once via write_defect_ledger at end of run. No CLI lives here.
"""

import hashlib
import random
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal

import pyarrow as pa
from faker import Faker

from fabellala_gen import writers
from fabellala_gen.config import (
    BAD_DV_RUT_RATE,
    CAT_VARIANT_RATE,
    DEFECT_SEED_OFFSET,
    DUP_TRANSACCION_RATE,
    END_DATE_ANCHOR,
    LATE_ARRIVAL_RATE,
    N_CONFLICTING_CLIENTES,
    N_ORPHAN_COMERCIO_REFS,
    N_UF_PRECISION_VIOLATIONS,
    SALDO_PRECISION,
    SEED,
    SENTINEL_NULL_RATE,
    START_DATE,
    UF_FORMAT_DIRT_RATE,
    UF_SCALE,
    Scale,
)
from fabellala_gen.rut import compute_dv, format_rut

DEFECT_TYPES = (
    "bad_dv_rut",
    "conflicting_cliente",
    "sentinel_null",
    "cat_variant",
    "orphan_comercio_ref",
    "dup_transaccion",
    "late_arrival",
    "uf_format_dirt",
    "uf_precision_violation",
)

SENTINEL_NULLS = ("N/A", "SIN DATO", "-", "")

SEGMENTOS = ("RENTA ALTA", "RENTA MEDIA", "RENTA BAJA")

CATEGORIAS = (
    "SUPERMERCADO",
    "RESTAURANTE",
    "FARMACIA",
    "VESTIMENTA",
    "FERRETERIA",
    "LIBRERIA",
)

_CUENTA_TIPOS = ("CORRIENTE", "AHORRO", "VISTA")


@dataclass(frozen=True)
class Chunk:
    """One ready-to-write extract partition plus its ledger ground truth.

    Attributes:
        entidad: Entity name, already a COMPLIANCE_TAGS key with a
            registered schema, ready for write_extract.
        fecha: The partition date. Snapshots carry END_DATE_ANCHOR;
            daily chunks carry their own movement day.
        table: The partition rows with the declared schema exactly.
        ledger_rows: Defect rows observed while building this chunk, in
            DEFECT_LEDGER_SCHEMA order — empty for clean chunks. The CLI
            concatenates them across the run and hands one table to
            write_defect_ledger.
    """

    entidad: str
    fecha: date
    table: pa.Table
    ledger_rows: tuple[dict[str, str | float], ...]


def _seed_int(label: str) -> int:
    """Derive a stable integer seed from SEED plus a stream label.

    Args:
        label: Slash-free label, e.g. "clientes|rows".

    Returns:
        A 256-bit digest of "SEED|label" rendered as one integer, so the
        same label always seeds the same stream.
    """
    key = f"{SEED}|{label}".encode()
    return int.from_bytes(hashlib.sha256(key).digest(), "big")


def _stream(label: str) -> random.Random:
    """Return the labeled clean-data RNG stream.

    Args:
        label: Stream label, e.g. "clientes|rows" or
            "transacciones_fbl|rows".

    Returns:
        A random.Random seeded from SEED, independent of every other
        label's stream.
    """
    return random.Random(_seed_int(label))


def _defect_stream(entidad: str) -> random.Random:
    """Return the defect-injection RNG stream for one entity.

    The stream is hashed from SEED plus the DEFECT_SEED_OFFSET offset
    and the entity label, so defect draws are statistically independent
    of clean-row draws: raising a rate in config changes which rows are
    dirty but never the values of the clean rows.

    Args:
        entidad: Entity name, one of the COMPLIANCE_TAGS keys.

    Returns:
        A random.Random dedicated to that entity's defect draws.
    """
    key = f"{SEED + DEFECT_SEED_OFFSET}|{entidad}".encode()
    return random.Random(int.from_bytes(hashlib.sha256(key).digest(), "big"))


def _fake(label: str) -> Faker:
    """Return a name/provider generator seeded from a labeled stream.

    Faker's per-instance seed_instance() replays deterministically (the
    constructor's random= parameter does not), so every call gets a
    freshly seeded instance. Because seeding mutates only the new
    instance's own random source, each generator call is reproducible
    provided calls run sequentially, never interleaved — the CLI's
    calling pattern.

    Args:
        label: Stream label, e.g. "clientes|rows" (appended with
            "|names" internally).

    Returns:
        A seeded Faker bound to the es_CL locale.
    """
    fake = Faker("es_CL")
    fake.seed_instance(_seed_int(f"{label}|names"))
    return fake


def _history_dates() -> list[date]:
    """Return the fixed run window, oldest day first.

    Both bounds come from config, never from date.today(): START_DATE
    and END_DATE_ANCHOR are frozen constants, so the generated calendar
    is part of the reproducibility contract.

    Returns:
        One date per calendar day from START_DATE through
        END_DATE_ANCHOR inclusive.
    """
    span = (END_DATE_ANCHOR - START_DATE).days + 1
    return [START_DATE + timedelta(days=offset) for offset in range(span)]


def _ledger_row(
    entidad: str,
    llave: str,
    tipo_defecto: str,
    valor_original: str,
    valor_inyectado: str,
    tasa: float,
) -> dict[str, str | float]:
    """Build one defect-ledger payload row.

    Args:
        entidad: Entity whose chunk carries the defect.
        llave: Stable natural key of the affected row, prefixed by its
            kind, e.g. "rut=12345678" (body only for bad_dv_rut, where
            only the digit varies) or "folio=F-20250301-000004".
        tipo_defecto: One of the families listed in DEFECT_TYPES.
        valor_original: The clean/canonical value before injection, or
            an empty string when no canonical counterpart exists (a
            duplicated row, or an orphan reference that never had one).
        valor_inyectado: The dirty or affected value as written.
        tasa: The configured rate that produced the defect, or — for
            count-configured defects (N_CONFLICTING_CLIENTES,
            N_ORPHAN_COMERCIO_REFS, N_UF_PRECISION_VIOLATIONS) — the
            applied incidence, stored as tasa_configurada (a rate is not
            money: plain float64).

    Returns:
        A dict with exactly the DEFECT_LEDGER_SCHEMA field names.
    """
    return {
        "entidad": entidad,
        "llave": llave,
        "tipo_defecto": tipo_defecto,
        "valor_original": valor_original,
        "valor_inyectado": valor_inyectado,
        "tasa_configurada": tasa,
    }


def _wrong_dv(body: int, rng: random.Random) -> str:
    """Return a check digit that deliberately fails the body's modulo 11.

    Args:
        body: The RUT body whose deliberate failure is wanted.
        rng: The defect stream drawing which wrong digit appears.

    Returns:
        A single character in 0-9 or K guaranteed unequal to
        compute_dv(body).
    """
    alternatives = [c for c in "0123456789K" if c != compute_dv(body)]
    return rng.choice(alternatives)


def _table(schema: pa.Schema, columns: dict[str, list]) -> pa.Table:
    """Build a table typed by an exact schema, never inferred.

    Args:
        schema: The frozen pa.Schema this table must satisfy.
        columns: Field name to python values, one list per schema
            column.

    Returns:
        The pa.Table with the declared types applied verbatim.
    """
    arrays = {
        name: pa.array(values, type=schema.field(name).type)
        for name, values in columns.items()
    }
    return pa.table(arrays, schema=schema)


def _contextual_variant(value: str, rng: random.Random) -> str:
    """Return a free-text variant of value that differs from it.

    Mimics human keying drift in free-text categorical columns: case
    shifts and stray whitespace. Guaranteed unequal to the canonical
    value: no-op transforms (e.g. double-spacing a single-word value)
    are filtered out before the choice.

    Args:
        value: The canonical uppercase categorical value.
        rng: The defect stream drawing which variant appears.

    Returns:
        A string differing from value, of the same kind.
    """
    forms = (
        value.lower(),
        value.title(),
        f" {value} ",
        f"{value} ",
        value.replace(" ", "  "),
    )
    differing = [form for form in forms if form != value]
    return rng.choice(differing)


def _uf_string(value: Decimal, decimals: int = UF_SCALE) -> str:
    """Render a UF amount in the Chilean legacy feed convention.

    The canonical format is dot for thousands, comma for decimals,
    exactly like the CMF/SBIF API returns it: 41021.58 becomes
    "41.021,58". The decimals argument exists only for the precision
    defect, which must look plausible to a human eye.

    Args:
        value: The UF amount, first quantized to the requested number
            of decimals.
        decimals: Fraction digits to render, normally UF_SCALE (2);
            precision violations pass UF_SCALE + 1.

    Returns:
        The legacy-format string, e.g. "41.021,58" or "41.021,575".
    """
    quantized = value.quantize(Decimal(1).scaleb(-decimals))
    whole, frac = str(quantized).split(".")
    thousands = f"{int(whole):,}".replace(",", ".")
    return f"{thousands},{frac}"


def _uf_walk(rng: random.Random, days: int) -> list[Decimal]:
    """Produce a deterministic monotone daily UF series.

    Args:
        rng: The clean stream that drives the series.
        days: Number of daily observations.

    Returns:
        Exactly `days` Decimal amounts quantized to UF_SCALE decimals,
        strictly increasing but plausibly wiggly.
    """
    amounts: list[Decimal] = []
    current = Decimal(41000)
    for _ in range(days):
        current += Decimal(f"{rng.uniform(0.05, 0.85):.2f}")
        amounts.append(current.quantize(Decimal(1).scaleb(-UF_SCALE)))
    return amounts


# ---------------------------------------------------------------------------
# Frozen extract contracts. Declared here, registered into
# writers.EXTRACT_SCHEMAS at import — never inferred, never mutable.
# ---------------------------------------------------------------------------
GENERATOR_SCHEMAS: dict[str, pa.Schema] = {
    "clientes": pa.schema(
        [
            ("rut", pa.string()),
            ("nombre", pa.string()),
            ("segmento", pa.string()),
            ("fecha_alta", pa.date32()),
        ]
    ),
    "comercios": pa.schema(
        [
            ("id", pa.string()),
            ("nombre", pa.string()),
            ("categoria", pa.string()),
        ]
    ),
    "cuentas": pa.schema(
        [
            ("id", pa.string()),
            ("rut", pa.string()),
            ("tipo", pa.string()),
            ("moneda", pa.string()),
            ("saldo", pa.decimal128(*SALDO_PRECISION)),
            ("fecha_apertura", pa.date32()),
        ]
    ),
    "tarjetas": pa.schema(
        [
            ("id", pa.string()),
            ("rut", pa.string()),
            ("cuenta_id", pa.string()),
            ("estado", pa.string()),
            ("fecha_emision", pa.date32()),
        ]
    ),
    "transacciones_fbl": pa.schema(
        [
            ("folio", pa.string()),
            ("tarjeta_id", pa.string()),
            ("comercio_id", pa.string()),
            ("monto_clp", pa.int64()),
            ("hora", pa.timestamp("s")),
        ]
    ),
    "saldos_diarios": pa.schema(
        [
            ("cuenta_id", pa.string()),
            ("saldo", pa.decimal128(*SALDO_PRECISION)),
        ]
    ),
}


def _register_extract_schemas() -> None:
    """Register the generator-owned schemas into the writers' registry.

    The single mutation this module performs at import: it grows
    writers.EXTRACT_SCHEMAS from the writers-pinned uf_diaria entry to
    the full COMPLIANCE_TAGS set, after which write_extract refuses no
    entity for lack of a declared schema.

    Raises:
        ValueError: If any generator schema conflicts with an
            already-declared entry (defensive parity check).
    """
    for name, schema in GENERATOR_SCHEMAS.items():
        existing = writers.EXTRACT_SCHEMAS.get(name)
        if existing is not None and existing != schema:
            raise ValueError(
                f"declared schema for {name!r} conflicts with writers' registry"
            )
        writers.EXTRACT_SCHEMAS[name] = schema


# ---------------------------------------------------------------------------
# Snapshot generators: one anchor-date partition per entity.
# ---------------------------------------------------------------------------
def generate_clientes(scale: Scale) -> Chunk:
    """Generate the clientes snapshot as of END_DATE_ANCHOR.

    RUTs are minted with rut.py: bodies sampled without replacement,
    check digits computed, format canonical. Three defect families
    inject at the configured rates and are recorded: deliberately bad
    check digits (BAD_DV_RUT_RATE), sentinel-null segmento values
    (SENTINEL_NULL_RATE), and duplicate-rut rows carrying a conflicting
    segmento (N_CONFLICTING_CLIENTES appended rows, written with correct
    digits so the defect kinds stay independent).

    Args:
        scale: Row-count preset; uses scale.clientes.

    Returns:
        A Chunk with entidad "clientes", fecha END_DATE_ANCHOR, the
        generated table, and its ledger rows.
    """
    rng = _stream("clientes|rows")
    defect_rng = _defect_stream("clientes")
    fake = _fake("clientes")
    rut_bodies = rng.sample(range(3_000_000, 100_000_000), k=scale.clientes)

    ruts: list[str] = []
    ledger: list[dict[str, str | float]] = []
    segmentos: list[str] = []

    for body in rut_bodies:
        dv = compute_dv(body)
        if defect_rng.random() < BAD_DV_RUT_RATE:
            clean_rut = format_rut(body, dv)
            dirty_rut = format_rut(body, _wrong_dv(body, defect_rng))

            ruts.append(dirty_rut)
            ledger.append(
                _ledger_row(
                    "clientes",
                    f"rut={body}",
                    "bad_dv_rut",
                    clean_rut,
                    dirty_rut,
                    BAD_DV_RUT_RATE,
                )
            )

        else:
            ruts.append(format_rut(body, dv))

        segmento = rng.choice(SEGMENTOS)
        if defect_rng.random() < SENTINEL_NULL_RATE:
            dirty_segmento = defect_rng.choice(SENTINEL_NULLS)

            segmentos.append(dirty_segmento)
            ledger.append(
                _ledger_row(
                    "clientes",
                    f"rut={body}",
                    "sentinel_null",
                    segmento,
                    dirty_segmento,
                    SENTINEL_NULL_RATE,
                )
            )
        else:
            segmentos.append(segmento)

    alta_window = (END_DATE_ANCHOR - START_DATE).days
    fechas_alta = [
        START_DATE + timedelta(days=rng.randint(0, alta_window))
        for _ in rut_bodies
    ]
    nombres = [fake.name() for _ in rut_bodies]

    for position in defect_rng.sample(
        range(scale.clientes), k=N_CONFLICTING_CLIENTES
    ):
        body = rut_bodies[position]
        original_segmento = segmentos[position]
        conflicting = _contextual_variant(
            defect_rng.choice(SEGMENTOS), defect_rng
        )

        ruts.append(format_rut(body, compute_dv(body)))
        nombres.append(fake.name())
        segmentos.append(conflicting)
        fechas_alta.append(
            START_DATE + timedelta(days=defect_rng.randint(0, alta_window))
        )
        ledger.append(
            _ledger_row(
                "clientes",
                f"rut={body}",
                "conflicting_cliente",
                original_segmento,
                conflicting,
                N_CONFLICTING_CLIENTES / scale.clientes,
            )
        )

    table = _table(
        GENERATOR_SCHEMAS["clientes"],
        {
            "rut": ruts,
            "nombre": nombres,
            "segmento": segmentos,
            "fecha_alta": fechas_alta,
        },
    )
    return Chunk("clientes", END_DATE_ANCHOR, table, tuple(ledger))


def generate_comercios(scale: Scale) -> Chunk:
    """Generate the comercios snapshot as of END_DATE_ANCHOR.

    One defect family injects: free-text category drift at
    CAT_VARIANT_RATE, where the clean category is replaced by a
    contextual variant of itself and every occurrence is recorded in the
    ledger, so the DQ layer can practice variant-aware classification.

    Args:
        scale: Row-count preset; uses scale.comercios.

    Returns:
        A Chunk with entidad "comercios", fecha END_DATE_ANCHOR, the
        generated table, and its ledger rows.
    """
    rng = _stream("comercios|rows")
    defect_rng = _defect_stream("comercios")
    fake = _fake("comercios")

    ids = [f"COM-{number:04d}" for number in range(1, scale.comercios + 1)]
    nombres = [fake.company() for _ in ids]

    categorias: list[str] = []
    ledger: list[dict[str, str | float]] = []

    for index in range(scale.comercios):
        clean_category = rng.choice(CATEGORIAS)
        if defect_rng.random() < CAT_VARIANT_RATE:
            dirty_category = _contextual_variant(clean_category, defect_rng)
            ledger.append(
                _ledger_row(
                    "comercios",
                    ids[index],
                    "cat_variant",
                    clean_category,
                    dirty_category,
                    CAT_VARIANT_RATE,
                )
            )
            categorias.append(dirty_category)

        else:
            categorias.append(clean_category)

    table = _table(
        GENERATOR_SCHEMAS["comercios"],
        {"id": ids, "nombre": nombres, "categoria": categorias},
    )
    return Chunk("comercios", END_DATE_ANCHOR, table, tuple(ledger))


def generate_cuentas(scale: Scale, ruts: Sequence[str]) -> Chunk:
    """Generate the cuentas snapshot as of END_DATE_ANCHOR.

    saldo honors the SALDO_PRECISION contract exactly: every value is a
    Decimal quantized to two places (integral values for CLP accounts).
    CLP and UF monedas mix four-to-one. This generator injects no
    defects: cuentas is the referential anchor the saldos_diarios walk
    depends on.

    Args:
        scale: Row-count preset; uses scale.cuentas.
        ruts: The clientes ruts, as generated (FK source). A single rut
            may own several cuentas; every rut owns at least one as long
            as scale.cuentas >= scale.clientes.

    Returns:
        A Chunk with entidad "cuentas", fecha END_DATE_ANCHOR, the
        generated table, and no ledger rows.

    Raises:
        ValueError: If ruts holds fewer rows than scale.clientes.
    """
    if len(ruts) < scale.clientes:
        raise ValueError(
            f"cuentas need at least {scale.clientes} ruts, got {len(ruts)}"
        )

    rng = _stream("cuentas|rows")
    opening_window = (END_DATE_ANCHOR - START_DATE).days

    ids: list[str] = []
    account_ruts: list[str] = []
    tipos: list[str] = []
    monedas: list[str] = []
    saldos: list[Decimal] = []
    fechas_apertura: list[date] = []

    for index in range(scale.cuentas):
        ids.append(f"CUE-{index + 1:06d}")
        account_ruts.append(ruts[index % scale.clientes])
        tipos.append(rng.choice(_CUENTA_TIPOS))

        if rng.uniform(0, 1) < 0.2: 
            monedas.append("UF")
            saldos.append(
                Decimal(str(rng.uniform(30, 2_500))).quantize(Decimal("0.01"))
            )

        else:
            monedas.append("CLP")
            saldos.append(
                Decimal(rng.randint(5_000, 4_000_000)).quantize(Decimal("0.01"))
            )

        fechas_apertura.append(
            END_DATE_ANCHOR - timedelta(days=rng.randint(0, opening_window))
        )

    table = _table(
        GENERATOR_SCHEMAS["cuentas"],
        {
            "id": ids,
            "rut": account_ruts,
            "tipo": tipos,
            "moneda": monedas,
            "saldo": saldos,
            "fecha_apertura": fechas_apertura,
        },
    )
    return Chunk("cuentas", END_DATE_ANCHOR, table, ())


def generate_tarjetas(scale: Scale, cuentas: Sequence[tuple[str, str]]) -> Chunk:
    """Generate the tarjetas snapshot as of END_DATE_ANCHOR.

    Every card binds to one cuenta and inherits that cuenta's rut, so
    card-level FKs never contradict account-level FKs. Nine cards in
    ten are ACTIVA. This generator injects no defects; the Tarjeta FBL
    identity lives in config (CARD_PRODUCT), not in the data.

    Args:
        scale: Row-count preset; uses scale.tarjetas.
        cuentas: The generated cuentas as (id, rut) pairs, in id order
            (FK source).

    Returns:
        A Chunk with entidad "tarjetas", fecha END_DATE_ANCHOR, the
        generated table, and no ledger rows.

    Raises:
        ValueError: If cuentas is empty.
    """
    if not cuentas:
        raise ValueError("tarjetas need at least one cuenta pair")

    rng = _stream("tarjetas|rows")
    opening_window = (END_DATE_ANCHOR - START_DATE).days

    ids: list[str] = []
    card_ruts: list[str] = []
    card_cuentas: list[str] = []
    estados: list[str] = []
    fechas_emision: list[date] = []

    for index in range(scale.tarjetas):
        cuenta_id, rut = cuentas[rng.randrange(len(cuentas))]

        ids.append(f"TAR-{index + 1:07d}")
        card_ruts.append(rut)
        card_cuentas.append(cuenta_id)
        estados.append("ACTIVA" if rng.uniform(0, 1) < 0.9 else "BLOQUEADA")
        fechas_emision.append(
            END_DATE_ANCHOR - timedelta(days=rng.randint(0, opening_window))
        )

    table = _table(
        GENERATOR_SCHEMAS["tarjetas"],
        {
            "id": ids,
            "rut": card_ruts,
            "cuenta_id": card_cuentas,
            "estado": estados,
            "fecha_emision": fechas_emision,
        },
    )
    return Chunk("tarjetas", END_DATE_ANCHOR, table, ())


# ---------------------------------------------------------------------------
# Daily generators: deterministic per-day chunk streams, oldest first.
# ---------------------------------------------------------------------------
def iter_transacciones_fbl(
    scale: Scale, tarjeta_ids: Sequence[str]
) -> Iterator[Chunk]:
    """Stream one transacciones_fbl Chunk per day across the run window.

    Row distribution fixes the exact scale.transacciones clean total:
    the base-per-day quotient is spread over the window with the
    remainder assigned to the earliest days. Foreign keys draw only from
    tarjeta_ids; the N_ORPHAN_COMERCIO_REFS orphan references live in a
    verifiable nowhere — ids beyond the comercios range, appended as one
    extra row per orphan day with no canonical counterpart. Duplicate
    folios (DUP_TRANSACCION_RATE) repeat an identical row in the same
    day partition; late arrivals (LATE_ARRIVAL_RATE) keep their
    partition date but carry a next-day early-morning timestamp,
    modeling an out-of-order feed. All three families are
    ledger-recorded.

    Args:
        scale: Row-count preset; uses scale.transacciones and
            scale.comercios.
        tarjeta_ids: The tarjetas ids, as generated (FK source).

    Yields:
        One Chunk per day, oldest first, with entidad
        "transacciones_fbl" and that day's ledger rows.

    Raises:
        ValueError: If tarjeta_ids is empty or the window is too short
            for the orphan placements.
    """
    if not tarjeta_ids:
        raise ValueError("transacciones need at least one tarjeta id")

    days = _history_dates()
    if N_ORPHAN_COMERCIO_REFS > len(days):
        raise ValueError(
            f"need at least {N_ORPHAN_COMERCIO_REFS} days for the orphans"
        )

    rng = _stream("transacciones_fbl|rows")
    defect_rng = _defect_stream("transacciones_fbl")

    base_per_day, remainder = divmod(scale.transacciones, len(days))
    orphan_days = set(
        defect_rng.sample(range(len(days)), k=N_ORPHAN_COMERCIO_REFS)
    )

    folio_seq = 0
    orphan_seq = 0

    for day_index, fecha in enumerate(days):
        ledger: list[dict[str, str | float]] = []
        folios: list[str] = []
        tarjetas: list[str] = []
        comercios: list[str] = []
        montos: list[int] = []
        horas: list[datetime] = []
        rows_for_day = (
            base_per_day
            + (1 if day_index < remainder else 0)
            + (1 if day_index in orphan_days else 0)
        )

        for _ in range(rows_for_day):
            folio_seq += 1
            folios.append(f"F-{fecha:%Y%m%d}-{folio_seq:06d}")
            tarjetas.append(tarjeta_ids[rng.randrange(len(tarjeta_ids))])
            comercios.append(f"COM-{rng.randint(1, scale.comercios):04d}")
            montos.append(rng.randint(1_500, 180_000))
            horas.append(
                datetime.combine(
                    fecha,
                    time(
                        rng.randint(8, 22),
                        rng.randint(0, 59),
                        rng.randint(0, 59),
                    ),
                )
            )

            if day_index in orphan_days and len(folios) == rows_for_day:
                orphan_seq += 1
                orphan_id = f"COM-{scale.comercios + orphan_seq:04d}"
                comercios[-1] = orphan_id
                ledger.append(
                    _ledger_row(
                        "transacciones_fbl",
                        f"folio={folios[-1]}",
                        "orphan_comercio_ref",
                        "",
                        orphan_id,
                        N_ORPHAN_COMERCIO_REFS / scale.transacciones,
                    )
                )

            if defect_rng.random() < LATE_ARRIVAL_RATE:
                original = horas[-1].isoformat()
                horas[-1] = datetime.combine(
                    fecha + timedelta(days=1),
                    time(
                        2,
                        defect_rng.randint(0, 59),
                        defect_rng.randint(0, 59),
                    ),
                )
                ledger.append(
                    _ledger_row(
                        "transacciones_fbl",
                        f"folio={folios[-1]}",
                        "late_arrival",
                        original,
                        horas[-1].isoformat(),
                        LATE_ARRIVAL_RATE,
                    )
                )

            if defect_rng.random() < DUP_TRANSACCION_RATE:
                folios.append(folios[-1])
                tarjetas.append(tarjetas[-1])
                comercios.append(comercios[-1])
                montos.append(montos[-1])
                horas.append(horas[-1])
                ledger.append(
                    _ledger_row(
                        "transacciones_fbl",
                        f"folio={folios[-1]}",
                        "dup_transaccion",
                        "",
                        folios[-1],
                        DUP_TRANSACCION_RATE,
                    )
                )
        table = _table(
            GENERATOR_SCHEMAS["transacciones_fbl"],
            {
                "folio": folios,
                "tarjeta_id": tarjetas,
                "comercio_id": comercios,
                "monto_clp": montos,
                "hora": horas,
            },
        )
        yield Chunk("transacciones_fbl", fecha, table, tuple(ledger))


def iter_saldos_diarios(
    cuentas: Sequence[tuple[str, str, Decimal, date]],
) -> Iterator[Chunk]:
    """Stream one saldos_diarios Chunk per day across the run window.

   Balances are an account-level statement anchored to the snapshot:
    every day's saldo is the cuenta's snapshot balance — the value the
    cuentas snapshot published as of END_DATE_ANCHOR — nudged by an
    independent factor of up to ±0.2 percent. The anchor day itself is
    pinned to the snapshot exactly, and rows before the cuenta's
    fecha_apertura do not exist — a cuenta joins its statement series
    the day it opens, so row counts grow from the earliest openings to
    the full population. Quantization follows moneda: CLP accounts
    carry whole pesos (the peso has no circulating subunit), UF
    accounts carry two decimals (the UF is officially quoted with
    exactly two), and each walks only its own magnitude, so CLP-peso
    and UF-unit balances stop being confusable.

    Args:
        cuentas: The generated cuentas as (id, moneda, saldo,
            fecha_apertura) tuples, in id order (FK source). saldo is
            the snapshot balance as of END_DATE_ANCHOR and anchors the
            whole series.

    Yields:
        One Chunk per day, oldest first, with entidad
        "saldos_diarios". No ledger rows: no defect family touches
        balances.

    Raises:
        ValueError: If cuentas is empty.
    """
    if not cuentas:
        raise ValueError("saldos_diarios need at least one cuenta tuple")
    rng = _stream("saldos_diarios|drift")

    for fecha in _history_dates():
        row_ids: list[str] = []
        saldos: list[Decimal] = []
        for cuenta_id, moneda, snapshot, opened in cuentas:
            if fecha < opened:
                continue
            if fecha == END_DATE_ANCHOR:
                row_ids.append(cuenta_id)
                saldos.append(snapshot)
                continue

            factor = Decimal(f"{rng.uniform(-0.002, 0.002):.6f}")
            step = Decimal(1) if moneda == "CLP" else Decimal("0.01")
            row_ids.append(cuenta_id)
            saldos.append(
                (snapshot * (Decimal(1) + factor)).quantize(step)
            )

        table = _table(
            GENERATOR_SCHEMAS["saldos_diarios"],
            {"cuenta_id": row_ids, "saldo": saldos},
        )

        yield Chunk("saldos_diarios", fecha, table, ())


def iter_uf_diaria() -> Iterator[Chunk]:
    """Stream one uf_diaria Chunk per day across the run window.

    The valor column is the only string column carrying the Chilean
    comma-decimal legacy convention. Two defect families inject: format
    dirt at UF_FORMAT_DIRT_RATE (thousands-dot and separator drift on
    2-decimal values) and exactly N_UF_PRECISION_VIOLATIONS rows with
    three decimals instead of the official two — both recorded so the
    DQ layer can measure format-aware parsing.

    Yields:
        One Chunk per day, oldest first, with entidad "uf_diaria" and
        that day's ledger rows.
    """
    days = _history_dates()
    walk_rng = _stream("uf_diaria|walk")
    defect_rng = _defect_stream("uf_diaria")

    amounts = _uf_walk(walk_rng, len(days))
    violation_days = set(
        defect_rng.sample(range(len(days)), k=N_UF_PRECISION_VIOLATIONS)
    )

    schema = writers.EXTRACT_SCHEMAS["uf_diaria"]
    incidence = N_UF_PRECISION_VIOLATIONS / max(len(days), 1)

    for day_index, fecha in enumerate(days):
        ledger: list[dict[str, str | float]] = []

        if day_index in violation_days:
            clean = _uf_string(amounts[day_index])
            valor = _uf_string(amounts[day_index], decimals=UF_SCALE + 1)

            ledger.append(
                _ledger_row(
                    "uf_diaria",
                    f"fecha={fecha:%Y-%m-%d}",
                    "uf_precision_violation",
                    clean,
                    valor,
                    incidence,
                )
            )

        else:
            clean = _uf_string(amounts[day_index])
            if defect_rng.random() < UF_FORMAT_DIRT_RATE:
                variants = (
                    clean.replace(".", ""),
                    clean.replace(".", " "),
                    f"UF {clean}",
                    f"{clean} uf",
                )

                valor = defect_rng.choice(variants)
                ledger.append(
                    _ledger_row(
                        "uf_diaria",
                        f"fecha={fecha:%Y-%m-%d}",
                        "uf_format_dirt",
                        clean,
                        valor,
                        UF_FORMAT_DIRT_RATE,
                    )
                )
            else:
                valor = clean

        table = _table(schema, {"valor": [valor]})
        yield Chunk("uf_diaria", fecha, table, tuple(ledger))


# ---------------------------------------------------------------------------
# Registration: importing this module completes the writers' registry.
# ---------------------------------------------------------------------------
_register_extract_schemas()