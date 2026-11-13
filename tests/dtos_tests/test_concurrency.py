"""
test_concurrency.py
===================
Hallazgo 8 (D-57): contención MULTIPROCESO sobre la persistencia de DTOS (`SQLiteStore`).

UNAE ya tenía configuración explícita (busy_timeout, WAL) y pruebas multiproceso (tests/unae_tests/test_concurrency.py); DTOS dependía del
`timeout=5.0` implícito de `sqlite3.connect` y del `PRAGMA journal_mode = WAL` metido en schema.sql, sin verificar el resultado. Esta suite
fija lo que debe cumplirse con varios PROCESOS (no hilos) sobre el mismo archivo en disco.

Los workers arrancan con `spawn` y esperan en una Barrier, así las transacciones compiten de verdad en lugar de ejecutarse una tras otra por
la latencia de arranque. Cada worker devuelve por cola sus resultados y TODO error (incluido `sqlite3.OperationalError: database is locked`);
el padre nunca da por buenos los resultados de un worker que no respondió.

Secciones
  A. Configuración: busy_timeout y WAL explícitos y verificables; valores inválidos rechazados.
  B. Semántica del timeout: espera mientras hay un bloqueo temporal; con un bloqueo permanente falla con OperationalError al agotarse el plazo
     configurado (no el de 5 s por defecto); WAL desacopla lecturas de escrituras.
  C. Apertura simultánea de una base NUEVA desde varios procesos (carrera de la inicialización del esquema).
  D. Escrituras simultáneas (mandatos, efectos, cadena de auditoría, Commit Gate) sin errores de bloqueo, con lectores concurrentes (los escritores
     usan STRESS_BUSY_TIMEOUT_MS, ver su comentario); integridad
     posterior: sin pérdidas ni duplicados, cadena de auditoría válida y contigua, integrity_check y foreign_key_check limpios, append-only intacto.
  E. Carreras sobre la MISMA clave: exactamente un ganador y el resto recibe el error de dominio (no de bloqueo), sin filas huérfanas.
  F. Apertura simultánea de una base ANTERIOR a los parches 0003/0008 (sin `effects.evidence` ni `policies.content`): la migración de
     columnas no puede fallar con «duplicate column name» cuando varios procesos migran a la vez.

Ejecutar: python run_tests.py dtos
"""

from __future__ import annotations

import multiprocessing as mp
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
from collections import Counter
from contextlib import closing
from pathlib import Path

from dtos.audit import AuditChain
from dtos.enums import AuditIntegrityState, MandateStrength, MandateTriggerKind
from dtos.registries import MandateRecord
from dtos.store import BootstrapConflictError, ConflictError, SQLiteStore

FAILURES: list[str] = []

N_WORKERS = 6        # procesos escritores simultáneos
ROUNDS = 12          # iteraciones de la sección D por worker
RACE_ROUNDS = 6      # rondas de la sección E (una barrera por ronda)
FRESH_DBS = 4        # bases nuevas distintas en la sección C
FRESH_PROCS = 8      # procesos que abren la misma base nueva a la vez
BARRIER_TIMEOUT = 60
RESULT_TIMEOUT = 180
# Plazo de espera de los ESCRITORES de la sección D (g0.8 r19). SQLite no reparte el turno de escritura con equidad: un proceso que consigue el
# bloqueo suele encadenar todas sus transacciones y los demás, que solo reintentan cada pocos ms, esperan en cola. El trabajo total en cola es
# ~ N_WORKERS x ROUNDS x 5 transacciones x latencia de commit (FULL): ~0,05 s en Linux, pero ~20 s en un Windows 10 con disco lento (commit ~45 ms),
# donde los últimos de la cola agotaban los 10 s de producción y recibían «database is locked» (medido: errores a los 10,9-11,0 s). Esta prueba
# verifica la CORRECCIÓN bajo contención (sin pérdidas, duplicados ni atomicidad rota), no la latencia del disco; por eso aquí el plazo es
# explícito y holgado (3 veces lo medido). El valor de producción (10000 ms) NO cambia y sigue verificado en las secciones A, B y C.
STRESS_BUSY_TIMEOUT_MS = 60_000


def check(condition: bool, description: str) -> None:
    print(f"[{'OK ' if condition else 'FAIL'}] {description}")
    if not condition:
        FAILURES.append(description)


def expect_raises(exc_type, fn, description: str, contains: str | None = None) -> None:
    try:
        fn()
        check(False, description + " (no lanzó excepción)")
    except exc_type as e:
        check(contains is None or contains in str(e), description + ("" if contains is None else f" [mensaje con «{contains}»]"))
    except Exception as e:
        check(False, f"{description} (lanzó {type(e).__name__}: {e})")


def pragma(conn: sqlite3.Connection, name: str):
    return conn.execute(f"PRAGMA {name}").fetchone()[0]


def mandate(mid: str, task: str) -> MandateRecord:
    return MandateRecord(mandate_id=mid, task_id=task, strength=MandateStrength.EXPLICIT, trigger_kind=MandateTriggerKind.USER,
                         created_at="2026-11-05T10:00:00Z")


def seed_task(store: SQLiteStore, task_id: str) -> None:
    with store.transaction() as conn:
        conn.execute("INSERT INTO tasks (task_id, goal_id, status) VALUES (?, 'g-1', 'ACTIVE')", (task_id,))


# =====================================================================
# Workers (nivel de módulo: con spawn se importan por nombre en el hijo)
# =====================================================================

def _worker_entry(scenario: str, db: str, idx: int, rounds: int, barrier, stop, queue, role: str) -> None:
    out: dict = {"idx": idx, "role": role, "scenario": scenario, "errors": [], "data": {}}
    try:
        globals()[f"_scn_{scenario}"](db, idx, rounds, barrier, stop, out)
    except BaseException as exc:  # un fallo del hijo debe llegar al padre, no perderse
        out["errors"].append(f"{type(exc).__name__}: {exc}")
    queue.put(out)


def _guard(out: dict, what: str, fn):
    """Ejecuta fn(); cualquier excepción inesperada (sobre todo OperationalError) se registra y devuelve None."""
    try:
        return fn()
    except Exception as exc:
        out["errors"].append(f"{what}: {type(exc).__name__}: {exc}")
        return None


def _scn_open_fresh(db, idx, rounds, barrier, stop, out):
    """Sección C: todos abren a la vez una base que aún no existe (carrera de PRAGMA journal_mode + executescript del esquema)."""
    barrier.wait(timeout=BARRIER_TIMEOUT)
    store = SQLiteStore(db)
    with closing(sqlite3.connect(db)) as probe:
        out["data"]["journal_mode"] = pragma(probe, "journal_mode")
    out["data"]["busy_timeout"] = pragma(store._conn, "busy_timeout")
    store.close()


def _scn_open_legacy(db, idx, rounds, barrier, stop, out):
    """Sección F: todos abren a la vez una base con el esquema antiguo; cada uno intenta la migración de columnas."""
    barrier.wait(timeout=BARRIER_TIMEOUT)
    SQLiteStore(db).close()


def _scn_stress(db, idx, rounds, barrier, stop, out):
    """Sección D: cada worker escribe mandatos, efectos, cadena de auditoría y avanza un ciclo compartido por Commit Gate."""
    store = SQLiteStore(db, busy_timeout_ms=STRESS_BUSY_TIMEOUT_MS)
    out["data"]["busy_timeout"] = store._conn.execute("PRAGMA busy_timeout").fetchone()[0]
    chain = AuditChain(store)
    bumps = conflicts = 0
    barrier.wait(timeout=BARRIER_TIMEOUT)
    for r in range(rounds):
        tag = f"w{idx}-r{r}"
        _guard(out, f"{tag} task", lambda: seed_task(store, f"task-{tag}"))
        _guard(out, f"{tag} mandate", lambda: store.create_mandate(mandate(f"m-{tag}", f"task-{tag}"), f"ev-{tag}"))
        _guard(out, f"{tag} effect", lambda: store.insert_effect(f"eff-{tag}", f"op-{tag}", f"int-{tag}", f"att-{tag}", "prov", f"idem-{tag}", "CONFIRMED"))
        _guard(out, f"{tag} audit", lambda: chain.append({"worker": idx, "round": r}))
        while True:  # Commit Gate: leer versión, intentar con expected_version, reintentar si otro ganó (ConflictError ES lo esperado)
            cyc = _guard(out, f"{tag} read cycle", lambda: store.get_cognitive_cycle("cyc-shared"))
            if cyc is None:
                break
            try:
                store.commit_cognitive_cycle_transition("cyc-shared", "ACTIVE", cyc["cycle_version"], f"sh-{tag}", f"ch-{tag}")
                bumps += 1
                break
            except ConflictError:
                conflicts += 1
            except Exception as exc:
                out["errors"].append(f"{tag} commit gate: {type(exc).__name__}: {exc}")
                break
    out["data"].update(bumps=bumps, conflicts=conflicts)
    store.close()


def _scn_reader(db, idx, rounds, barrier, stop, out):
    """Sección D: lector concurrente. Nunca debe recibir 'database is locked' y lo que ve solo puede crecer (versión del ciclo, filas)."""
    store = SQLiteStore(db)
    last_version, last_audit, last_effects, reads = 0, 0, 0, 0
    monotonic = True
    barrier.wait(timeout=BARRIER_TIMEOUT)
    while not stop.is_set():
        cyc = _guard(out, "reader cycle", lambda: store.get_cognitive_cycle("cyc-shared"))
        n_audit = _guard(out, "reader audit", lambda: store._conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0])
        n_eff = _guard(out, "reader effects", lambda: store._conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0])
        if cyc is not None and n_audit is not None and n_eff is not None:
            if cyc["cycle_version"] < last_version or n_audit < last_audit or n_eff < last_effects:
                monotonic = False
            last_version, last_audit, last_effects = cyc["cycle_version"], n_audit, n_eff
            reads += 1
        stop.wait(0.001)
    out["data"].update(monotonic=monotonic, reads=reads)
    store.close()


def _race_outcome(fn) -> str:
    try:
        fn()
        return "ok"
    except BootstrapConflictError:
        return "bootstrap_conflict"
    except ConflictError:
        return "conflict"
    except sqlite3.IntegrityError:
        return "integrity"
    except Exception as exc:  # incluye OperationalError: database is locked
        return f"ERROR {type(exc).__name__}: {exc}"


def _scn_race(db, idx, rounds, barrier, stop, out):
    """Sección E: todos los workers pelean por la MISMA clave en cada ronda; debe haber exactamente un ganador."""
    store = SQLiteStore(db)
    outcomes: dict[str, list[str]] = {"genesis": [], "effect": [], "mandate": [], "commit": []}
    for r in range(rounds):
        barrier.wait(timeout=BARRIER_TIMEOUT)
        outcomes["genesis"].append(_race_outcome(
            lambda: store.write_genesis_authority(f"auth-r{r}-w{idx}", f"install-r{r}", f"principal-{idx}")))
        outcomes["effect"].append(_race_outcome(
            lambda: store.insert_effect(f"eff-race-r{r}-w{idx}", f"op-r{r}-w{idx}", f"int-r{r}", f"att-r{r}-w{idx}", "prov-race", f"idem-race-r{r}", "CONFIRMED")))
        outcomes["mandate"].append(_race_outcome(
            lambda: store.create_mandate(mandate(f"m-race-r{r}-w{idx}", f"task-race-{r}"), f"ev-race-r{r}-w{idx}")))
        outcomes["commit"].append(_race_outcome(
            lambda: store.commit_cognitive_cycle_transition(f"cyc-race-{r}", "ACTIVE", 1, f"sh-r{r}-w{idx}", f"ch-r{r}-w{idx}")))
    out["data"]["outcomes"] = outcomes
    store.close()


# =====================================================================
# Utilidades del padre
# =====================================================================

def run_workers(scenario: str, db: str, n: int, rounds: int, *, readers: int = 0):
    """Lanza n escritores (+ lectores) con spawn. Devuelve (resultados_escritores, resultados_lectores, problemas_de_proceso)."""
    ctx = mp.get_context("spawn")
    queue, barrier, stop = ctx.Queue(), ctx.Barrier(n + readers), ctx.Event()
    procs = [ctx.Process(target=_worker_entry, args=(scenario, db, i, rounds, barrier, stop, queue, "writer")) for i in range(n)]
    procs += [ctx.Process(target=_worker_entry, args=("reader", db, 100 + j, rounds, barrier, stop, queue, "reader")) for j in range(readers)]
    problems: list[str] = []
    writers: list[dict] = []
    readers_out: list[dict] = []
    for p in procs:
        p.start()
    try:
        while len(writers) < n:
            r = queue.get(timeout=RESULT_TIMEOUT)
            (writers if r["role"] == "writer" else readers_out).append(r)
        stop.set()
        while len(readers_out) < readers:
            readers_out.append(queue.get(timeout=RESULT_TIMEOUT))
    except Exception as exc:  # cola vacía por tiempo: un worker murió o se colgó
        problems.append(f"{scenario}: faltan resultados de workers ({type(exc).__name__})")
    finally:
        stop.set()
        for p in procs:
            p.join(timeout=30)
            if p.is_alive():
                p.terminate()
                problems.append(f"{scenario}: un worker no terminó")
            elif p.exitcode != 0:
                problems.append(f"{scenario}: un worker terminó con exitcode {p.exitcode}")
    return writers, readers_out, problems


def errors_of(results: list[dict]) -> list[str]:
    return [f"[{r['role']} {r['idx']}] {e}" for r in results for e in r["errors"]]


class TempDir:
    def __enter__(self) -> str:
        self.path = tempfile.mkdtemp(prefix="dtos-conc-")
        return self.path

    def __exit__(self, *exc) -> None:
        shutil.rmtree(self.path, ignore_errors=True)


# =====================================================================
# Secciones
# =====================================================================

def section_a_config() -> None:
    print("\n--- A. Configuración explícita ---")
    with TempDir() as d:
        db = os.path.join(d, "a.db")
        s = SQLiteStore(db)
        check(pragma(s._conn, "busy_timeout") == 10000, "busy_timeout por defecto = 10000 ms (misma estrategia que UNAE)")
        check(pragma(s._conn, "journal_mode") == "wal", "journal_mode = wal en una base en disco")
        check(pragma(s._conn, "foreign_keys") == 1, "foreign_keys = ON")
        s.close()
        s2 = SQLiteStore(db, busy_timeout_ms=2500)
        check(pragma(s2._conn, "busy_timeout") == 2500, "busy_timeout configurable por parámetro (2500 ms)")
        s2.close()
        with closing(sqlite3.connect(db)) as probe:
            check(pragma(probe, "journal_mode") == "wal", "el modo WAL es persistente en el archivo (lo ve una conexión ajena)")
        for bad in (True, False, -1, 1.5, "10000", None):
            expect_raises(ValueError, lambda b=bad: SQLiteStore(os.path.join(d, "bad.db"), busy_timeout_ms=b), f"busy_timeout_ms={bad!r} rechazado")
        check(not os.path.exists(os.path.join(d, "bad.db")), "un busy_timeout_ms inválido no deja ninguna base creada")


def section_b_timeout_semantics() -> None:
    print("\n--- B. Semántica del timeout y de WAL ---")
    with TempDir() as d:
        db = os.path.join(d, "b.db")
        SQLiteStore(db).close()

        # B1. Bloqueo permanente + timeout corto: falla con OperationalError tras ~el plazo configurado (no tras los 5 s por defecto).
        holder = sqlite3.connect(db, isolation_level=None, check_same_thread=False)
        holder.execute("BEGIN IMMEDIATE")
        try:
            short = SQLiteStore(db, busy_timeout_ms=200)
            t0 = time.monotonic()
            try:
                short.create_effect_attempt("att-b1", "int-b1", "prov", "idem-b1", "CREATED")
                check(False, "con el escritor bloqueado y busy_timeout=200 ms debe lanzar OperationalError (no lo hizo)")
            except sqlite3.OperationalError as exc:
                elapsed = time.monotonic() - t0
                check("locked" in str(exc).lower(), "bloqueo permanente: OperationalError «database is locked»")
                check(0.1 <= elapsed < 3.0, f"el error llega al agotarse el plazo CONFIGURADO (200 ms), no el de 5 s: {elapsed:.2f} s")
            short.close()
        finally:
            holder.execute("ROLLBACK")
            holder.close()

        # B2. Bloqueo temporal + timeout largo: la escritura ESPERA y termina bien (reintento automático del driver).
        holder = sqlite3.connect(db, isolation_level=None, check_same_thread=False)
        holder.execute("BEGIN IMMEDIATE")
        threading.Timer(0.6, lambda: holder.execute("ROLLBACK")).start()
        waiting = SQLiteStore(db, busy_timeout_ms=10000)
        t0 = time.monotonic()
        try:
            waiting.create_effect_attempt("att-b2", "int-b2", "prov", "idem-b2", "CREATED")
            elapsed = time.monotonic() - t0
            check(elapsed >= 0.4, f"bloqueo temporal: la escritura esperó al liberarse el bloqueo ({elapsed:.2f} s)")
            check(waiting.get_effect_attempt("att-b2") is not None, "bloqueo temporal: la fila quedó escrita")
        except sqlite3.OperationalError as exc:
            check(False, f"bloqueo temporal: la escritura no debió fallar ({exc})")
        waiting.close()
        time.sleep(0.2)
        holder.close()

        # B3. WAL desacopla: una lectura abierta (transacción de lectura sin cerrar) NO impide escribir ni confirmar.
        reader = sqlite3.connect(db, isolation_level=None, check_same_thread=False)
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM audit_log").fetchone()
        w = SQLiteStore(db, busy_timeout_ms=300)
        t0 = time.monotonic()
        try:
            w.insert_effect("eff-b3", "op-b3", "int-b3", "att-b3", "prov", "idem-b3", "CONFIRMED")
            check(time.monotonic() - t0 < 0.25, "WAL: con un lector con transacción abierta la escritura confirma sin esperar")
        except sqlite3.OperationalError as exc:
            check(False, f"WAL: la escritura no debió bloquearse por un lector ({exc})")
        # ...y a la inversa: con un escritor dentro de su transacción, una lectura nueva no espera ni falla.
        holder = sqlite3.connect(db, isolation_level=None, check_same_thread=False)
        holder.execute("BEGIN IMMEDIATE")
        holder.execute("INSERT INTO effect_attempts (attempt_id, intent_id, provider, idempotency_key, status, version) VALUES ('att-b3w','i','p','k','CREATED',1)")
        t0 = time.monotonic()
        try:
            seen = w.get_effect_attempt("att-b3w")
            check(seen is None and time.monotonic() - t0 < 0.25, "WAL: durante la transacción de otro escritor se lee la última versión confirmada, sin esperar")
        except sqlite3.OperationalError as exc:
            check(False, f"WAL: una lectura no debió bloquearse por un escritor ({exc})")
        holder.execute("ROLLBACK")
        holder.close()
        reader.execute("ROLLBACK")
        reader.close()
        w.close()


def section_c_open_fresh() -> None:
    print("\n--- C. Apertura simultánea de una base nueva ---")
    all_errors: list[str] = []
    problems: list[str] = []
    modes: set = set()
    timeouts: set = set()
    with TempDir() as d:
        for k in range(FRESH_DBS):
            db = os.path.join(d, f"fresh-{k}.db")
            writers, _, probs = run_workers("open_fresh", db, FRESH_PROCS, 0)
            problems += probs
            all_errors += errors_of(writers)
            for w in writers:
                modes.add(w["data"].get("journal_mode"))
                timeouts.add(w["data"].get("busy_timeout"))
            if not probs and not errors_of(writers):
                s = SQLiteStore(db)
                tables = {r[0] for r in s._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                check({"effects", "mandates", "audit_log", "cognitive_cycles"} <= tables, f"base nueva #{k}: el esquema quedó completo tras {FRESH_PROCS} aperturas simultáneas")
                s.close()
    check(problems == [], f"todos los procesos de apertura respondieron y terminaron bien {problems}")
    check(all_errors == [], f"abrir la misma base nueva desde {FRESH_PROCS} procesos a la vez no produce errores {all_errors[:3]}")
    check(modes == {"wal"}, f"todas las conexiones ven journal_mode=wal ({sorted(map(str, modes))})")
    check(timeouts == {10000}, f"todas las conexiones tienen busy_timeout=10000 ({sorted(map(str, timeouts))})")


def section_d_stress() -> None:
    print("\n--- D. Escrituras simultáneas con lectores ---")
    with TempDir() as d:
        db = os.path.join(d, "d.db")
        s = SQLiteStore(db)
        seed_task(s, "task-shared")
        s.create_cognitive_cycle("cyc-shared", "task-shared", "ACTIVE", "sh-0", "ch-0")
        s.close()

        t0 = time.monotonic()
        writers, readers, problems = run_workers("stress", db, N_WORKERS, ROUNDS, readers=1)
        elapsed = time.monotonic() - t0
        check(problems == [], f"todos los procesos respondieron y terminaron con código 0 {problems}")
        used = {w["data"].get("busy_timeout") for w in writers}
        check(used == {STRESS_BUSY_TIMEOUT_MS}, f"los escritores usan el busy_timeout explícito de la sección D ({STRESS_BUSY_TIMEOUT_MS} ms; el de producción, 10000, se verifica en A, B y C) {sorted(map(str, used))}")
        errs = errors_of(writers) + errors_of(readers)
        check(errs == [], f"ningún worker recibió errores (incluido 'database is locked') {errs[:3]}")
        print(f"      ({N_WORKERS} escritores x {ROUNDS} rondas + 1 lector en {elapsed:.1f} s)")

        total = N_WORKERS * ROUNDS
        s = SQLiteStore(db)
        c = s._conn
        n = lambda t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]  # noqa: E731
        check(n("mandates") == total, f"mandatos: {n('mandates')} == {total} (ninguno perdido)")
        check(n("mandate_events") == total and
              c.execute("SELECT COUNT(*) FROM mandates m WHERE (SELECT COUNT(*) FROM mandate_events e WHERE e.mandate_id = m.mandate_id) != 1").fetchone()[0] == 0,
              "cada mandato tiene exactamente un evento (create_mandate atómico bajo contención)")
        check(n("effects") == total and c.execute("SELECT COUNT(DISTINCT effect_id) FROM effects").fetchone()[0] == total, f"ledger de efectos: {n('effects')} filas únicas == {total}")
        check(n("audit_log") == total, f"entradas de auditoría: {n('audit_log')} == {total}")
        seqs = [r[0] for r in c.execute("SELECT seq FROM audit_log ORDER BY seq")]
        check(seqs == list(range(1, total + 1)), "la secuencia de auditoría es contigua 1..N (sin huecos ni duplicados)")
        verdict = AuditChain(s).verify()
        check(verdict.status == AuditIntegrityState.VALID and verdict.total_entries == total,
              f"la cadena de auditoría escrita por {N_WORKERS} procesos es válida de extremo a extremo ({verdict})")
        cyc = s.get_cognitive_cycle("cyc-shared")
        bumps = sum(w["data"].get("bumps", 0) for w in writers)
        conflicts = sum(w["data"].get("conflicts", 0) for w in writers)
        check(bumps == total and cyc["cycle_version"] == 1 + total,
              f"Commit Gate: {bumps} avances == {total} y versión final {cyc['cycle_version']} == {1 + total} (sin actualizaciones perdidas)")
        print(f"      (reintentos por ConflictError del Commit Gate: {conflicts})")
        check(all(r["data"].get("monotonic") for r in readers) and sum(r["data"].get("reads", 0) for r in readers) > 0,
              f"el lector concurrente ({sum(r['data'].get('reads', 0) for r in readers)} lecturas) solo vio estados que crecen")
        check(pragma(c, "integrity_check") == "ok", "PRAGMA integrity_check = ok")
        check(c.execute("PRAGMA foreign_key_check").fetchall() == [], "PRAGMA foreign_key_check sin violaciones")
        expect_raises(sqlite3.DatabaseError, lambda: c.execute("UPDATE effects SET status = 'REJECTED'"), "append-only intacto tras la contención: UPDATE sobre effects sigue rechazado", "append-only")
        expect_raises(sqlite3.DatabaseError, lambda: c.execute("DELETE FROM audit_log"), "append-only intacto tras la contención: DELETE sobre audit_log sigue rechazado", "append-only")
        s.close()


def section_e_same_key_races() -> None:
    print("\n--- E. Carreras sobre la misma clave ---")
    with TempDir() as d:
        db = os.path.join(d, "e.db")
        s = SQLiteStore(db)
        seed_task(s, "task-e")
        for r in range(RACE_ROUNDS):
            seed_task(s, f"task-race-{r}")
            s.create_cognitive_cycle(f"cyc-race-{r}", f"task-race-{r}", "ACTIVE", "sh-0", "ch-0")
        s.close()

        writers, _, problems = run_workers("race", db, N_WORKERS, RACE_ROUNDS)
        check(problems == [], f"todos los procesos respondieron y terminaron con código 0 {problems}")
        check(errors_of(writers) == [], f"ningún worker falló fuera de los resultados esperados {errors_of(writers)[:3]}")
        expected_loser = {"genesis": "bootstrap_conflict", "effect": "conflict", "mandate": "integrity", "commit": "conflict"}
        for kind, loser in expected_loser.items():
            bad_rounds = []
            for r in range(RACE_ROUNDS):
                tally = Counter(w["data"]["outcomes"][kind][r] for w in writers if "outcomes" in w["data"])
                if tally != Counter({"ok": 1, loser: N_WORKERS - 1}):
                    bad_rounds.append((r, dict(tally)))
            check(bad_rounds == [], f"{kind}: en cada una de {RACE_ROUNDS} rondas hay exactamente 1 ganador y {N_WORKERS - 1} {loser} {bad_rounds[:2]}")
        s = SQLiteStore(db)
        c = s._conn
        one = lambda sql: c.execute(sql).fetchone()[0]  # noqa: E731
        check(one("SELECT COUNT(*) FROM authorities WHERE is_genesis = 1") == RACE_ROUNDS, "una sola autoridad génesis por instalación (P-063) aun con procesos en paralelo")
        check(one("SELECT COUNT(*) FROM effects WHERE provider = 'prov-race'") == RACE_ROUNDS, "un solo efecto por (provider, idempotency_key)")
        check(one("SELECT COUNT(*) FROM mandates") == RACE_ROUNDS and one("SELECT COUNT(*) FROM mandate_events") == RACE_ROUNDS,
              "un solo mandato por Task y ningún evento huérfano de los intentos perdedores")
        check(all(s.get_cognitive_cycle(f"cyc-race-{r}")["cycle_version"] == 2 for r in range(RACE_ROUNDS)), "cada ciclo avanzó exactamente una versión")
        check(pragma(c, "integrity_check") == "ok", "PRAGMA integrity_check = ok")
        s.close()


def make_legacy_db(db: str) -> str | None:
    """Fabrica una base con el esquema de antes de los parches 0003 (effects.evidence) y 0008 (policies.content), a partir del schema.sql
    actual. Devuelve None si lo logró o el motivo si no pudo (p. ej. cambió el formato de schema.sql): nunca finge una base antigua."""
    schema = (Path(__file__).resolve().parents[2] / "src" / "dtos" / "schema.sql").read_text(encoding="utf-8")
    legacy = re.sub(r",\n    evidence              TEXT[^\n]*\n\);", "\n);", schema, count=1)
    legacy = re.sub(r"    content         TEXT,\n", "", legacy, count=1)
    with closing(sqlite3.connect(db)) as conn:
        conn.executescript(legacy)
        effects = {r[1] for r in conn.execute("PRAGMA table_info(effects)")}
        policies = {r[1] for r in conn.execute("PRAGMA table_info(policies)")}
    if "evidence" in effects or "content" in policies:
        return "no se pudo quitar effects.evidence / policies.content de schema.sql (¿cambió su formato?)"
    return None


def section_f_legacy_migration() -> None:
    print("\n--- F. Migración de una base antigua abierta a la vez ---")
    problems: list[str] = []
    errors: list[str] = []
    with TempDir() as d:
        for k in range(FRESH_DBS):
            db = os.path.join(d, f"legacy-{k}.db")
            why = make_legacy_db(db)
            if why:
                check(False, f"preparación de la base antigua #{k}: {why}")
                return
            writers, _, probs = run_workers("open_legacy", db, FRESH_PROCS, 0)
            problems += probs
            errors += errors_of(writers)
            with closing(sqlite3.connect(db)) as conn:
                effects = {r[1] for r in conn.execute("PRAGMA table_info(effects)")}
                policies = {r[1] for r in conn.execute("PRAGMA table_info(policies)")}
            check("evidence" in effects and "content" in policies, f"base antigua #{k}: ambas columnas existen tras {FRESH_PROCS} aperturas simultáneas")
    check(problems == [], f"todos los procesos respondieron y terminaron con código 0 {problems}")
    check(errors == [], f"migrar la misma base antigua desde {FRESH_PROCS} procesos a la vez no produce errores {errors[:3]}")


def guarded(name: str, fn) -> None:
    try:
        fn()
    except Exception as exc:
        check(False, f"{name}: la sección no pudo ejecutarse ({type(exc).__name__}: {exc})")


def main() -> int:
    guarded("A", section_a_config)
    guarded("B", section_b_timeout_semantics)
    guarded("C", section_c_open_fresh)
    guarded("D", section_d_stress)
    guarded("E", section_e_same_key_races)
    guarded("F", section_f_legacy_migration)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} FALLO(S):")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print("test_concurrency: todo OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
