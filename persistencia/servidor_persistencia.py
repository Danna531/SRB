"""
PERSISTENCIA
Unico componente que toca la base de datos (SQLite, modo WAL). Hay dos nodos, A y B, con replicacion
primario-secundario: el primario aplica cada escritura en una transaccion, la envia al secundario y
solo entonces hace COMMIT. Si el secundario cae, el primario sigue en modo degradado; si cae el
primario, el secundario se promueve (heartbeat o primera escritura). Cada escritura se numera en
la tabla log_ops para poder poner al dia al nodo que vuelve. Con SRB_BASE=1 solo existe el nodo A.
Uso: python3 persistencia/servidor_persistencia.py A|B
"""
import json
import os
import sqlite3
import sys
import threading
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

CARPETA = os.path.dirname(os.path.abspath(__file__))
ANIO = 2026
HILOS = 8
TIMEOUT_REPLICA_MS = 1000
TIMEOUT_PING_MS = 1000
PERIODO_HEARTBEAT_S = 1.0
FALLOS_PARA_PROMOVER = 3

SILENCIO = bool(os.environ.get("SRB_SILENCIO"))

LECTURAS = {"LISTAR_EVENTOS", "DISPONIBILIDAD"}
ESCRITURAS = {"CREAR_RESERVA", "MODIFICAR_RESERVA", "LIBERAR_RESERVA"}

contexto = zmq.Context.instance()

NODO = "A"
PEER = "B"
RUTA_BD = ""

lock_escritura = threading.RLock()
local = threading.local()
con_escritura = None
estado = {"rol": "SECUNDARIO", "seq": 0, "peer_vivo": False}


def log(texto):
    """Imprime una linea de registro con el nodo y la hora (se silencia con SRB_SILENCIO=1)."""
    if not SILENCIO:
        print(f"[PERSISTENCIA-{NODO}] {time.strftime('%H:%M:%S')} {texto}", flush=True)


def ok(detalle="", data=None):
    """Respuesta de exito."""
    return {"status": "OK", "detalle": detalle, "data": data or {}}


def rechazado(detalle):
    """Respuesta de rechazo por regla de negocio (p. ej. sin asientos)."""
    return {"status": "RECHAZADO", "detalle": detalle, "data": {}}


def error(detalle):
    """Respuesta de error tecnico."""
    return {"status": "ERROR", "detalle": detalle, "data": {}}


def get_ocurrencia(cur, evento_id, fecha):
    """Busca una ocurrencia (evento + fecha) y devuelve (id, libres, ocupados) o None."""
    return cur.execute(
        "SELECT ocurrencia_id, asientos_libres, asientos_ocupados "
        "FROM ocurrencias WHERE evento_id=? AND fecha=?",
        (evento_id, fecha),
    ).fetchone()


def listar_eventos(cur, d):
    """Lista las ocurrencias de un mes con sus asientos."""
    mes = int(d["mes"])
    patron = f"{ANIO}-{mes:02d}-%"
    filas = cur.execute(
        "SELECT evento_id, fecha, asientos_libres, asientos_ocupados "
        "FROM ocurrencias WHERE fecha LIKE ? ORDER BY evento_id, fecha",
        (patron,),
    ).fetchall()
    eventos = [
        {"evento_id": f[0], "fecha": f[1], "asientos_libres": f[2], "asientos_ocupados": f[3]}
        for f in filas
    ]
    return ok(f"{len(eventos)} ocurrencias", {"eventos": eventos})


def disponibilidad(cur, d):
    """Asientos libres y ocupados de una ocurrencia."""
    o = get_ocurrencia(cur, d["evento_id"], d["fecha"])
    if o is None:
        return rechazado("El evento o la fecha no existe")
    return ok("", {"asientos_libres": o[1], "asientos_ocupados": o[2]})


def crear_reserva(cur, d):
    """Descuenta los asientos de la ocurrencia y registra la reserva."""
    asientos = int(d["asientos"])
    if asientos <= 0:
        return rechazado("Número de asientos inválido")
    o = get_ocurrencia(cur, d["evento_id"], d["fecha"])
    if o is None:
        return rechazado("El evento o la fecha no existe")
    ocurrencia_id, libres, _ = o
    if libres < asientos:
        return rechazado(f"No hay asientos suficientes (libres: {libres})")
    cur.execute(
        "UPDATE ocurrencias SET asientos_libres = asientos_libres - ?, "
        "asientos_ocupados = asientos_ocupados + ? WHERE ocurrencia_id=?",
        (asientos, asientos, ocurrencia_id),
    )
    cur.execute(
        "INSERT INTO reservas (cliente_id, ocurrencia_id, asientos) VALUES (?, ?, ?)",
        (d["cliente_id"], ocurrencia_id, asientos),
    )
    return ok("Reserva creada", {"reserva_id": cur.lastrowid})


def modificar_reserva(cur, d):
    """Cambia los asientos y/o la fecha de una reserva activa, ajustando las ocurrencias."""
    r = cur.execute(
        "SELECT r.asientos, r.estado, o.evento_id, o.fecha, o.ocurrencia_id "
        "FROM reservas r JOIN ocurrencias o ON r.ocurrencia_id = o.ocurrencia_id "
        "WHERE r.reserva_id=?",
        (d["reserva_id"],),
    ).fetchone()
    if r is None or r[1] != "ACTIVA":
        return rechazado("La reserva no existe o no está activa")
    actual, _, evento_id, fecha_actual, ocurrencia_actual = r

    nuevos = int(d.get("nuevos_asientos") or actual)
    nueva_fecha = d.get("nueva_fecha") or fecha_actual
    if nuevos <= 0:
        return rechazado("Número de asientos inválido")

    if nueva_fecha == fecha_actual:
        delta = nuevos - actual
        libres = cur.execute(
            "SELECT asientos_libres FROM ocurrencias WHERE ocurrencia_id=?",
            (ocurrencia_actual,),
        ).fetchone()[0]
        if delta > libres:
            return rechazado(f"No hay asientos suficientes (libres: {libres})")
        cur.execute(
            "UPDATE ocurrencias SET asientos_libres = asientos_libres - ?, "
            "asientos_ocupados = asientos_ocupados + ? WHERE ocurrencia_id=?",
            (delta, delta, ocurrencia_actual),
        )
        cur.execute("UPDATE reservas SET asientos=? WHERE reserva_id=?", (nuevos, d["reserva_id"]))
        return ok("Reserva modificada")

    # Cambio de fecha (otra ocurrencia del mismo evento)
    nueva = get_ocurrencia(cur, evento_id, nueva_fecha)
    if nueva is None:
        return rechazado("No existe esa otra ocurrencia del evento")
    nueva_id, libres_nueva, _ = nueva
    if libres_nueva < nuevos:
        return rechazado(f"No hay asientos en la nueva fecha (libres: {libres_nueva})")
    cur.execute(
        "UPDATE ocurrencias SET asientos_libres = asientos_libres + ?, "
        "asientos_ocupados = asientos_ocupados - ? WHERE ocurrencia_id=?",
        (actual, actual, ocurrencia_actual),
    )
    cur.execute(
        "UPDATE ocurrencias SET asientos_libres = asientos_libres - ?, "
        "asientos_ocupados = asientos_ocupados + ? WHERE ocurrencia_id=?",
        (nuevos, nuevos, nueva_id),
    )
    cur.execute(
        "UPDATE reservas SET ocurrencia_id=?, asientos=? WHERE reserva_id=?",
        (nueva_id, nuevos, d["reserva_id"]),
    )
    return ok("Reserva movida a otra fecha")


def liberar_reserva(cur, d):
    """Cancela una reserva activa y devuelve sus asientos a la ocurrencia."""
    r = cur.execute(
        "SELECT asientos, estado, ocurrencia_id FROM reservas WHERE reserva_id=?",
        (d["reserva_id"],),
    ).fetchone()
    if r is None or r[1] != "ACTIVA":
        return rechazado("La reserva no existe o ya fue cancelada")
    asientos, _, ocurrencia_id = r
    cur.execute(
        "UPDATE ocurrencias SET asientos_libres = asientos_libres + ?, "
        "asientos_ocupados = asientos_ocupados - ? WHERE ocurrencia_id=?",
        (asientos, asientos, ocurrencia_id),
    )
    cur.execute("UPDATE reservas SET estado='CANCELADA' WHERE reserva_id=?", (d["reserva_id"],))
    return ok("Reserva liberada")


OPERACIONES = {
    "LISTAR_EVENTOS": listar_eventos,
    "DISPONIBILIDAD": disponibilidad,
    "CREAR_RESERVA": crear_reserva,
    "MODIFICAR_RESERVA": modificar_reserva,
    "LIBERAR_RESERVA": liberar_reserva,
}


def abrir_bd(solo_lectura=False):
    """Abre la BD en modo WAL; las conexiones de lectura se abren como solo lectura."""
    con = sqlite3.connect(RUTA_BD, check_same_thread=False, isolation_level=None, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    if solo_lectura:
        con.execute("PRAGMA query_only=ON")
    return con


def con_lectura():
    """Conexion de solo lectura del hilo actual (una por hilo)."""
    if not hasattr(local, "con"):
        local.con = abrir_bd(solo_lectura=True)
    return local.con


def ejecutar_escritura(con, msg):
    """Ejecuta una escritura en la transaccion abierta. Si el id ya se proceso devuelve la respuesta guardada (idempotencia) en vez de repetirla."""
    op = msg["op"]
    id_ = msg.get("id")
    cur = con.cursor()
    if id_:
        anterior = cur.execute(
            "SELECT operacion, respuesta FROM solicitudes_procesadas WHERE id=?", (id_,)
        ).fetchone()
        if anterior is not None:
            if anterior[0] != op:
                return error("Identificador utilizado para otra operacion"), True
            return json.loads(anterior[1]), True
    respuesta = OPERACIONES[op](cur, msg)
    if id_:
        cur.execute(
            "INSERT INTO solicitudes_procesadas (id, operacion, respuesta) VALUES (?, ?, ?)",
            (id_, op, json.dumps(respuesta)),
        )
    return respuesta, False


def direccion_repl_peer():
    """Direccion del puerto de replicacion del otro nodo."""
    n = config.NODOS_PERSISTENCIA[PEER]
    return config.addr(n["host"], n["puerto_repl"])


def socket_req(direccion, timeout_ms):
    """Crea un REQ con limites de espera hacia una direccion."""
    s = contexto.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, timeout_ms)
    s.setsockopt(zmq.SNDTIMEO, timeout_ms)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(direccion)
    return s


def ping_peer():
    """Pregunta al otro nodo su rol y su seq; devuelve None si no responde o si no hay otro nodo."""
    if PEER is None:
        return None
    s = socket_req(direccion_repl_peer(), TIMEOUT_PING_MS)
    try:
        s.send_json({"tipo": "PING"})
        return s.recv_json()
    except zmq.ZMQError:
        return None
    finally:
        s.close()


sock_replica = None


def enviar_replica(mensaje):
    """Envia un mensaje de replicacion al secundario; devuelve None si no responde a tiempo."""
    global sock_replica
    if sock_replica is None:
        sock_replica = socket_req(direccion_repl_peer(), TIMEOUT_REPLICA_MS)
    try:
        sock_replica.send_json(mensaje)
        return sock_replica.recv_json()
    except zmq.ZMQError:
        sock_replica.close()
        sock_replica = None
        return None


def replicar(con, seq, msg):
    """Envia la escritura al secundario (y lo pone al dia si hace falta). Devuelve OK, DEGRADADO (secundario caido) o ABORTAR (el otro nodo es el primario)."""
    if not estado["peer_vivo"]:
        return "DEGRADADO"
    resp = enviar_replica({"tipo": "REPLICAR", "seq": seq, "op": msg})
    if resp is None:
        estado["peer_vivo"] = False
        log("Secundario sin respuesta: modo degradado (se pondra al dia cuando vuelva)")
        return "DEGRADADO"
    if resp.get("tipo") == "RECHAZO_PRIMARIO":
        return "ABORTAR"
    if resp.get("tipo") == "NEED_SYNC":
        desde = resp["ultimo"]
        while desde < seq:
            filas = con.execute(
                "SELECT seq, op FROM log_ops WHERE seq>? ORDER BY seq LIMIT 200", (desde,)
            ).fetchall()
            if not filas:
                break
            r = enviar_replica({"tipo": "LOTE", "ops": [[f[0], json.loads(f[1])] for f in filas]})
            if r is None:
                estado["peer_vivo"] = False
                return "DEGRADADO"
            if r.get("tipo") == "RECHAZO_PRIMARIO":
                return "ABORTAR"
            desde = filas[-1][0]
        log(f"Secundario puesto al dia hasta seq={seq}")
    return "OK"


def escribir_primario(msg):
    """Escritura en el primario: transaccion, bitacora, replicacion y COMMIT, todo bajo el lock de escritura global."""
    with lock_escritura:
        con = con_escritura
        con.execute("BEGIN IMMEDIATE")
        try:
            resp, repetida = ejecutar_escritura(con, msg)
            if repetida:
                con.execute("ROLLBACK")
                log(f"Solicitud repetida: {msg.get('id')}")
                return resp
            seq = estado["seq"] + 1
            con.execute("INSERT INTO log_ops (seq, op) VALUES (?, ?)", (seq, json.dumps(msg)))
            resultado = replicar(con, seq, msg)
            if resultado == "ABORTAR":
                con.execute("ROLLBACK")
                estado["rol"] = "SECUNDARIO"
                log("El otro nodo es el primario: me degrado a secundario")
                return error("NO_PRIMARIO")
            con.execute("COMMIT")
            estado["seq"] = seq
            return resp
        except Exception:
            con.execute("ROLLBACK")
            raise


def aplicar_replica(seq, msg):
    """Aplica en este nodo una operacion replicada y la registra en la bitacora."""
    con = con_escritura
    con.execute("BEGIN IMMEDIATE")
    try:
        ejecutar_escritura(con, msg)
        con.execute("INSERT INTO log_ops (seq, op) VALUES (?, ?)", (seq, json.dumps(msg)))
        con.execute("COMMIT")
        estado["seq"] = seq
    except Exception:
        con.execute("ROLLBACK")
        raise


def aplicar_lote(ops):
    """Aplica operaciones replicadas en orden; pide NEED_SYNC si falta alguna y rechaza si este nodo es el primario."""
    with lock_escritura:
        if estado["rol"] == "PRIMARIO":
            return {"tipo": "RECHAZO_PRIMARIO", "seq": estado["seq"]}
        for seq, op in ops:
            if seq <= estado["seq"]:
                continue
            if seq != estado["seq"] + 1:
                return {"tipo": "NEED_SYNC", "ultimo": estado["seq"]}
            aplicar_replica(seq, op)
            log(f"Replica aplicada seq={seq} {op.get('op')} id={op.get('id')}")
        return {"tipo": "ACK", "seq": estado["seq"]}


def manejar_replicacion(msg):
    """Atiende los mensajes entre nodos: PING, REPLICAR, LOTE y SYNC."""
    tipo = msg.get("tipo")
    if tipo == "PING":
        return {"tipo": "PONG", "rol": estado["rol"], "seq": estado["seq"]}
    if tipo == "REPLICAR":
        return aplicar_lote([[msg["seq"], msg["op"]]])
    if tipo == "LOTE":
        return aplicar_lote(msg["ops"])
    if tipo == "SYNC":
        filas = con_lectura().execute(
            "SELECT seq, op FROM log_ops WHERE seq>? ORDER BY seq LIMIT 200", (msg["desde"],)
        ).fetchall()
        return {"tipo": "OPS", "ops": [[f[0], json.loads(f[1])] for f in filas],
                "rol": estado["rol"], "seq": estado["seq"]}
    return {"tipo": "ERROR"}


def servidor_replicacion(puerto):
    """Hilo que escucha en el puerto de replicacion."""
    s = contexto.socket(zmq.REP)
    s.bind("tcp://*:%d" % puerto)
    while True:
        try:
            msg = s.recv_json()
            resp = manejar_replicacion(msg)
        except Exception as e:
            resp = {"tipo": "ERROR", "detalle": str(e)}
        s.send_json(resp)


def sincronizar_desde_peer():
    """Pide al otro nodo las operaciones que le faltan hasta quedar al dia."""
    s = socket_req(direccion_repl_peer(), 3000)
    try:
        while True:
            s.send_json({"tipo": "SYNC", "desde": estado["seq"]})
            r = s.recv_json()
            if not r["ops"]:
                return
            with lock_escritura:
                for seq, op in r["ops"]:
                    if seq == estado["seq"] + 1:
                        aplicar_replica(seq, op)
            log(f"Sincronizado hasta seq={estado['seq']}")
    except zmq.ZMQError:
        pass
    finally:
        s.close()


def promover(motivo):
    """Convierte este nodo en primario."""
    with lock_escritura:
        if estado["rol"] != "PRIMARIO":
            estado["rol"] = "PRIMARIO"
            log(f"*** PROMOVIDO A PRIMARIO ({motivo}) en seq={estado['seq']} ***")


def heartbeat():
    """Hilo que hace ping al otro nodo cada segundo: promueve tras varios fallos, resuelve el caso de dos primarios y sincroniza."""
    fallos = 0
    while True:
        time.sleep(PERIODO_HEARTBEAT_S)
        pong = ping_peer()
        estado["peer_vivo"] = pong is not None
        if pong is None:
            fallos += 1
            if estado["rol"] == "SECUNDARIO" and fallos >= FALLOS_PARA_PROMOVER:
                promover("el primario no responde")
            continue
        fallos = 0
        if estado["rol"] == "PRIMARIO" and pong.get("rol") == "PRIMARIO":
            if pong["seq"] > estado["seq"] or (pong["seq"] == estado["seq"] and NODO == "B"):
                with lock_escritura:
                    estado["rol"] = "SECUNDARIO"
                log("Habia dos primarios: me degrado a secundario")
        if estado["rol"] == "SECUNDARIO" and pong["seq"] > estado["seq"]:
            sincronizar_desde_peer()


def atender(msg):
    """Decide que hacer segun la operacion: lecturas locales, escrituras solo en el primario."""
    op = msg.get("op")
    if op in LECTURAS:
        return OPERACIONES[op](con_lectura().cursor(), msg)
    if op in ESCRITURAS:
        if estado["rol"] != "PRIMARIO":
            pong = ping_peer()
            if pong is not None and pong.get("rol") == "PRIMARIO":
                return error("NO_PRIMARIO")
            promover("llego una escritura y el primario no responde")
        return escribir_primario(msg)
    if op == "ESTADO":
        return ok("", {"nodo": NODO, "rol": estado["rol"], "seq": estado["seq"]})
    return error(f"Operacion desconocida: {op}")


def trabajador():
    """Hilo trabajador: recibe del DEALER, atiende y responde siempre."""
    s = contexto.socket(zmq.REP)
    s.connect("inproc://trabajadores")
    while True:
        try:
            msg = s.recv_json()
            inicio = time.perf_counter()
            try:
                resp = atender(msg)
            except KeyError as e:
                resp = error(f"Falta el campo {e}")
            except Exception as e:
                resp = error(str(e))
            resp["id"] = msg.get("id")
            if msg.get("op") in ESCRITURAS or msg.get("op") == "ESTADO":
                log(f"{msg.get('op')} id={msg.get('id')} -> {resp['status']} {resp['detalle']} "
                    f"({(time.perf_counter() - inicio) * 1000:.1f} ms)")
        except Exception as e:
            resp = error(f"Mensaje invalido: {e}")
        s.send_json(resp)


def main():
    """Arranca el nodo (A o B): abre la BD, decide su rol inicial y lanza replicacion, heartbeat y los hilos trabajadores."""
    global NODO, PEER, RUTA_BD, con_escritura
    NODO = sys.argv[1].upper() if len(sys.argv) > 1 else "A"
    if NODO not in config.NODOS_PERSISTENCIA:
        print("Uso: python persistencia/servidor_persistencia.py [A|B]")
        return
    PEER = "B" if NODO == "A" else "A"
    if PEER not in config.NODOS_PERSISTENCIA:
        PEER = None
    RUTA_BD = os.path.join(CARPETA, f"srb_{NODO}.db")
    if not os.path.exists(RUTA_BD):
        print(f"No existe {RUTA_BD}. Ejecuta primero: python persistencia/crear_bd.py")
        return

    con_escritura = abrir_bd()
    estado["seq"] = con_escritura.execute("SELECT COALESCE(MAX(seq), 0) FROM log_ops").fetchone()[0]

    pong = ping_peer()
    estado["peer_vivo"] = pong is not None
    if pong is not None and pong.get("rol") == "PRIMARIO":
        estado["rol"] = "SECUNDARIO"
    elif NODO == "A":
        estado["rol"] = "PRIMARIO"
    else:
        estado["rol"] = "SECUNDARIO"

    puertos = config.NODOS_PERSISTENCIA[NODO]
    threading.Thread(target=servidor_replicacion, args=(puertos["puerto_repl"],), daemon=True).start()
    if estado["rol"] == "SECUNDARIO" and pong is not None:
        sincronizar_desde_peer()
    if PEER is not None:
        threading.Thread(target=heartbeat, daemon=True).start()

    frontal = contexto.socket(zmq.ROUTER)
    frontal.bind("tcp://*:%d" % puertos["puerto"])
    trasero = contexto.socket(zmq.DEALER)
    trasero.bind("inproc://trabajadores")
    for _ in range(HILOS):
        threading.Thread(target=trabajador, daemon=True).start()

    print(f"Persistencia {NODO} ({estado['rol']}, seq={estado['seq']}) atendiendo clientes en el puerto "
          f"{puertos['puerto']} y replicacion en {puertos['puerto_repl']}...", flush=True)
    try:
        zmq.proxy(frontal, trasero)
    except KeyboardInterrupt:
        print(f"\nPersistencia {NODO} detenida")


if __name__ == "__main__":
    main()
