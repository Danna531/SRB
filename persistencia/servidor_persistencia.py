import os
import sys
import sqlite3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

RUTA_BD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "srb.db")
ANIO = 2026


def ok(detalle="", data=None):
    return {"status": "OK", "detalle": detalle, "data": data or {}}


def rechazado(detalle):
    return {"status": "RECHAZADO", "detalle": detalle, "data": {}}


def get_ocurrencia(cur, evento_id, fecha):
    return cur.execute(
        "SELECT ocurrencia_id, asientos_libres, asientos_ocupados "
        "FROM ocurrencias WHERE evento_id=? AND fecha=?",
        (evento_id, fecha),
    ).fetchone()


def listar_eventos(cur, d):
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
    o = get_ocurrencia(cur, d["evento_id"], d["fecha"])
    if o is None:
        return rechazado("El evento o la fecha no existe")
    return ok("", {"asientos_libres": o[1], "asientos_ocupados": o[2]})


def crear_reserva(cur, d):
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


def main():
    if not os.path.exists(RUTA_BD):
        print("No existe srb.db. Ejecuta primero: python persistencia/crear_bd.py")
        return

    con = sqlite3.connect(RUTA_BD)
    contexto = zmq.Context()
    socket = contexto.socket(zmq.REP)
    socket.bind("tcp://*:%d" % config.PUERTO_PERSISTENCIA)
    print(f"Persistencia escuchando en el puerto {config.PUERTO_PERSISTENCIA}...")

    while True:
        msg = socket.recv_json()
        op = msg.get("op")
        print(f"[PERSISTENCIA] Recibí: {msg}")

        funcion = OPERACIONES.get(op)
        if funcion is None:
            respuesta = {"status": "ERROR", "detalle": f"Operación desconocida: {op}", "data": {}}
        else:
            try:
                cur = con.cursor()
                respuesta = funcion(cur, msg)
                if respuesta["status"] == "OK":
                    con.commit()
                else:
                    con.rollback()
            except Exception as e:
                con.rollback()
                respuesta = {"status": "ERROR", "detalle": str(e), "data": {}}

        respuesta["id"] = msg.get("id")
        print(f"[PERSISTENCIA] Respondí: {respuesta['status']} - {respuesta['detalle']}")
        socket.send_json(respuesta)


if __name__ == "__main__":
    main()
    