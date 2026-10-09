import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

TIMEOUT_MS = 3000  # tiempo máximo de espera a Persistencia

contexto = zmq.Context()


def nuevo_socket_persistencia():
    s = contexto.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
    s.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(config.addr(config.HOST_PERSISTENCIA, config.PUERTO_PERSISTENCIA))
    return s


sock_p = nuevo_socket_persistencia()


def llamar_persistencia(msg):
    """Envía un mensaje a Persistencia. Si no responde a tiempo, recrea el socket."""
    global sock_p
    try:
        sock_p.send_json(msg)
        return sock_p.recv_json()
    except zmq.Again:
        sock_p.close()
        sock_p = nuevo_socket_persistencia()
        return {"status": "ERROR", "detalle": "Persistencia no responde", "data": {}}


def consulta(m):
    return llamar_persistencia({"op": "LISTAR_EVENTOS", "mes": m["mes"]})


def reserva(m):
    # 1) Consultar disponibilidad en tiempo real
    disp = llamar_persistencia(
        {"op": "DISPONIBILIDAD", "evento_id": m["evento_id"], "fecha": m["fecha"]}
    )
    if disp["status"] != "OK":
        return disp
    libres = disp["data"]["asientos_libres"]
    if libres < int(m["asientos"]):
        return {
            "status": "RECHAZADO",
            "detalle": f"No hay asientos suficientes (libres: {libres})",
            "data": {},
        }
    # 2) Confirmar la reserva en la BD
    return llamar_persistencia(
        {
            "op": "CREAR_RESERVA",
            "cliente_id": m["cliente_id"],
            "evento_id": m["evento_id"],
            "fecha": m["fecha"],
            "asientos": m["asientos"],
        }
    )


def modificacion(m):
    # Persistencia valida disponibilidad y aplica el cambio de forma atómica
    return llamar_persistencia(
        {
            "op": "MODIFICAR_RESERVA",
            "reserva_id": m["reserva_id"],
            "nuevos_asientos": m.get("nuevos_asientos"),
            "nueva_fecha": m.get("nueva_fecha"),
        }
    )


OPERACIONES = {
    "CONSULTA": consulta,
    "RESERVA": reserva,
    "MODIFICACION": modificacion,
}


def main():
    socket = contexto.socket(zmq.REP)
    socket.bind("tcp://*:%d" % config.PUERTO_RMC)
    print(f"RMC escuchando en el puerto {config.PUERTO_RMC}...")

    while True:
        msg = socket.recv_json()
        print(f"[RMC] Recibí: {msg}")

        funcion = OPERACIONES.get(msg.get("op"))
        if funcion is None:
            respuesta = {"status": "ERROR", "detalle": f"Operación desconocida: {msg.get('op')}", "data": {}}
        else:
            try:
                respuesta = funcion(msg)
            except KeyError as e:
                respuesta = {"status": "ERROR", "detalle": f"Falta el campo {e}", "data": {}}
            except Exception as e:
                respuesta = {"status": "ERROR", "detalle": str(e), "data": {}}

        respuesta["id"] = msg.get("id")
        print(f"[RMC] Respondí: {respuesta['status']} - {respuesta['detalle']}")
        socket.send_json(respuesta)


if __name__ == "__main__":
    main()
    