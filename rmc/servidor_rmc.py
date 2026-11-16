import os
import sys
import threading
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config
from cliente_persistencia import ClientePersistencia

SILENCIO = bool(os.environ.get("SRB_SILENCIO"))

contexto = zmq.Context.instance()
local = threading.local()


def persistencia():
    if not hasattr(local, "p"):
        local.p = ClientePersistencia()
    return local.p


def llamar_persistencia(msg):
    return persistencia().llamar(msg)


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
    return llamar_persistencia(
        {
            "op": "CREAR_RESERVA",
            "id": m["id"],
            "cliente_id": m["cliente_id"],
            "evento_id": m["evento_id"],
            "fecha": m["fecha"],
            "asientos": m["asientos"],
        }
    )


def modificacion(m):
    return llamar_persistencia(
        {
            "op": "MODIFICAR_RESERVA",
            "id": m["id"],
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


def procesar(msg):
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
    return respuesta


def trabajador(puerto):
    s = contexto.socket(zmq.REP)
    s.connect("inproc://rmc-trabajadores")
    while True:
        try:
            msg = s.recv_json()
            inicio = time.perf_counter()
            respuesta = procesar(msg)
            if not SILENCIO:
                print(f"[RMC {puerto}] {time.strftime('%H:%M:%S')} {msg.get('op')} id={msg.get('id')} "
                      f"-> {respuesta['status']} - {respuesta['detalle']} "
                      f"({(time.perf_counter() - inicio) * 1000:.1f} ms)", flush=True)
        except Exception as e:
            respuesta = {"status": "ERROR", "detalle": f"Mensaje invalido: {e}", "data": {}}
        s.send_json(respuesta)


def main():
    puerto = int(sys.argv[1]) if len(sys.argv) > 1 else config.PUERTO_RMC
    hilos = int(sys.argv[2]) if len(sys.argv) > 2 else config.HILOS_RMC

    frontal = contexto.socket(zmq.ROUTER)
    frontal.bind("tcp://*:%d" % puerto)
    trasero = contexto.socket(zmq.DEALER)
    trasero.bind("inproc://rmc-trabajadores")
    for _ in range(hilos):
        threading.Thread(target=trabajador, args=(puerto,), daemon=True).start()

    print(f"RMC escuchando en el puerto {puerto} con {hilos} hilos...", flush=True)
    try:
        zmq.proxy(frontal, trasero)
    except KeyboardInterrupt:
        print("\nRMC detenido")


if __name__ == "__main__":
    main()
