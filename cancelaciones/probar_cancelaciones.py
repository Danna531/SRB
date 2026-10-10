
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

PUERTO_NOTIF = 6001
NOTIF_ADDR = f"tcp://localhost:{PUERTO_NOTIF}"

contexto = zmq.Context()

# Hacia Persistencia (para crear la reserva de prueba)
persistencia = contexto.socket(zmq.REQ)
persistencia.connect(config.addr(config.HOST_PERSISTENCIA, config.PUERTO_PERSISTENCIA))

# Hacia Cancelaciones (lo que haria el GT)
hacia_cancelaciones = contexto.socket(zmq.PUSH)
hacia_cancelaciones.connect(config.addr(config.HOST_CANCELACIONES, config.PUERTO_CANCELACIONES))

# Buzon del cliente donde va a llegar la notificacin
buzon = contexto.socket(zmq.PULL)
buzon.bind("tcp://*:%d" % PUERTO_NOTIF)
buzon.setsockopt(zmq.RCVTIMEO, 5000)


def cancelar(id_, reserva_id):
    hacia_cancelaciones.send_json({"id": id_, "op": "CANCELACION", "cliente_id": "C1",
                                   "reserva_id": reserva_id, "notif_addr": NOTIF_ADDR})
    try:
        print("  notificacion:", buzon.recv_json())
    except zmq.Again:
        print("  NO llego la notificacion en 5 segs (esta encendido Cancelaciones?)")


persistencia.send_json({"op": "CREAR_RESERVA", "cliente_id": "C1", "evento_id": "E1",
                        "fecha": "2026-11-05", "asientos": 2})
r = persistencia.recv_json()
print("CREAR_RESERVA ->", r["status"], r["data"])
reserva_id = r["data"]["reserva_id"]

print(f"Cancelar la reserva {reserva_id} (debe mostrar OK):")
cancelar("prueba-1", reserva_id)

print("Cancelar la reserva otra vez (debe mostrar que fue RECHAZADO):")
cancelar("prueba-2", reserva_id)
