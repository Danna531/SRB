import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

contexto = zmq.Context()
socket = contexto.socket(zmq.REQ)
socket.connect(config.addr(config.HOST_PERSISTENCIA, config.PUERTO_PERSISTENCIA))


def enviar(msg):
    socket.send_json(msg)
    r = socket.recv_json()
    print(f"{msg['op']:<18} -> {r['status']} | {r['detalle']} | {r['data'] if r['data'] and msg['op'] != 'LISTAR_EVENTOS' else ''}")
    return r


r = enviar({"op": "LISTAR_EVENTOS", "mes": 11})
print("   (primeras 3 ocurrencias:", r["data"]["eventos"][:3], ")")

enviar({"op": "DISPONIBILIDAD", "evento_id": "E1", "fecha": "2026-11-05"})
r = enviar({"op": "CREAR_RESERVA", "cliente_id": "C1", "evento_id": "E1", "fecha": "2026-11-05", "asientos": 2})
rid = r["data"]["reserva_id"]
enviar({"op": "DISPONIBILIDAD", "evento_id": "E1", "fecha": "2026-11-05"})
enviar({"op": "MODIFICAR_RESERVA", "reserva_id": rid, "nuevos_asientos": 3})
enviar({"op": "MODIFICAR_RESERVA", "reserva_id": rid, "nueva_fecha": "2026-11-19"})
enviar({"op": "DISPONIBILIDAD", "evento_id": "E1", "fecha": "2026-11-19"})
enviar({"op": "LIBERAR_RESERVA", "reserva_id": rid})
enviar({"op": "DISPONIBILIDAD", "evento_id": "E1", "fecha": "2026-11-19"})
enviar({"op": "CREAR_RESERVA", "cliente_id": "C1", "evento_id": "E1", "fecha": "2026-11-05", "asientos": 500})