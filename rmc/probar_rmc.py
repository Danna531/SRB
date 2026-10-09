import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

contexto = zmq.Context()
socket = contexto.socket(zmq.REQ)
socket.connect(config.addr(config.HOST_RMC, config.PUERTO_RMC))


def enviar(msg):
    socket.send_json(msg)
    r = socket.recv_json()
    extra = r["data"] if r["data"] and msg["op"] != "CONSULTA" else ""
    print(f"{msg['op']:<13} -> {r['status']} | {r['detalle']} | {extra}")
    return r


enviar({"id": "1", "op": "CONSULTA", "mes": 11})
r = enviar({"id": "2", "op": "RESERVA", "cliente_id": "C1", "evento_id": "E1", "fecha": "2026-11-05", "asientos": 2})
rid = r["data"]["reserva_id"]
enviar({"id": "3", "op": "MODIFICACION", "reserva_id": rid, "nuevos_asientos": 3})
enviar({"id": "4", "op": "MODIFICACION", "reserva_id": rid, "nueva_fecha": "2026-11-19"})
enviar({"id": "5", "op": "RESERVA", "cliente_id": "C1", "evento_id": "E1", "fecha": "2026-11-05", "asientos": 500})
enviar({"id": "6", "op": "MODIFICACION", "reserva_id": 999, "nuevos_asientos": 2})
enviar({"id": "7", "op": "BAILAR"})