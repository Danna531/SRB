"""
CLIENTE
La clase Cliente tambien la usa el generador de carga.
"""
import json
import os
import sys
import time
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

TIMEOUT_MS = 8000  # el cliente espera mas que el GT (5 s) y que el RMC (3 s), asi recibe su ERROR antes de rendirse


class Cliente:
    def __init__(self):
        self.contexto = zmq.Context.instance()
        self.socket = self.crear_socket()

    def crear_socket(self):
        """Crea el socket REQ conectado al GT."""
        s = self.contexto.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
        s.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(config.addr(config.HOST_GT, config.PUERTO_GT))
        return s

    def enviar(self, mensaje):
        """
        Envia un mensaje al GT y espera la respuesta.
        Devuelve (respuesta, milisegundos que tardo).
        """
        mensaje["id"] = str(uuid.uuid4())  # identificador unico de la peticion
        inicio = time.perf_counter()
        try:
            self.socket.send_json(mensaje)
            resp = self.socket.recv_json()
        except zmq.Again:
            # El GT no respondio: se recrea el socket (uno REQ sin respuesta queda trabado)
            self.socket.close()
            self.socket = self.crear_socket()
            resp = {"id": mensaje["id"], "status": "ERROR",
                    "detalle": "El GT no respondio a tiempo", "data": {}}
        milisegundos = (time.perf_counter() - inicio) * 1000
        return resp, milisegundos

    def consulta(self, mes):
        return self.enviar({"op": "CONSULTA", "mes": mes})

    def reserva(self, cliente_id, evento_id, fecha, asientos):
        return self.enviar({"op": "RESERVA", "cliente_id": cliente_id,
                            "evento_id": evento_id, "fecha": fecha, "asientos": asientos})

    def cancelacion(self, cliente_id, reserva_id, notif_addr):
        return self.enviar({"op": "CANCELACION", "cliente_id": cliente_id,
                            "reserva_id": reserva_id, "notif_addr": notif_addr})

    def cerrar(self):
        self.socket.close()


def mostrar(resp, milisegundos):
    print(json.dumps(resp, indent=2, ensure_ascii=False))
    print(f"[Cliente] {resp['status']} en {milisegundos:.1f} ms")


def mostrar_uso():
    print("Uso:")
    print("  python3 cliente/cliente.py consulta <mes>")
    print("  python3 cliente/cliente.py reserva <cliente_id> <evento_id> <fecha> <asientos>")
    print("  python3 cliente/cliente.py cancelar <cliente_id> <reserva_id> [puerto_notif] [host_notif]")


def main():
    if len(sys.argv) < 2:
        mostrar_uso()
        return

    comando = sys.argv[1]
    cliente = Cliente()

    if comando == "consulta" and len(sys.argv) == 3:
        resp, ms = cliente.consulta(int(sys.argv[2]))
        mostrar(resp, ms)

    elif comando == "reserva" and len(sys.argv) == 6:
        resp, ms = cliente.reserva(sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]))
        mostrar(resp, ms)

    elif comando == "cancelar" and len(sys.argv) >= 4:
        cliente_id = sys.argv[2]
        reserva_id = int(sys.argv[3])
        puerto_notif = int(sys.argv[4]) if len(sys.argv) > 4 else 6001
        host_notif = sys.argv[5] if len(sys.argv) > 5 else "localhost"

        # 1) Abrir el buzon donde llegara la notificacion (socket PULL).
        buzon = zmq.Context.instance().socket(zmq.PULL)
        buzon.bind("tcp://*:%d" % puerto_notif)

        # 2) Pedir la cancelacion al GT, diciendole donde notificarme.
        notif_addr = f"tcp://{host_notif}:{puerto_notif}"
        resp, ms = cliente.cancelacion(cliente_id, reserva_id, notif_addr)
        mostrar(resp, ms)

        # 3) Si el GT la acepto, esperar el aviso de Cancelaciones (maximo 10 s).
        if resp["status"] == "OK":
            print("[Cliente] Esperando la notificacion...")
            buzon.setsockopt(zmq.RCVTIMEO, 10000)
            try:
                notificacion = buzon.recv_json()
                print("[Cliente] NOTIFICACION:", json.dumps(notificacion, ensure_ascii=False))
            except zmq.Again:
                print("[Cliente] No llego ninguna notificacion en 10 segundos")
        buzon.close()

    else:
        mostrar_uso()

    cliente.cerrar()


if __name__ == "__main__":
    main()
