"""
GESTOR DE TRANSACCIONES (GT)
"""
import json
import os
import sys
import time

# Para poder importar config.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

TIMEOUT_RMC_MS = 5000  # si el RMC no responde en 5 segundos, se avisa al cliente

OPERACIONES_SINCRONAS = ["CONSULTA", "RESERVA", "MODIFICACION"]
CAMPOS_CANCELACION = ["cliente_id", "reserva_id", "notif_addr"]

contexto = zmq.Context()


def respuesta(id_, status, detalle):
    """Arma una respuesta con el formato acordado: id, status, detalle, data."""
    return {"id": id_, "status": status, "detalle": detalle, "data": {}}


def crear_socket_rmc():
    """Crea el socket REQ conectado al RMC."""
    s = contexto.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_RMC_MS)  # maximo de espera al recibir
    s.setsockopt(zmq.SNDTIMEO, TIMEOUT_RMC_MS)  # maximo de espera al enviar
    s.setsockopt(zmq.LINGER, 0)                 # al cerrar, no esperar mensajes pendientes
    s.connect(config.addr(config.HOST_RMC, config.PUERTO_RMC))
    return s


socket_rmc = crear_socket_rmc()

# Socket PUSH hacia Cancelaciones. Si Cancelaciones no esta encendido, el mensaje
# queda en cola y se entrega cuando el servicio arranque.
socket_cancelaciones = contexto.socket(zmq.PUSH)
socket_cancelaciones.setsockopt(zmq.LINGER, 0)
socket_cancelaciones.connect(config.addr(config.HOST_CANCELACIONES, config.PUERTO_CANCELACIONES))


def enviar_al_rmc(mensaje):
    """Manda el mensaje al RMC y devuelve su respuesta."""
    global socket_rmc
    try:
        socket_rmc.send_json(mensaje)
        return socket_rmc.recv_json()
    except zmq.Again:
        # El RMC no contesto. Un socket REQ que no recibio respuesta queda
        # "trabado", asi que lo cerramos y creamos uno nuevo.
        socket_rmc.close()
        socket_rmc = crear_socket_rmc()
        return respuesta(mensaje.get("id"), "ERROR", "El RMC no respondio a tiempo")


def atender(mensaje):
    """Decide que hacer segun la operacion y devuelve la respuesta para el cliente."""
    id_ = mensaje.get("id")
    op = mensaje.get("op")

    if op in OPERACIONES_SINCRONAS:
        return enviar_al_rmc(mensaje)

    if op == "CANCELACION":
        for campo in CAMPOS_CANCELACION:
            if campo not in mensaje:
                return respuesta(id_, "ERROR", f"Falta el campo {campo}")
        socket_cancelaciones.send_json(mensaje)  # solo la deja en cola, no espera
        return respuesta(id_, "OK", "Cancelacion aceptada; se le notificara cuando se efectue")

    return respuesta(id_, "ERROR", f"Operacion desconocida: {op}")


def main():
    # Socket REP: aqui llegan los clientes. bind = "yo espero conexiones en este puerto".
    socket_clientes = contexto.socket(zmq.REP)
    socket_clientes.bind("tcp://*:%d" % config.PUERTO_GT)
    print(f"[GT] Escuchando clientes en el puerto {config.PUERTO_GT}")

    try:
        while True:
            texto = socket_clientes.recv_string()  # espera hasta que llegue un mensaje
            inicio = time.time()
            op = "?"
            try:
                mensaje = json.loads(texto)
                op = mensaje.get("op")
                resp = atender(mensaje)
            except Exception as error:
                resp = respuesta(None, "ERROR", f"Mensaje invalido: {error}")

            # Un socket REP SIEMPRE debe responder a lo que recibe.
            socket_clientes.send_json(resp)

            milisegundos = (time.time() - inicio) * 1000
            print(f"[GT] {time.strftime('%H:%M:%S')} op={op} -> {resp['status']} ({milisegundos:.1f} ms)")
    except KeyboardInterrupt:
        print("\n[GT] Detenido")


if __name__ == "__main__":
    main()
