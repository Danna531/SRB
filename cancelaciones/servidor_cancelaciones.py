"""
SERVICIO DE CANCELACIONES Y NOTIFICACIONES
"""
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

TIMEOUT_PERSISTENCIA_MS = 3000

contexto = zmq.Context()


def crear_socket_persistencia():
    """Crea el socket REQ conectado a Persistencia."""
    s = contexto.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_PERSISTENCIA_MS)
    s.setsockopt(zmq.SNDTIMEO, TIMEOUT_PERSISTENCIA_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(config.addr(config.HOST_PERSISTENCIA, config.PUERTO_PERSISTENCIA))
    return s


socket_persistencia = crear_socket_persistencia()


def liberar_reserva(id_, reserva_id):
    """Pide a Persistencia liberar la reserva y devuelve su respuesta."""
    global socket_persistencia
    try:
        socket_persistencia.send_json({"id": id_, "op": "LIBERAR_RESERVA", "reserva_id": reserva_id})
        return socket_persistencia.recv_json()
    except zmq.Again:
        # Persistencia no contesto: se recrea el socket (un REQ sin respuesta queda trabado)
        socket_persistencia.close()
        socket_persistencia = crear_socket_persistencia()
        return {"status": "ERROR", "detalle": "Persistencia no responde", "data": {}}


def notificar_cliente(notif_addr, notificacion):
    """Envia la notificacion al buzon (PULL) que el cliente abrio en notif_addr."""
    s = contexto.socket(zmq.PUSH)
    s.connect(notif_addr)
    s.send_json(notificacion)
    s.close(linger=2000)  # da 2 segundos para que el mensaje salga antes de cerrar


def procesar(mensaje):
    id_ = mensaje.get("id")
    reserva_id = mensaje["reserva_id"]
    notif_addr = mensaje["notif_addr"]

    respuesta = liberar_reserva(id_, reserva_id)

    if respuesta["status"] == "OK":
        detalle = "Reserva cancelada"
    else:
        detalle = respuesta["detalle"]  # por ejemplo: "La reserva no existe o ya fue cancelada"

    notificacion = {"id": id_, "tipo": "NOTIFICACION", "status": respuesta["status"],
                    "reserva_id": reserva_id, "detalle": detalle}
    notificar_cliente(notif_addr, notificacion)
    print(f"[CANCELACIONES] reserva {reserva_id}: {respuesta['status']} -> cliente notificado en {notif_addr}")


def main():
    # Socket PULL: aqui llegan las cancelaciones que envia el GT.
    entrada = contexto.socket(zmq.PULL)
    entrada.bind("tcp://*:%d" % config.PUERTO_CANCELACIONES)
    print(f"[CANCELACIONES] Escuchando en el puerto {config.PUERTO_CANCELACIONES}...")

    try:
        while True:
            mensaje = entrada.recv_json()  # espera hasta que llegue una cancelacion
            print(f"[CANCELACIONES] {time.strftime('%H:%M:%S')} Recibi: {mensaje}")
            try:
                procesar(mensaje)
            except KeyError as campo:
                print(f"[CANCELACIONES] Mensaje descartado: falta el campo {campo}")
            except Exception as error:  # un mensaje malo no debe tumbar el servicio
                print(f"[CANCELACIONES] Error: {error}")
    except KeyboardInterrupt:
        print("\n[CANCELACIONES] Detenido")


if __name__ == "__main__":
    main()
