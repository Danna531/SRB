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

TIMEOUT_RMC_MS = 3000  # si el RMC no responde en 5 segundos, se avisa al cliente

OPERACIONES_SINCRONAS = ["CONSULTA", "RESERVA", "MODIFICACION"]
CAMPOS_CANCELACION = ["cliente_id", "reserva_id", "notif_addr"]

contexto = zmq.Context()


def respuesta(id_, status, detalle):
    """Arma una respuesta con el formato acordado: id, status, detalle, data."""
    return {"id": id_, "status": status, "detalle": detalle, "data": {}}


def crear_socket_rmc(puerto):
    """Crea una conexion REQ a una instancia RMC."""
    s = contexto.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_RMC_MS)
    s.setsockopt(zmq.SNDTIMEO, TIMEOUT_RMC_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(config.addr(config.HOST_RMC, puerto))
    return s


# Crear una conexion para cada RMC.
sockets_rmc = [
    crear_socket_rmc(puerto)
    for puerto in config.PUERTOS_RMC
]

# Indica cual RMC atendera la siguiente solicitud.
indice_rmc = 0


# Socket PUSH hacia Cancelaciones. Si Cancelaciones no esta encendido, el mensaje
# queda en cola y se entrega cuando el servicio arranque.
socket_cancelaciones = contexto.socket(zmq.PUSH)
socket_cancelaciones.setsockopt(zmq.LINGER, 0)
socket_cancelaciones.connect(config.addr(config.HOST_CANCELACIONES, config.PUERTO_CANCELACIONES))


def enviar_al_rmc(mensaje):
    """Round Robin con failover hacia otra instancia RMC."""
    global indice_rmc

    cantidad = len(sockets_rmc)

    # Seleccionar el RMC inicial utilizando Round Robin.
    inicial = indice_rmc % cantidad
    indice_rmc += 1

    # Intentar cada RMC disponible como maximo una vez.
    for intento in range(cantidad):
        indice = (inicial + intento) % cantidad
        puerto = config.PUERTOS_RMC[indice]
        socket_rmc = sockets_rmc[indice]

        print(f"[GT] Intento {intento + 1}: {mensaje.get('op')} -> RMC {puerto}")

        try:
            socket_rmc.send_json(mensaje)
            respuesta_rmc = socket_rmc.recv_json()

            if intento > 0:
                print(f"[GT] Recuperacion exitosa usando RMC {puerto}")

            return respuesta_rmc

        except zmq.Again:
            print(f"[GT] RMC {puerto} no respondio. Intentando recuperacion...")

            # Un socket REQ sin respuesta debe recrearse.
            socket_rmc.close()
            sockets_rmc[indice] = crear_socket_rmc(puerto)

    return respuesta(
        mensaje.get("id"),
        "ERROR",
        "Ningun RMC respondio a la solicitud"
    )


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
