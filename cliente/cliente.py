"""
CLIENTE
La clase Cliente tambien la usa el generador de carga.
"""
import json
import os
import socket
import sys
import time
import uuid

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

# El cliente espera mas que el GT (5 s) y que el RMC (3 s): asi, si algo falla
# mas adentro, le alcanza a llegar el ERROR del GT antes de que el cliente se rinda.
TIMEOUT_MS = 8000

PUERTO_NOTIF_POR_DEFECTO = 6001
ESPERA_NOTIFICACION_MS = 10000


class Cliente:
    """Conexion con el GT. Cada metodo envia una operacion y devuelve (respuesta, ms)."""

    def __init__(self):
        # Context.instance() = un unico contexto por proceso, compartido por
        # todos los hilos del generador de carga (como un singleton en Java).
        self.contexto = zmq.Context.instance()
        self.indice_gt = 0  # 0 = GT principal
        self.socket = self.crear_socket()

    def crear_socket(self):
        """Crea el socket REQ conectado al GT actual, con timeouts."""
        puerto = config.PUERTOS_GT[self.indice_gt]
        s = self.contexto.socket(zmq.REQ)
        s.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)  # maximo de espera al recibir
        s.setsockopt(zmq.SNDTIMEO, TIMEOUT_MS)  # maximo de espera al enviar
        s.setsockopt(zmq.LINGER, 0)             # al cerrar, descartar lo pendiente
        s.connect(config.addr(config.HOST_GT, puerto))
        return s

    def cambiar_de_gt(self):
        """Cierra el socket trabado y se conecta al siguiente GT."""
        self.socket.close()
        self.indice_gt = (self.indice_gt + 1) % len(config.PUERTOS_GT)
        self.socket = self.crear_socket()
        print(f"[Cliente] GT sin respuesta, probando puerto {config.PUERTOS_GT[self.indice_gt]}")

    def enviar(self, mensaje):
        """
        Envia un mensaje al GT; si no responde, prueba el otro GT con el mismo id.
        Devuelve (respuesta, milisegundos que tardo).
        """
        mensaje["id"] = str(uuid.uuid4())  # mismo id en los reintentos
        inicio = time.perf_counter()
        resp = None
        for intento in range(len(config.PUERTOS_GT)):
            try:
                self.socket.send_json(mensaje)
                resp = self.socket.recv_json()
                break
            except zmq.Again:
                self.cambiar_de_gt()
        if resp is None:
            resp = {"id": mensaje["id"], "status": "ERROR",
                    "detalle": "Ningun GT respondio a tiempo", "data": {}}
        milisegundos = (time.perf_counter() - inicio) * 1000
        return resp, milisegundos

    def consulta(self, mes):
        """Pide la lista de eventos (con asientos libres) del mes indicado."""
        return self.enviar({"op": "CONSULTA", "mes": mes})

    def reserva(self, cliente_id, evento_id, fecha, asientos):
        """Reserva asientos de una ocurrencia (evento + fecha). Si sale OK, data trae el reserva_id."""
        return self.enviar({"op": "RESERVA", "cliente_id": cliente_id,
                            "evento_id": evento_id, "fecha": fecha, "asientos": asientos})

    def modificacion(self, cliente_id, reserva_id, nuevos_asientos, nueva_fecha):
        """
        Cambia el numero de asientos y/o la fecha de una reserva.
        Lo que no se quiere cambiar se pasa como None (viaja como null en el JSON)
        y Persistencia conserva el valor actual. Si el cambio no se puede hacer,
        la respuesta es RECHAZADO y la reserva queda como estaba.
        cliente_id no lo usa el RMC hoy; se envia para los logs y por si luego se valida el dueno.
        """
        return self.enviar({"op": "MODIFICACION", "cliente_id": cliente_id,
                            "reserva_id": reserva_id, "nuevos_asientos": nuevos_asientos,
                            "nueva_fecha": nueva_fecha})

    def cancelacion(self, cliente_id, reserva_id, notif_addr):
        """
        Pide cancelar una reserva. El GT responde de inmediato (aceptada) y
        despues Cancelaciones envia la notificacion a notif_addr.
        """
        return self.enviar({"op": "CANCELACION", "cliente_id": cliente_id,
                            "reserva_id": reserva_id, "notif_addr": notif_addr})

    def cerrar(self):
        """Cierra el socket hacia el GT."""
        self.socket.close()


def ip_local_hacia(host_destino):
    """
    Devuelve la IP de ESTA maquina que ve el host_destino.
    Hace falta para notif_addr: si pusieramos "localhost", el servicio de
    Cancelaciones (en otra maquina) intentaria notificarse a si mismo.
    Truco: un socket UDP con connect() NO envia nada; solo le pide al sistema
    operativo que elija la ruta, y getsockname() dice que IP local eligio.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((host_destino, 9))  # el puerto da igual: no se envia ningun paquete
        return s.getsockname()[0]
    except OSError:
        return "localhost"  # sin red: al menos funciona si todo esta en la misma maquina
    finally:
        s.close()


def esperar_notificacion(buzon):
    """Espera en el buzon (PULL) la notificacion de Cancelaciones, maximo ESPERA_NOTIFICACION_MS."""
    print("[Cliente] Esperando la notificacion...")
    buzon.setsockopt(zmq.RCVTIMEO, ESPERA_NOTIFICACION_MS)
    try:
        notificacion = buzon.recv_json()
        print("[Cliente] NOTIFICACION:", json.dumps(notificacion, ensure_ascii=False))
    except zmq.Again:
        print(f"[Cliente] No llego ninguna notificacion en {ESPERA_NOTIFICACION_MS // 1000} segundos")


def cancelar(cliente, cliente_id, reserva_id, puerto_notif, ip_notif):
    """Abre el buzon, pide la cancelacion al GT y, si fue aceptada, espera la notificacion."""
    # 1) Abrir primero el buzon, para que este listo antes de que llegue el aviso.
    buzon = zmq.Context.instance().socket(zmq.PULL)
    buzon.setsockopt(zmq.LINGER, 0)
    buzon.bind("tcp://*:%d" % puerto_notif)

    # 2) Pedir la cancelacion diciendo donde notificarme.
    notif_addr = f"tcp://{ip_notif}:{puerto_notif}"
    print(f"[Cliente] Notificaciones en {notif_addr}")
    resp, ms = cliente.cancelacion(cliente_id, reserva_id, notif_addr)
    mostrar(resp, ms)

    # 3) Solo si el GT la acepto llegara una notificacion.
    if resp["status"] == "OK":
        esperar_notificacion(buzon)
    buzon.close()


def mostrar(resp, milisegundos):
    """Imprime la respuesta completa y una linea de resumen."""
    print(json.dumps(resp, indent=2, ensure_ascii=False))
    print(f"[Cliente] {resp['status']} en {milisegundos:.1f} ms")


def mostrar_uso():
    print("Uso:")
    print("  python3 cliente/cliente.py consulta <mes>")
    print("  python3 cliente/cliente.py reserva <cliente_id> <evento_id> <fecha AAAA-MM-DD> <asientos>")
    print("  python3 cliente/cliente.py modificar <cliente_id> <reserva_id> <nuevos_asientos|-> [nueva_fecha]")
    print("        (use - para no cambiar los asientos)")
    print("  python3 cliente/cliente.py cancelar <cliente_id> <reserva_id> [puerto_notif] [ip_notif]")


def ejecutar_comando(cliente, args):
    """
    Interpreta los argumentos de la linea de comandos y ejecuta la operacion.
    Devuelve False si los argumentos no son validos (para mostrar el uso).
    """
    comando = args[0]

    if comando == "consulta" and len(args) == 2:
        mostrar(*cliente.consulta(int(args[1])))

    elif comando == "reserva" and len(args) == 5:
        mostrar(*cliente.reserva(args[1], args[2], args[3], int(args[4])))

    elif comando == "modificar" and len(args) in (4, 5):
        nuevos_asientos = None if args[3] == "-" else int(args[3])
        nueva_fecha = args[4] if len(args) == 5 else None
        if nuevos_asientos is None and nueva_fecha is None:
            print("Hay que cambiar al menos los asientos o la fecha.")
            return False
        mostrar(*cliente.modificacion(args[1], int(args[2]), nuevos_asientos, nueva_fecha))

    elif comando == "cancelar" and 3 <= len(args) <= 5:
        puerto_notif = int(args[3]) if len(args) > 3 else PUERTO_NOTIF_POR_DEFECTO
        ip_notif = args[4] if len(args) > 4 else ip_local_hacia(config.HOST_CANCELACIONES)
        cancelar(cliente, args[1], int(args[2]), puerto_notif, ip_notif)

    else:
        return False
    return True


def main():
    if len(sys.argv) < 2:
        mostrar_uso()
        return

    cliente = Cliente()
    try:
        if not ejecutar_comando(cliente, sys.argv[1:]):
            mostrar_uso()
    except ValueError:
        # int() fallo: por ejemplo "asientos" no era un numero.
        print("Argumento invalido (se esperaba un numero).")
        mostrar_uso()
    finally:
        cliente.cerrar()


if __name__ == "__main__":
    main()
