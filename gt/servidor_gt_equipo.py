import json
import os
import sys
import threading
import time
from collections import deque

# Para poder importar config.py (esta en la carpeta padre)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

# Plazos. Un REQ sin respuesta queda trabado, por eso todo socket REQ/PUSH
# tiene un limite de espera.
TIMEOUT_RMC_MS = 7000           # espera maxima por la respuesta de UN RMC
PAUSA_RMC_CAIDO_S = 10          # tiempo que se evita un RMC que no respondio
TIMEOUT_CANCELACIONES_MS = 2000 # espera maxima para dejar una cancelacion en la cola
VENTANA_TPM_S = 60              # ventana del conteo de transacciones por minuto

SILENCIO = bool(os.environ.get("SRB_SILENCIO"))

OPERACIONES_SINCRONAS = ["CONSULTA", "RESERVA", "MODIFICACION"]
CAMPOS_CANCELACION = ["cliente_id", "reserva_id", "notif_addr"]

contexto = zmq.Context.instance()
local = threading.local()  # cada hilo guarda aqui sus propios sockets

# Estado compartido entre hilos (se protege con un lock cada grupo)
lock_rmc = threading.Lock()
indice_rmc = 0   # para el round robin
rmc_caido_hasta = {puerto: 0.0 for puerto in config.PUERTOS_RMC}

lock_stats = threading.Lock()
marcas = {}      # operacion -> instantes recientes (para TPM en la ultima ventana)
totales = {}     # operacion -> total acumulado desde que arranco el GT
eventos = {}     # sucesos de tolerancia a fallos (reintentos, RMC caidos...)
inicio_gt = time.time()


def respuesta(id_, status, detalle, data=None):
    """Arma una respuesta con el formato acordado: id, status, detalle, data."""
    return {"id": id_, "status": status, "detalle": detalle, "data": data or {}}


def contar_evento(nombre):
    """Suma 1 a un contador de sucesos (se ve en ESTADISTICAS; sirve en las pruebas de fallas)."""
    with lock_stats:
        eventos[nombre] = eventos.get(nombre, 0) + 1


def crear_socket_rmc(puerto):
    """Crea un REQ conectado a un RMC, con limites de espera."""
    s = contexto.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_RMC_MS)
    s.setsockopt(zmq.SNDTIMEO, TIMEOUT_RMC_MS)
    s.setsockopt(zmq.LINGER, 0)  # al cerrar, descartar lo pendiente
    s.connect(config.addr(config.HOST_RMC, puerto))
    return s


def sockets_hilo():
    """
    Devuelve los sockets del hilo actual y los crea la primera vez.
    Un socket de ZeroMQ no se puede compartir entre hilos, por eso cada
    hilo trabajador tiene los suyos (threading.local).
    """
    if not hasattr(local, "rmc"):
        local.rmc = {puerto: crear_socket_rmc(puerto) for puerto in config.PUERTOS_RMC}
        # PUSH hacia Cancelaciones: si el servicio no esta encendido, los mensajes
        # quedan en cola (en memoria) y se entregan cuando arranque.
        local.cancelaciones = contexto.socket(zmq.PUSH)
        local.cancelaciones.setsockopt(zmq.LINGER, 0)
        # Sin este limite, con la cola llena el hilo se bloquearia para siempre.
        local.cancelaciones.setsockopt(zmq.SNDTIMEO, TIMEOUT_CANCELACIONES_MS)
        local.cancelaciones.connect(config.addr(config.HOST_CANCELACIONES, config.PUERTO_CANCELACIONES))
    return local


def orden_rmc():
    """
    Devuelve los puertos de RMC en el orden en que se deben probar:
    por turnos (round robin) y dejando de ultimo los que se marcaron caidos.
    """
    global indice_rmc
    puertos = config.PUERTOS_RMC
    with lock_rmc:
        inicial = indice_rmc % len(puertos)
        indice_rmc += 1
        ahora = time.time()
        base = [puertos[(inicial + k) % len(puertos)] for k in range(len(puertos))]
        # sorted es estable: conserva el turno y solo manda al final los caidos
        return sorted(base, key=lambda p: rmc_caido_hasta[p] > ahora)


def enviar_al_rmc(mensaje):
    """
    Reenvia la solicitud a un RMC y devuelve su respuesta.
    Si el RMC no responde a tiempo prueba con el siguiente, con el MISMO id
    (asi la operacion no se duplica). Si ninguno responde devuelve ERROR.
    """
    hilo = sockets_hilo()
    for intento, puerto in enumerate(orden_rmc()):
        if not SILENCIO:
            print(f"[GT] Intento {intento + 1}: {mensaje.get('op')} -> RMC {puerto}", flush=True)
        try:
            hilo.rmc[puerto].send_json(mensaje)
            resp = hilo.rmc[puerto].recv_json()
            if intento > 0:
                contar_evento("recuperado_con_otro_rmc")
                if not SILENCIO:
                    print(f"[GT] Recuperacion exitosa usando RMC {puerto}", flush=True)
            return resp
        except zmq.ZMQError:  # incluye zmq.Again (se acabo el tiempo)
            print(f"[GT] RMC {puerto} no respondio. Se evitara por {PAUSA_RMC_CAIDO_S} s", flush=True)
            contar_evento("rmc_sin_respuesta")
            with lock_rmc:
                rmc_caido_hasta[puerto] = time.time() + PAUSA_RMC_CAIDO_S
            # Un REQ sin respuesta queda trabado: hay que cerrarlo y crear otro.
            hilo.rmc[puerto].close()
            hilo.rmc[puerto] = crear_socket_rmc(puerto)

    return respuesta(mensaje.get("id"), "ERROR", "Ningun RMC respondio a la solicitud")


def registrar(op, status):
    """Cuenta una operacion terminada (total y por resultado) para calcular TPM."""
    ahora = time.time()
    with lock_stats:
        for clave in (op, f"{op}:{status}"):
            marcas.setdefault(clave, deque()).append(ahora)
            totales[clave] = totales.get(clave, 0) + 1


def estadisticas():
    """
    Devuelve las estadisticas del GT:
      tpm_ultimos_60s: operaciones terminadas en la ultima ventana
      totales:         acumulado desde que arranco este GT
      eventos:         reintentos y RMC caidos (tolerancia a fallos)
    """
    ahora = time.time()
    with lock_stats:
        tpm = {}
        for clave, cola in marcas.items():
            while cola and cola[0] < ahora - VENTANA_TPM_S:
                cola.popleft()
            tpm[clave] = len(cola)
        return {"tpm_ultimos_60s": tpm, "totales": dict(totales), "eventos": dict(eventos),
                "segundos_activo": round(ahora - inicio_gt, 1)}


def validar_cancelacion(mensaje):
    """Devuelve el texto del error si la cancelacion esta incompleta o mal formada; si esta bien, None."""
    for campo in CAMPOS_CANCELACION:
        if campo not in mensaje:
            return f"Falta el campo {campo}"
    # Cancelaciones hara connect() a esta direccion: si no es tcp:// fallaria despues, en silencio.
    if not str(mensaje["notif_addr"]).startswith("tcp://"):
        return "notif_addr debe tener la forma tcp://ip:puerto"
    return None


def atender(mensaje):
    """Decide que hacer segun la operacion y devuelve la respuesta para el cliente."""
    id_ = mensaje.get("id")
    op = mensaje.get("op")

    if op in OPERACIONES_SINCRONAS or op == "CANCELACION":
        if not id_:
            # El id es lo que permite reintentar sin duplicar: sin el no se acepta.
            return respuesta(id_, "ERROR", "Falta el campo id")

    if op in OPERACIONES_SINCRONAS:
        return enviar_al_rmc(mensaje)

    if op == "CANCELACION":
        error = validar_cancelacion(mensaje)
        if error:
            return respuesta(id_, "ERROR", error)
        try:
            # Solo la deja en la cola (PUSH); no espera a que se procese.
            sockets_hilo().cancelaciones.send_json(mensaje)
        except zmq.Again:
            # Cola llena: mejor avisar al cliente que decir "aceptada" y perderla.
            contar_evento("cancelacion_cola_llena")
            return respuesta(id_, "ERROR", "Cancelaciones no esta disponible; intente de nuevo")
        return respuesta(id_, "OK", "Cancelacion aceptada; se le notificara cuando se efectue")

    if op == "ESTADISTICAS":
        return respuesta(id_, "OK", "Estadisticas del GT", estadisticas())

    return respuesta(id_, "ERROR", f"Operacion desconocida: {op}")


def trabajador(puerto):
    """
    Hilo trabajador: recibe una peticion del DEALER, la atiende y responde.
    Un REP SIEMPRE debe responder lo que recibe; por eso cualquier error,
    incluso un mensaje malformado, termina en una respuesta ERROR y no tumba el GT.
    """
    s = contexto.socket(zmq.REP)
    s.connect("inproc://gt-trabajadores")
    while True:
        texto = s.recv_string()
        inicio = time.time()
        op = "?"
        try:
            mensaje = json.loads(texto)
            op = mensaje.get("op")
            resp = atender(mensaje)
        except Exception as error:
            resp = respuesta(None, "ERROR", f"Mensaje invalido: {error}")

        s.send_json(resp)

        if op != "ESTADISTICAS":
            registrar(op, resp["status"])
        if not SILENCIO:
            milisegundos = (time.time() - inicio) * 1000
            print(f"[GT {puerto}] {time.strftime('%H:%M:%S')} op={op} -> {resp['status']} ({milisegundos:.1f} ms)",
                  flush=True)


def reporte_tpm():
    """Cada 10 s imprime las operaciones del ultimo minuto (sin separar por resultado)."""
    while True:
        time.sleep(10)
        tpm = estadisticas()["tpm_ultimos_60s"]
        resumen = {clave: v for clave, v in tpm.items() if ":" not in clave}
        if resumen:
            print(f"[GT] TPM (ultimo minuto): {resumen}", flush=True)


def main():
    """Abre el puerto de clientes, lanza los hilos trabajadores y reparte el trabajo."""
    puerto = int(sys.argv[1]) if len(sys.argv) > 1 else config.PUERTO_GT
    hilos = int(sys.argv[2]) if len(sys.argv) > 2 else config.HILOS_GT

    frontal = contexto.socket(zmq.ROUTER)   # cara a los clientes
    frontal.bind("tcp://*:%d" % puerto)
    trasero = contexto.socket(zmq.DEALER)   # cara a los hilos
    trasero.bind("inproc://gt-trabajadores")
    for _ in range(hilos):
        threading.Thread(target=trabajador, args=(puerto,), daemon=True).start()
    threading.Thread(target=reporte_tpm, daemon=True).start()

    print(f"[GT] Escuchando clientes en el puerto {puerto} con {hilos} hilos", flush=True)
    try:
        zmq.proxy(frontal, trasero)  # une las dos caras y no retorna
    except KeyboardInterrupt:
        print("\n[GT] Detenido")


if __name__ == "__main__":
    main()
