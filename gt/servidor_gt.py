"""
GESTOR DE TRANSACCIONES (GT)
"""
import json
import os
import sys
import threading
import time
from collections import deque

# Para poder importar config.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zmq
import config

TIMEOUT_RMC_MS = 7000
PAUSA_RMC_CAIDO_S = 10
VENTANA_TPM_S = 60

SILENCIO = bool(os.environ.get("SRB_SILENCIO"))

OPERACIONES_SINCRONAS = ["CONSULTA", "RESERVA", "MODIFICACION"]
CAMPOS_CANCELACION = ["cliente_id", "reserva_id", "notif_addr"]

contexto = zmq.Context.instance()
local = threading.local()

lock_rmc = threading.Lock()
indice_rmc = 0
rmc_caido_hasta = {puerto: 0.0 for puerto in config.PUERTOS_RMC}

lock_stats = threading.Lock()
marcas = {}
totales = {}
inicio_gt = time.time()


def respuesta(id_, status, detalle, data=None):
    """Arma una respuesta con el formato acordado: id, status, detalle, data."""
    return {"id": id_, "status": status, "detalle": detalle, "data": data or {}}


def crear_socket_rmc(puerto):
    """Crea una conexion REQ a una instancia RMC."""
    s = contexto.socket(zmq.REQ)
    s.setsockopt(zmq.RCVTIMEO, TIMEOUT_RMC_MS)
    s.setsockopt(zmq.SNDTIMEO, TIMEOUT_RMC_MS)
    s.setsockopt(zmq.LINGER, 0)
    s.connect(config.addr(config.HOST_RMC, puerto))
    return s


def sockets_hilo():
    if not hasattr(local, "rmc"):
        local.rmc = {puerto: crear_socket_rmc(puerto) for puerto in config.PUERTOS_RMC}
        # queda en cola y se entrega cuando el servicio arranque.
        local.cancelaciones = contexto.socket(zmq.PUSH)
        local.cancelaciones.setsockopt(zmq.LINGER, 0)
        local.cancelaciones.connect(config.addr(config.HOST_CANCELACIONES, config.PUERTO_CANCELACIONES))
    return local


def orden_rmc():
    global indice_rmc
    puertos = config.PUERTOS_RMC
    with lock_rmc:
        inicial = indice_rmc % len(puertos)
        indice_rmc += 1
        ahora = time.time()
        base = [puertos[(inicial + k) % len(puertos)] for k in range(len(puertos))]
        return sorted(base, key=lambda p: rmc_caido_hasta[p] > ahora)


def enviar_al_rmc(mensaje):
    hilo = sockets_hilo()
    for intento, puerto in enumerate(orden_rmc()):
        if not SILENCIO:
            print(f"[GT] Intento {intento + 1}: {mensaje.get('op')} -> RMC {puerto}", flush=True)
        try:
            hilo.rmc[puerto].send_json(mensaje)
            resp = hilo.rmc[puerto].recv_json()
            if intento > 0 and not SILENCIO:
                print(f"[GT] Recuperacion exitosa usando RMC {puerto}", flush=True)
            return resp
        except zmq.ZMQError:
            print(f"[GT] RMC {puerto} no respondio. Se evitara por {PAUSA_RMC_CAIDO_S} s", flush=True)
            with lock_rmc:
                rmc_caido_hasta[puerto] = time.time() + PAUSA_RMC_CAIDO_S
            # Un socket REQ sin respuesta debe recrearse.
            hilo.rmc[puerto].close()
            hilo.rmc[puerto] = crear_socket_rmc(puerto)

    return respuesta(mensaje.get("id"), "ERROR", "Ningun RMC respondio a la solicitud")


def registrar(op, status):
    clave = f"{op}:{status}"
    ahora = time.time()
    with lock_stats:
        for k in (op, clave):
            marcas.setdefault(k, deque()).append(ahora)
            totales[k] = totales.get(k, 0) + 1


def estadisticas():
    ahora = time.time()
    with lock_stats:
        tpm = {}
        for k, cola in marcas.items():
            while cola and cola[0] < ahora - VENTANA_TPM_S:
                cola.popleft()
            tpm[k] = len(cola)
        return {"tpm_ultimos_60s": tpm, "totales": dict(totales),
                "segundos_activo": round(ahora - inicio_gt, 1)}


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
        sockets_hilo().cancelaciones.send_json(mensaje)  # solo la deja en cola, no espera
        return respuesta(id_, "OK", "Cancelacion aceptada; se le notificara cuando se efectue")

    if op == "ESTADISTICAS":
        return respuesta(id_, "OK", "TPM del GT", estadisticas())

    return respuesta(id_, "ERROR", f"Operacion desconocida: {op}")


def trabajador(puerto):
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

        # Un socket REP SIEMPRE debe responder a lo que recibe.
        s.send_json(resp)

        if op != "ESTADISTICAS":
            registrar(op, resp["status"])
        if not SILENCIO:
            milisegundos = (time.time() - inicio) * 1000
            print(f"[GT {puerto}] {time.strftime('%H:%M:%S')} op={op} -> {resp['status']} ({milisegundos:.1f} ms)",
                  flush=True)


def reporte_tpm():
    while True:
        time.sleep(10)
        est = estadisticas()["tpm_ultimos_60s"]
        resumen = {k: v for k, v in est.items() if ":" not in k}
        if resumen:
            print(f"[GT] TPM (ultimo minuto): {resumen}", flush=True)


def main():
    # Puerto opcional: python3 gt/servidor_gt.py 5551 (respaldo)
    puerto = int(sys.argv[1]) if len(sys.argv) > 1 else config.PUERTO_GT
    hilos = int(sys.argv[2]) if len(sys.argv) > 2 else config.HILOS_GT

    frontal = contexto.socket(zmq.ROUTER)
    frontal.bind("tcp://*:%d" % puerto)
    trasero = contexto.socket(zmq.DEALER)
    trasero.bind("inproc://gt-trabajadores")
    for _ in range(hilos):
        threading.Thread(target=trabajador, args=(puerto,), daemon=True).start()
    threading.Thread(target=reporte_tpm, daemon=True).start()

    print(f"[GT] Escuchando clientes en el puerto {puerto} con {hilos} hilos", flush=True)
    try:
        zmq.proxy(frontal, trasero)
    except KeyboardInterrupt:
        print("\n[GT] Detenido")


if __name__ == "__main__":
    main()
