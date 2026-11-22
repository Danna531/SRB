"""
CONFIGURACION
Direcciones y puertos de los servicios. Para desplegar en las VMs solo se cambian los HOST
(variables SRB_HOST_*), nunca el codigo. Con SRB_BASE=1 se usa la arquitectura base: un solo GT,
un solo RMC y solo el nodo A de Persistencia (sin replicas).
"""
# Direcciones de los servicios.
# Para desplegar en las VMs solo se cambian los HOST (nunca el código).
import os


def _host(nombre, defecto="localhost"):
    """Devuelve el host de un servicio: la variable SRB_HOST_<NOMBRE> o, si no existe, el valor por defecto."""
    return os.environ.get(f"SRB_HOST_{nombre}", defecto)


HOST_GT = _host("GT")
HOST_RMC = _host("RMC")
HOST_CANCELACIONES = _host("CANCELACIONES")
HOST_PERSISTENCIA = _host("PERSISTENCIA")
HOST_PERSISTENCIA_2 = _host("PERSISTENCIA_2", HOST_PERSISTENCIA)

BASE = os.environ.get("SRB_BASE") == "1"

PUERTO_GT = 5550
PUERTOS_GT = [5550] if BASE else [5550, 5551]   # principal y respaldo
PUERTO_RMC = 5555                               # RMC por defecto
PUERTOS_RMC = [5555] if BASE else [5555, 5556]  # instancias RMC (Round Robin)
PUERTO_PERSISTENCIA = 5560
PUERTO_CANCELACIONES = 5570

NODOS_PERSISTENCIA = {
    "A": {"host": HOST_PERSISTENCIA,   "puerto": 5560, "puerto_repl": 5562},
}
if not BASE:
    NODOS_PERSISTENCIA["B"] = {"host": HOST_PERSISTENCIA_2, "puerto": 5561, "puerto_repl": 5563}

HILOS_GT = 8
HILOS_RMC = 8


def addr(host, puerto):
    """Arma la direccion ZeroMQ tcp://host:puerto."""
    return f"tcp://{host}:{puerto}"


def direcciones_persistencia():
    """Direcciones de cliente de todos los nodos de Persistencia (A y, si no es SRB_BASE, B)."""
    return [addr(n["host"], n["puerto"]) for n in NODOS_PERSISTENCIA.values()]
