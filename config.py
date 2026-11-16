# Direcciones de los servicios.
# Para desplegar en las VMs solo se cambian los HOST (nunca el código).
import os


def _host(nombre, defecto="localhost"):
    return os.environ.get(f"SRB_HOST_{nombre}", defecto)


HOST_GT = _host("GT")
HOST_RMC = _host("RMC")
HOST_CANCELACIONES = _host("CANCELACIONES")
HOST_PERSISTENCIA = _host("PERSISTENCIA")
HOST_PERSISTENCIA_2 = _host("PERSISTENCIA_2", HOST_PERSISTENCIA)

PUERTO_GT = 5550
PUERTOS_GT = [5550, 5551]   # principal y respaldo
PUERTO_RMC = 5555           # RMC por defecto
PUERTOS_RMC = [5555, 5556]  # instancias RMC (Round Robin)
PUERTO_PERSISTENCIA = 5560
PUERTO_CANCELACIONES = 5570

NODOS_PERSISTENCIA = {
    "A": {"host": HOST_PERSISTENCIA,   "puerto": 5560, "puerto_repl": 5562},
    "B": {"host": HOST_PERSISTENCIA_2, "puerto": 5561, "puerto_repl": 5563},
}

HILOS_GT = 8
HILOS_RMC = 8


def addr(host, puerto):
    return f"tcp://{host}:{puerto}"


def direcciones_persistencia():
    return [addr(n["host"], n["puerto"]) for n in NODOS_PERSISTENCIA.values()]
