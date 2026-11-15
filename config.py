# Direcciones de los servicios.
# Para desplegar en las VMs solo se cambian los HOST (nunca el código).
HOST_GT = "localhost"
HOST_RMC = "localhost"
HOST_PERSISTENCIA = "localhost"
HOST_CANCELACIONES = "localhost"

PUERTO_GT = 5550
PUERTOS_GT = [5550, 5551]  # principal y respaldo
PUERTO_RMC = 5555           # RMC por defecto
PUERTOS_RMC = [5555, 5556]  # instancias RMC (Round Robin)
PUERTO_PERSISTENCIA = 5560
PUERTO_CANCELACIONES = 5570

def addr(host, puerto):
    return f"tcp://{host}:{puerto}"
