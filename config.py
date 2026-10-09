# Direcciones de los servicios.
# Para desplegar en las VMs solo se cambian los HOST (nunca el código).
HOST_GT = "localhost"
HOST_RMC = "localhost"
HOST_PERSISTENCIA = "localhost"
HOST_CANCELACIONES = "localhost"

PUERTO_GT = 5550
PUERTO_RMC = 5555
PUERTO_PERSISTENCIA = 5560
PUERTO_CANCELACIONES = 5570


def addr(host, puerto):
    return f"tcp://{host}:{puerto}"