"""
PRUEBA DE CONCURRENCIA
Varios clientes reservan el mismo evento: no debe haber sobreventa y A y B deben quedar iguales.
Uso: python3 pruebas/probar_concurrencia.py [clientes] [reservas_por_cliente]
"""
import os
import random
import sqlite3
import sys
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cliente"))
from cliente import Cliente

EVENTO, FECHA, CAPACIDAD = "E1", "2026-11-05", 100


def reservar(numero, cantidad, aceptadas, lock):
    """Un cliente que reserva varias veces el mismo evento."""
    cliente = Cliente()
    for _ in range(cantidad):
        asientos = random.randint(1, 4)
        resp, _ = cliente.reserva(f"C{numero}", EVENTO, FECHA, asientos)
        if resp["status"] == "OK":
            with lock:
                aceptadas.append(asientos)
    cliente.cerrar()


def leer_bd(nombre):
    """Lee la BD de un nodo (None si no existe)."""
    ruta = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "persistencia", f"srb_{nombre}.db")
    if not os.path.exists(ruta):
        return None
    con = sqlite3.connect(ruta)
    ocurrencias = con.execute("SELECT * FROM ocurrencias ORDER BY ocurrencia_id").fetchall()
    reservas = con.execute("SELECT * FROM reservas ORDER BY reserva_id").fetchall()
    con.close()
    return ocurrencias, reservas


def main():
    """Lanza los clientes y verifica el resultado."""
    n_clientes = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    por_cliente = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    inicial, _ = Cliente().consulta(11)
    ocupados_antes = [e for e in inicial["data"]["eventos"]
                      if e["evento_id"] == EVENTO and e["fecha"] == FECHA][0]["asientos_ocupados"]

    aceptadas, lock = [], threading.Lock()
    hilos = [threading.Thread(target=reservar, args=(i, por_cliente, aceptadas, lock))
             for i in range(1, n_clientes + 1)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()

    consulta, _ = Cliente().consulta(11)
    fila = [e for e in consulta["data"]["eventos"] if e["evento_id"] == EVENTO and e["fecha"] == FECHA][0]
    libres, ocupados = fila["asientos_libres"], fila["asientos_ocupados"]
    vendidos = sum(aceptadas) + ocupados_antes

    print(f"Reservas aceptadas: {len(aceptadas)} ({vendidos} asientos) de {n_clientes * por_cliente} intentos")
    print(f"BD -> libres={libres} ocupados={ocupados}")
    fallos = []
    if libres < 0:
        fallos.append("asientos libres negativos (sobreventa)")
    if libres + ocupados != CAPACIDAD:
        fallos.append("libres + ocupados != 100")
    if vendidos != ocupados:
        fallos.append(f"los asientos de las reservas aceptadas ({vendidos}) no coinciden con los ocupados ({ocupados})")

    a, b = leer_bd("A"), leer_bd("B")
    if a is not None and b is not None:
        if a != b:
            fallos.append("las BD de los nodos A y B NO son identicas")
        else:
            print("Nodos A y B identicos.")
    else:
        print("(No se encontraron las BD locales: se omite la comparacion A vs B)")

    if fallos:
        print("FALLO:", "; ".join(fallos))
        sys.exit(1)
    print("PRUEBA EXITOSA: sin sobreventa y datos consistentes.")


if __name__ == "__main__":
    main()
