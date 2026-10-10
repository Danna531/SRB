"""
GENERADOR DE CARGA
Requisito según el profe en el enunciado: cada cliente debe enviar al menos 20 solicitudes por minuto.
"""
import csv
import random
import statistics
import sys
import threading
import time

from cliente import Cliente  # el cliente.py de esta misma carpeta

MES = 11  # el mes donde estan los eventos


def obtener_ocurrencias():
    """Hace una consulta para saber que eventos y fechas existen (se usan en las reservas)."""
    cliente = Cliente()
    resp, _ = cliente.consulta(MES)
    cliente.cerrar()
    if resp["status"] != "OK":
        print("No se pudo consultar los eventos:", resp["detalle"])
        sys.exit(1)
    return resp["data"]["eventos"]


def simular_cliente(numero, ocurrencias, por_minuto, segundos, porcentaje_consultas, resultados):
    """Lo que hace cada hilo es enviar peticiones al ritmo pedido hasta que se acabe el tiempo."""
    cliente = Cliente()
    cliente_id = f"C{numero}"
    pausa_entre_envios = 60.0 / por_minuto
    hora_fin = time.time() + segundos

    while time.time() < hora_fin:
        inicio_ciclo = time.time()

        # Se elige al azar si esta peticion es consulta o reserva.
        if random.random() * 100 < porcentaje_consultas:
            operacion = "CONSULTA"
            resp, ms = cliente.consulta(MES)
        else:
            operacion = "RESERVA"
            evento = random.choice(ocurrencias)
            asientos = random.randint(1, 4)
            resp, ms = cliente.reserva(cliente_id, evento["evento_id"], evento["fecha"], asientos)

        resultados.append([time.strftime("%H:%M:%S"), cliente_id, operacion, resp["status"], round(ms, 1)])

        # Esperar lo que falte para cumplir el ritmo (solicitudes por minuto).
        falta = pausa_entre_envios - (time.time() - inicio_ciclo)
        if falta > 0:
            time.sleep(falta)

    cliente.cerrar()


def mostrar_resumen(resultados, duracion_real):
    print("\n===== RESUMEN =====")
    for operacion in ["CONSULTA", "RESERVA"]:
        tiempos = [fila[4] for fila in resultados if fila[2] == operacion]
        if len(tiempos) == 0:
            continue
        promedio = statistics.mean(tiempos)
        desviacion = statistics.stdev(tiempos) if len(tiempos) > 1 else 0.0
        print(f"{operacion:<9} cantidad={len(tiempos):<5} promedio={promedio:.1f} ms  desviacion={desviacion:.1f} ms")

    errores = len([fila for fila in resultados if fila[3] == "ERROR"])
    rechazadas = len([fila for fila in resultados if fila[3] == "RECHAZADO"])
    print(f"Total: {len(resultados)} peticiones | rechazadas: {rechazadas} | errores: {errores}")
    print(f"Transacciones por minuto: {len(resultados) / (duracion_real / 60):.1f}")


def main():
    if len(sys.argv) != 5:
        print(__doc__)
        return

    num_clientes = int(sys.argv[1])
    por_minuto = int(sys.argv[2])
    segundos = int(sys.argv[3])
    porcentaje_consultas = int(sys.argv[4])

    if por_minuto < 20:
        print("Aviso: el enunciado pide al menos 20 solicitudes por minuto por cliente.")

    ocurrencias = obtener_ocurrencias()
    print(f"Iniciando {num_clientes} clientes, {por_minuto} sol/min cada uno, {segundos} s, "
          f"{porcentaje_consultas}% consultas / {100 - porcentaje_consultas}% reservas")

    resultados = []  # aqui todos los hilos van guardando sus resultados
    hilos = []
    inicio = time.time()
    for numero in range(1, num_clientes + 1):
        hilo = threading.Thread(target=simular_cliente,
                                args=(numero, ocurrencias, por_minuto, segundos, porcentaje_consultas, resultados))
        hilo.start()
        hilos.append(hilo)
    for hilo in hilos:
        hilo.join()  # esperar a que todos terminen
    duracion_real = time.time() - inicio

    with open("resultados_carga.csv", "w", newline="") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(["hora", "cliente", "operacion", "status", "ms"])
        escritor.writerows(resultados)

    mostrar_resumen(resultados, duracion_real)
    print("Guardado en resultados_carga.csv")


if __name__ == "__main__":
    main()
