"""
GENERADOR DE CARGA
Requisito según el profe en el enunciado: cada cliente debe enviar al menos 20 solicitudes por minuto.

Uso:
  python cliente/generador_carga.py <clientes> <solicitudes_por_minuto> <duracion_segundos> <porcentaje_consultas> [enunciado]

Sin el quinto argumento todos los clientes son iguales (consultas y reservas segun el porcentaje).
Con "enunciado" cada cliente toma un perfil, como pide el enunciado:
  - 3 clientes: 2 solo consultan y 1 consulta y reserva (usa <porcentaje_consultas>).
  - 5 u 8 clientes: 3 o 4 solo consultan y el resto hace reservas y modificaciones.
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


PERFIL_CONSULTA = "solo_consulta"
PERFIL_CONSULTA_RESERVA = "consulta_reserva"
PERFIL_RESERVA_MODIF = "reserva_modif"
PERFIL_MIXTO = "mixto"            # el comportamiento original (consultas y reservas segun porcentaje)
PORCENTAJE_MODIF = 40             # en el perfil reserva_modif: % de modificaciones (el resto son reservas)


def asignar_perfiles(num_clientes, porcentaje_consultas, usar_enunciado):
    """Devuelve una lista con (perfil, porcentaje_consultas) para cada cliente."""
    if not usar_enunciado:
        return [(PERFIL_MIXTO, porcentaje_consultas)] * num_clientes
    if num_clientes == 3:
        return [(PERFIL_CONSULTA, 100), (PERFIL_CONSULTA, 100), (PERFIL_CONSULTA_RESERVA, porcentaje_consultas)]
    solo_consultan = (num_clientes + 1) // 2  # 5 -> 3, 8 -> 4 (como el enunciado)
    return ([(PERFIL_CONSULTA, 100)] * solo_consultan
            + [(PERFIL_RESERVA_MODIF, 0)] * (num_clientes - solo_consultan))


def elegir_operacion(perfil, porcentaje_consultas, reservas_propias):
    """Decide la proxima operacion del cliente. Modificar solo se puede si ya tiene una reserva suya."""
    if perfil == PERFIL_RESERVA_MODIF:
        if reservas_propias and random.random() * 100 < PORCENTAJE_MODIF:
            return "MODIFICACION"
        return "RESERVA"
    if random.random() * 100 < porcentaje_consultas:
        return "CONSULTA"
    return "RESERVA"


def simular_cliente(numero, ocurrencias, por_minuto, segundos, perfil, porcentaje_consultas, resultados):
    """Lo que hace cada hilo es enviar peticiones al ritmo pedido hasta que se acabe el tiempo."""
    cliente = Cliente()
    reservas_propias = []  # reserva_id de las reservas que este cliente logro hacer (para modificarlas)
    cliente_id = f"C{numero}"
    pausa_entre_envios = 60.0 / por_minuto
    hora_fin = time.time() + segundos

    while time.time() < hora_fin:
        inicio_ciclo = time.time()

        # Se elige al azar la operacion segun el perfil del cliente.
        operacion = elegir_operacion(perfil, porcentaje_consultas, reservas_propias)
        if operacion == "CONSULTA":
            resp, ms = cliente.consulta(MES)
        elif operacion == "MODIFICACION":
            # Solo cambia el numero de asientos (la fecha se deja igual: None).
            resp, ms = cliente.modificacion(cliente_id, random.choice(reservas_propias), random.randint(1, 4), None)
        else:
            evento = random.choice(ocurrencias)
            asientos = random.randint(1, 4)
            resp, ms = cliente.reserva(cliente_id, evento["evento_id"], evento["fecha"], asientos)
            if resp["status"] == "OK" and resp.get("data", {}).get("reserva_id"):
                reservas_propias.append(resp["data"]["reserva_id"])

        resultados.append([time.strftime("%H:%M:%S"), cliente_id, operacion, resp["status"], round(ms, 1), perfil])

        # Esperar lo que falte para cumplir el ritmo (solicitudes por minuto).
        falta = pausa_entre_envios - (time.time() - inicio_ciclo)
        if falta > 0:
            time.sleep(falta)

    cliente.cerrar()


def mostrar_resumen(resultados, duracion_real):
    """Imprime promedio y desviacion por operacion, rechazos, errores y transacciones por minuto."""
    print("\n===== RESUMEN =====")
    for operacion in ["CONSULTA", "RESERVA", "MODIFICACION"]:
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
    """Lee los argumentos, lanza un hilo por cliente, guarda el CSV y muestra el resumen."""
    if len(sys.argv) not in (5, 6) or (len(sys.argv) == 6 and sys.argv[5] != "enunciado"):
        print(__doc__)
        return

    num_clientes = int(sys.argv[1])
    por_minuto = int(sys.argv[2])
    segundos = int(sys.argv[3])
    porcentaje_consultas = int(sys.argv[4])
    perfiles = asignar_perfiles(num_clientes, porcentaje_consultas, len(sys.argv) == 6)

    if por_minuto < 20:
        print("Aviso: el enunciado pide al menos 20 solicitudes por minuto por cliente.")

    ocurrencias = obtener_ocurrencias()
    print(f"Iniciando {num_clientes} clientes, {por_minuto} sol/min cada uno, {segundos} s, "
          f"{porcentaje_consultas}% consultas / {100 - porcentaje_consultas}% reservas")
    if len(sys.argv) == 6:
        for numero, (perfil, _) in enumerate(perfiles, start=1):
            print(f"  C{numero}: {perfil}")

    resultados = []  # aqui todos los hilos van guardando sus resultados
    hilos = []
    inicio = time.time()
    for numero in range(1, num_clientes + 1):
        hilo = threading.Thread(target=simular_cliente,
                                args=(numero, ocurrencias, por_minuto, segundos, *perfiles[numero - 1], resultados))
        hilo.start()
        hilos.append(hilo)
    for hilo in hilos:
        hilo.join()  # esperar a que todos terminen
    duracion_real = time.time() - inicio

    with open("resultados_carga.csv", "w", newline="") as archivo:
        escritor = csv.writer(archivo)
        escritor.writerow(["hora", "cliente", "operacion", "status", "ms", "perfil"])
        escritor.writerows(resultados)

    mostrar_resumen(resultados, duracion_real)
    print("Guardado en resultados_carga.csv")


if __name__ == "__main__":
    main()
