"""
VER BD
Uso: python3 persistencia/ver_bd.py [A|B]
"""
import os
import sqlite3
import sys

CARPETA = os.path.dirname(os.path.abspath(__file__))


def leer(nodo):
    """Lee la BD del nodo (None si no existe)."""
    ruta = os.path.join(CARPETA, f"srb_{nodo}.db")
    if not os.path.exists(ruta):
        return None
    con = sqlite3.connect(f"file:{ruta}?mode=ro", uri=True)
    datos = {
        "ocurrencias": con.execute(
            "SELECT evento_id, fecha, asientos_libres, asientos_ocupados FROM ocurrencias "
            "ORDER BY evento_id, fecha").fetchall(),
        "reservas": con.execute(
            "SELECT reserva_id, cliente_id, ocurrencia_id, asientos, estado FROM reservas "
            "ORDER BY reserva_id").fetchall(),
        "seq": con.execute("SELECT COALESCE(MAX(seq), 0) FROM log_ops").fetchone()[0],
    }
    con.close()
    return datos


def mostrar(nodo, d):
    """Imprime el resumen del nodo."""
    ocupadas = [o for o in d["ocurrencias"] if o[3] > 0]
    print(f"\n===== Nodo {nodo} | operaciones replicadas: {d['seq']} =====")
    print(f"Ocurrencias: {len(d['ocurrencias'])} | con asientos ocupados: {len(ocupadas)} | "
          f"asientos libres totales: {sum(o[2] for o in d['ocurrencias'])}")
    for ev, fecha, libres, ocup in ocupadas:
        print(f"  {ev:<4} {fecha}  libres={libres:<3} ocupados={ocup}")
    activas = [r for r in d["reservas"] if r[4] == "ACTIVA"]
    print(f"Reservas: {len(d['reservas'])} (activas: {len(activas)}, canceladas: {len(d['reservas']) - len(activas)})")


def main():
    """Muestra los nodos y si A y B son identicos."""
    nodos = [sys.argv[1].upper()] if len(sys.argv) > 1 else ["A", "B"]
    datos = {n: leer(n) for n in nodos}
    for n, d in datos.items():
        if d is None:
            print(f"No existe la BD del nodo {n} (ejecute persistencia/crear_bd.py)")
        else:
            mostrar(n, d)
    if len(nodos) == 2 and all(datos.values()):
        iguales = (datos["A"]["ocurrencias"] == datos["B"]["ocurrencias"]
                   and datos["A"]["reservas"] == datos["B"]["reservas"])
        print("\nLos nodos A y B " + ("COINCIDEN." if iguales else "NO coinciden (puede haber una replica atrasada)."))


if __name__ == "__main__":
    main()
