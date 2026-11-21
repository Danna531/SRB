import os
import shutil
import sqlite3

CARPETA = os.path.dirname(os.path.abspath(__file__))
RUTA_BD = os.path.join(CARPETA, "srb_A.db")
RUTA_BD_B = os.path.join(CARPETA, "srb_B.db")

ANIO = 2026
MES = 11
TOTAL_EVENTOS = 50
EVENTOS_DOBLES = 20   # eventos con dos ocurrencias en el mes
ASIENTOS = 100


def crear_bd():
    for viejo in (RUTA_BD, RUTA_BD + "-wal", RUTA_BD + "-shm",
                  RUTA_BD_B, RUTA_BD_B + "-wal", RUTA_BD_B + "-shm"):
        if os.path.exists(viejo):
            os.remove(viejo)

    con = sqlite3.connect(RUTA_BD)
    cur = con.cursor()

    cur.executescript("""
    CREATE TABLE eventos (
        evento_id TEXT PRIMARY KEY,
        num_ocurrencias INTEGER NOT NULL
    );

    CREATE TABLE ocurrencias (
        ocurrencia_id INTEGER PRIMARY KEY AUTOINCREMENT,
        evento_id TEXT NOT NULL REFERENCES eventos(evento_id),
        fecha TEXT NOT NULL,
        asientos_libres INTEGER NOT NULL,
        asientos_ocupados INTEGER NOT NULL,
        UNIQUE (evento_id, fecha)
    );

    CREATE TABLE reservas (
        reserva_id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente_id TEXT NOT NULL,
        ocurrencia_id INTEGER NOT NULL REFERENCES ocurrencias(ocurrencia_id),
        asientos INTEGER NOT NULL,
        estado TEXT NOT NULL DEFAULT 'ACTIVA'
    );

    CREATE TABLE solicitudes_procesadas (
        id TEXT PRIMARY KEY,
        operacion TEXT NOT NULL,
        respuesta TEXT NOT NULL
    );

    -- Bitacora de escrituras: el primario la usa para reenviar al secundario
    -- y para poner al dia a un nodo que estuvo caido.
    CREATE TABLE log_ops (
        seq INTEGER PRIMARY KEY,
        op TEXT NOT NULL
    );
    """)

    for i in range(1, TOTAL_EVENTOS + 1):
        evento_id = f"E{i}"
        if i <= EVENTOS_DOBLES:
            fechas = [5, 19]
        else:
            fechas = [1 + (i % 28)]

        cur.execute("INSERT INTO eventos VALUES (?, ?)", (evento_id, len(fechas)))
        for dia in fechas:
            fecha = f"{ANIO}-{MES:02d}-{dia:02d}"
            cur.execute(
                "INSERT INTO ocurrencias (evento_id, fecha, asientos_libres, asientos_ocupados) "
                "VALUES (?, ?, ?, ?)",
                (evento_id, fecha, ASIENTOS, 0),
            )

    con.commit()

    n_eventos = cur.execute("SELECT COUNT(*) FROM eventos").fetchone()[0]
    n_ocurr = cur.execute("SELECT COUNT(*) FROM ocurrencias").fetchone()[0]
    con.close()
    shutil.copyfile(RUTA_BD, RUTA_BD_B)
    print(f"BD creada en: {RUTA_BD} y {RUTA_BD_B}")
    print(f"Eventos: {n_eventos} | Ocurrencias: {n_ocurr}")


if __name__ == "__main__":
    crear_bd()