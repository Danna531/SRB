"""
CLIENTE DE PERSISTENCIA
Lo usan el RMC y Cancelaciones para hablar con los nodos A y B de Persistencia. Reparte las lecturas
entre los nodos, manda las escrituras al primario y, si un nodo no responde o deja de ser el
primario, prueba con el otro. El estado de salud de los nodos se comparte entre todos los hilos.
"""
import threading
import time

import zmq

import config

LECTURAS = {"LISTAR_EVENTOS", "DISPONIBILIDAD"}
TIMEOUT_MS = 3000
PAUSA_NODO_CAIDO_S = 10


_lock = threading.Lock()
_compartido = {"caido_hasta": [0.0, 0.0], "primario": 0}


class ClientePersistencia:
    """Conexion con los nodos de Persistencia. Cada hilo debe tener su propia instancia (sus sockets no se comparten)."""
    def __init__(self, timeout_ms=TIMEOUT_MS):
        """Guarda las direcciones de los nodos; los sockets se crean la primera vez que se usan."""
        self.direcciones = config.direcciones_persistencia()
        self.timeout_ms = timeout_ms
        self.contexto = zmq.Context.instance()
        self.sockets = [None] * len(self.direcciones)
        self.turno = 0

    def _socket(self, i):
        """Devuelve el socket REQ hacia el nodo i, creandolo si hace falta."""
        if self.sockets[i] is None:
            s = self.contexto.socket(zmq.REQ)
            s.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
            s.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
            s.setsockopt(zmq.LINGER, 0)
            s.connect(self.direcciones[i])
            self.sockets[i] = s
        return self.sockets[i]

    def _llamar_nodo(self, i, mensaje):
        """Envia el mensaje al nodo i. Si no responde a tiempo lo marca caido para todos los hilos y devuelve None."""
        s = self._socket(i)
        try:
            s.send_json(mensaje)
            return s.recv_json()
        except zmq.ZMQError:
            s.close()
            self.sockets[i] = None
            with _lock:
                _compartido["caido_hasta"][i] = time.time() + PAUSA_NODO_CAIDO_S
            return None

    def _orden(self, es_lectura):
        """Orden en que se prueban los nodos: lecturas por turnos y escrituras primero al primario; los caidos van al final."""
        n = len(self.direcciones)
        if es_lectura:
            self.turno += 1
            base = [(self.turno + k) % n for k in range(n)]
        else:
            with _lock:
                primario = _compartido["primario"]
            base = [primario] + [i for i in range(n) if i != primario]
        ahora = time.time()
        with _lock:
            caido = list(_compartido["caido_hasta"])
        return sorted(base, key=lambda i: caido[i] > ahora)

    def llamar(self, mensaje):
        """Envia una operacion a Persistencia con failover entre nodos y devuelve su respuesta (o ERROR si ninguno responde)."""
        es_lectura = mensaje.get("op") in LECTURAS
        for i in self._orden(es_lectura):
            resp = self._llamar_nodo(i, mensaje)
            if resp is None:
                continue
            if resp.get("status") == "ERROR" and resp.get("detalle") == "NO_PRIMARIO":
                continue
            if not es_lectura:
                with _lock:
                    _compartido["primario"] = i
                    _compartido["caido_hasta"][i] = 0.0
            return resp
        return {"status": "ERROR", "detalle": "Persistencia no responde", "data": {}}

    def cerrar(self):
        """Cierra los sockets de este cliente."""
        for i, s in enumerate(self.sockets):
            if s is not None:
                s.close()
                self.sockets[i] = None
