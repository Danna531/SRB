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
    def __init__(self, timeout_ms=TIMEOUT_MS):
        self.direcciones = config.direcciones_persistencia()
        self.timeout_ms = timeout_ms
        self.contexto = zmq.Context.instance()
        self.sockets = [None] * len(self.direcciones)
        self.turno = 0

    def _socket(self, i):
        if self.sockets[i] is None:
            s = self.contexto.socket(zmq.REQ)
            s.setsockopt(zmq.RCVTIMEO, self.timeout_ms)
            s.setsockopt(zmq.SNDTIMEO, self.timeout_ms)
            s.setsockopt(zmq.LINGER, 0)
            s.connect(self.direcciones[i])
            self.sockets[i] = s
        return self.sockets[i]

    def _llamar_nodo(self, i, mensaje):
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
        for i, s in enumerate(self.sockets):
            if s is not None:
                s.close()
                self.sockets[i] = None
