# SRB - Sistema de Reserva de Boletos

Introducción a Sistemas Distribuidos - PUJ 2026-30

Sistema distribuido de tres capas (consumidores, negocio y almacenamiento) que se comunica con ZeroMQ.

## Requisitos

- Python 3.10 o superior
- pyzmq: `pip install pyzmq`

## Estructura

```
SRB/
  config.py        Hosts y puertos de todos los servicios
  persistencia/    Persistencia (nodos A y B con replicación primario-secundario) y creación de las BD (SQLite)
  cliente_persistencia.py  Cliente que usan RMC y Cancelaciones para hablar con los dos nodos
  pruebas/         Pruebas de concurrencia
  rmc/             Servicio RMC (Reserva, Modificación y Consultas)
  gt/              Gestor de Transacciones
  cliente/         Cliente y generador de carga
  cancelaciones/   Servicio de Cancelaciones y Notificaciones
  docs/            Informe y diagramas
```

## Puertos (config.py)

| Servicio | Puerto |
|---|---|
| Gestor de Transacciones (principal / respaldo) | 5550 / 5551 |
| RMC (instancia 1 / 2) | 5555 / 5556 |
| Persistencia nodo A (clientes / replicación) | 5560 / 5562 |
| Persistencia nodo B (clientes / replicación) | 5561 / 5563 |
| Cancelaciones | 5570 |

Los hosts se cambian en `config.py` o, sin tocar código, con variables de entorno:
`SRB_HOST_GT`, `SRB_HOST_RMC`, `SRB_HOST_CANCELACIONES`, `SRB_HOST_PERSISTENCIA` (nodo A) y `SRB_HOST_PERSISTENCIA_2` (nodo B, por defecto el mismo host que A).

## Arquitectura escalable y tolerante a fallos

- **Concurrencia**: GT, RMC y Persistencia reciben las solicitudes con un `ROUTER` que las reparte (`DEALER` interno) entre varios hilos trabajadores, así atienden muchos clientes a la vez.
- **Réplicas de RMC**: el GT las usa con Round Robin. Si una no responde, se marca como caída unos segundos y la solicitud se reenvía a la otra con el mismo `id`.
- **Persistencia primario-secundario**: dos nodos con su propia BD. Las escrituras solo las hace el primario, que las anota en una bitácora (`log_ops`) y las replica de forma síncrona al secundario antes de confirmar. Las lecturas (consultas y disponibilidad) se reparten entre los dos nodos.
- **Sin sobreventa**: las escrituras se serializan en el primario y Persistencia vuelve a validar los asientos dentro de la transacción; cada solicitud tiene un `id` y una escritura repetida devuelve la respuesta guardada (idempotencia).
- **Falla del primario**: el secundario detecta la caída (heartbeat o primera escritura que le llega) y se promueve a primario. Al volver el nodo caído arranca como secundario y se pone al día con `SYNC`.
- **Falla de un GT**: el cliente prueba el otro GT con el mismo `id`.
- **Métricas**: el GT cuenta las transacciones por minuto (se imprimen cada 10 s; la operación `ESTADISTICAS` las devuelve).

Variable `SRB_SILENCIO=1`: apaga el log por solicitud (recomendado en las pruebas de carga).

Variable `SRB_BASE=1`: arquitectura base. Deja un solo GT (5550), un solo RMC (5555) y solo el nodo A de Persistencia, sin replicación. Hay que exportarla en todas las máquinas, también en la del cliente, y arrancar solo `persistencia ... A`, `rmc ... 5555` y `gt ... 5550`; así no se pagan timeouts por réplicas que no están encendidas.

## Ejecución

Cada servicio corre en su propia terminal, siempre desde la carpeta raíz del proyecto. En Linux usar `python3` en lugar de `python`.

1. Crear las bases de datos (50 eventos, 20 con dos ocurrencias, 100 asientos cada uno; se crean la del nodo A y una copia idéntica para el B). Las BD no vienen en el repositorio:

```
   python persistencia/crear_bd.py
```

2. Persistencia, un proceso por nodo (A arranca como primario, B como secundario):

```
   python persistencia/servidor_persistencia.py A
   python persistencia/servidor_persistencia.py B
```

3. RMC (dos instancias):

```
   python rmc/servidor_rmc.py 5555
   python rmc/servidor_rmc.py 5556
```

4. Cancelaciones y Notificaciones:

```
   python cancelaciones/servidor_cancelaciones.py
```

5. Gestor de Transacciones (principal y respaldo):

```
   python gt/servidor_gt.py 5550
   python gt/servidor_gt.py 5551
```

Cada servicio imprime un mensaje de "escuchando en el puerto..." y queda a la espera de solicitudes. Para ver el estado de las BD en cualquier momento: `python persistencia/ver_bd.py`.

## Uso del cliente

Con los cuatro servicios activos, en otra terminal:

```
python cliente/cliente.py consulta 11
python cliente/cliente.py reserva C1 E1 2026-11-05 2
python cliente/cliente.py modificar C1 1 4
python cliente/cliente.py modificar C1 1 - 2026-11-19
python cliente/cliente.py cancelar C1 1
```

- `consulta <mes>`: lista los eventos del mes.
- `reserva <cliente> <evento> <fecha> <asientos>`: reserva asientos de forma síncrona.
- `modificar <cliente> <reserva_id> <nuevos_asientos|-> [nueva_fecha]`: cambia el número de asientos, la fecha o ambos (`-` = no cambiar los asientos). La fecha solo se puede cambiar a otra ocurrencia del mismo evento; si no se puede, se rechaza y la reserva queda igual.
- `cancelar <cliente> <reserva_id> [puerto_notif] [ip_notif]`: la cancelación es asíncrona; el sistema responde que la aceptó y después llega una notificación al cliente cuando se efectúa. Por defecto la notificación llega al puerto 6001 y a la IP con la que esta máquina llega a `HOST_CANCELACIONES` (se detecta sola, sirve en el despliegue de 3 máquinas). Si no funciona, se puede indicar la IP a mano.

## Generador de carga

```
python cliente/generador_carga.py <clientes> <solicitudes_por_minuto> <duracion_segundos> <porcentaje_consultas>
```

Ejemplo: 8 clientes, 20 solicitudes por minuto cada uno, durante 60 s, con 70 % consultas y 30 % reservas:

```
python cliente/generador_carga.py 8 20 60 70
```

Con el quinto argumento `enunciado` cada cliente toma un perfil distinto, como pide el enunciado:

```
python cliente/generador_carga.py 3 60 60 50 enunciado
python cliente/generador_carga.py 8 60 60 50 enunciado
```

- 3 clientes: 2 solo consultan y 1 consulta y reserva (con el porcentaje indicado).
- 5 u 8 clientes: la mitad (`N // 2`) solo consulta y el resto hace reservas (60 %) y modificaciones (40 %, sobre reservas propias; cambian el número de asientos).

El CSV incluye la columna `perfil` de cada solicitud.

Al terminar muestra el tiempo promedio, la desviación estándar, los rechazos, los errores y las transacciones por minuto, y guarda cada solicitud en `resultados_carga.csv`.

## Pruebas rápidas por servicio

- Persistencia (con los dos nodos activos): `python persistencia/probar_persistencia.py`
- RMC (con Persistencia y RMC activos): `python rmc/probar_rmc.py`
- Cancelaciones (con Persistencia y Cancelaciones activos): `python cancelaciones/probar_cancelaciones.py`
- Concurrencia (todo activo): `python pruebas/probar_concurrencia.py 20 10` — 20 clientes reservan a la vez el mismo evento; comprueba que no hay sobreventa y que los nodos A y B quedan idénticos.

Para dejar la BD limpia después de probar, detener todos los servicios con `Ctrl + C` y ejecutar de nuevo `python persistencia/crear_bd.py`.

## Pruebas de tolerancia a fallos

Con todo activo y una carga corriendo (`python cliente/generador_carga.py 5 60 60 30`):

1. Detener el RMC 5555 (`Ctrl + C`): el GT lo marca como caído y las solicitudes siguen por el 5556, sin errores para el cliente.
2. Detener el nodo A de Persistencia: el nodo B se promueve a primario (se ve `PROMOVIDO A PRIMARIO` en su terminal) y las reservas continúan.
3. Volver a arrancar el nodo A: entra como secundario, se sincroniza y `python persistencia/ver_bd.py` muestra que A y B coinciden.
4. Detener el GT 5550: el cliente pasa solo al GT 5551.

## Despliegue en 3 máquinas

- VM1: capa de consumidores (clientes y generador de carga)
- VM2: capa de negocio (GT x2, RMC x2 y Cancelaciones)
- VM3: capa de almacenamiento (Persistencia nodos A y B y sus BD)

Pasos en cada VM (Ubuntu 22.04):

```
sudo apt update
sudo apt install python3-pip
pip3 install pyzmq
```

Luego clonar el código. No hace falta editar `config.py`: se definen las IP con variables de entorno antes de lanzar los procesos. Ejemplo con VM1 = 10.0.0.11, VM2 = 10.0.0.12, VM3 = 10.0.0.13:

```
# VM1 (clientes)
export SRB_HOST_GT=10.0.0.12 SRB_HOST_CANCELACIONES=10.0.0.12

# VM2 (negocio)
export SRB_HOST_RMC=10.0.0.12 SRB_HOST_CANCELACIONES=10.0.0.12 SRB_HOST_PERSISTENCIA=10.0.0.13

# VM3 (almacenamiento)
export SRB_HOST_PERSISTENCIA=10.0.0.13
python3 persistencia/crear_bd.py
```

Abrir en los firewalls los puertos de la tabla (5550-5551, 5555-5556, 5560-5563, 5570 y 6001 en el cliente para las notificaciones). Los nodos A y B de Persistencia comparten la VM3 porque el enunciado pide una máquina por capa; si hubiera una cuarta máquina, B puede ir allí con `SRB_HOST_PERSISTENCIA_2`.
