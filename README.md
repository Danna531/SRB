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
  persistencia/    Servicio de Persistencia y creación de la BD (SQLite)
  rmc/             Servicio RMC (Reserva, Modificación y Consultas)
  gt/              Gestor de Transacciones
  cliente/         Cliente y generador de carga
  cancelaciones/   Servicio de Cancelaciones y Notificaciones
  docs/            Informe y diagramas
```

## Puertos (config.py)

| Servicio | Puerto |
|---|---|
| Gestor de Transacciones | 5550 |
| RMC | 5555 |
| Persistencia | 5560 |
| Cancelaciones | 5570 |

Para ejecutar en máquinas distintas, solo se cambian los `HOST_*` en `config.py`.

## Ejecución

Cada servicio corre en su propia terminal, siempre desde la carpeta raíz del proyecto. En Linux usar `python3` en lugar de `python`.

1. Crear la base de datos (50 eventos, 20 con dos ocurrencias, 100 asientos cada uno). La BD no viene en el repositorio, hay que crearla antes de arrancar:

```
   python persistencia/crear_bd.py
```

2. Persistencia:

```
   python persistencia/servidor_persistencia.py
```

3. RMC:

```
   python rmc/servidor_rmc.py
```

4. Cancelaciones y Notificaciones:

```
   python cancelaciones/servidor_cancelaciones.py
```

5. Gestor de Transacciones:

```
   python gt/servidor_gt.py
```

Cada servicio imprime un mensaje de "escuchando en el puerto..." y queda a la espera de solicitudes.

## Uso del cliente

Con los cuatro servicios activos, en otra terminal:

```
python cliente/cliente.py consulta 11
python cliente/cliente.py reserva C1 E1 2026-11-05 2
python cliente/cliente.py cancelar C1 1
```

- `consulta <mes>`: lista los eventos del mes.
- `reserva <cliente> <evento> <fecha> <asientos>`: reserva asientos de forma síncrona.
- `cancelar <cliente> <reserva_id>`: la cancelación es asíncrona; el sistema responde que la aceptó y después llega una notificación al cliente cuando se efectúa.

## Generador de carga

```
python cliente/generador_carga.py <clientes> <solicitudes_por_minuto> <duracion_segundos> <porcentaje_consultas>
```

Ejemplo: 8 clientes, 20 solicitudes por minuto cada uno, durante 60 s, con 70 % consultas y 30 % reservas:

```
python cliente/generador_carga.py 8 20 60 70
```

Al terminar muestra el tiempo promedio, la desviación estándar, los rechazos, los errores y las transacciones por minuto, y guarda cada solicitud en `resultados_carga.csv`.

## Pruebas rápidas por servicio

- Persistencia (con su servidor activo): `python persistencia/probar_persistencia.py`
- RMC (con Persistencia y RMC activos): `python rmc/probar_rmc.py`
- Cancelaciones (con Persistencia y Cancelaciones activos): `python cancelaciones/probar_cancelaciones.py`

Para dejar la BD limpia después de probar, detener los servicios con `Ctrl + C` y ejecutar de nuevo `python persistencia/crear_bd.py`.

## Despliegue en 3 máquinas

- VM1: capa de consumidores (clientes)
- VM2: capa de negocio (GT, RMC y Cancelaciones)
- VM3: capa de almacenamiento (Persistencia y BD)

Pasos en cada VM (Ubuntu 22.04):

```
sudo apt update
sudo apt install python3-pip
pip3 install pyzmq
```

Luego clonar el código y ajustar los `HOST_*` de `config.py` a las IP de las VMs.