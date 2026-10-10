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
  cliente/         Clientes y generación de carga
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

Cada servicio corre en su propia terminal, siempre desde la carpeta `SRB`, en este orden:

1. Crear la base de datos (50 eventos, 20 con dos ocurrencias, 100 asientos cada uno):
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
4. Gestor de Transacciones:
```
   python gt/servidor_gt.py
```
5. Cancelaciones y Notificaciones:
```
   python cancelaciones/servidor_cancelaciones.py
```
## Pruebas rápidas por servicio

- Persistencia (con su servidor activo): `python persistencia/probar_persistencia.py`
- RMC (con Persistencia y RMC activos): `python rmc/probar_rmc.py`

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

Luego copiar o clonar el código y ajustar los `HOST_*` de `config.py` a las IP de las VMs.
