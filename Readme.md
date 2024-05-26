# Simulación de HBase README

## Introducción

Este proyecto simula la funcionalidad de HBase, un almacén de datos distribuido y escalable para big data. La simulación incluye la creación y gestión de tablas, columnas y filas, utilizando archivos JSON para almacenar los datos de manera similar a los HFiles de HBase.

## Estructura de Archivos

- `main.py`: El script principal que contiene la implementación de los comandos y operaciones para la simulación de HBase.
- `Hfile.py`: Una clase que representa los HFiles, usados para almacenar datos y metadatos en formato JSON.
- `hfiles/`: Un directorio donde se almacenan todos los HFiles (tablas) como archivos JSON.

## Configuración

1. Asegúrese de tener Python instalado en su sistema.
2. Clone este repositorio.
3. Cree un directorio llamado `hfiles` en la raíz del proyecto.

### Librerías Necesarias
Asegúrese de tener instaladas las siguientes librerías antes de ejecutar el proyecto:
```
import json
import os
import re
import time
from tkinter import *
import customtkinter as ctk
```
Puede instalarlas utilizando pip:

```
pip install <libreria>
```

## Uso

### Simulación de HFiles

La clase `Hfile` en `Hfile.py` maneja la carga y el guardado de archivos JSON que representan tablas en el sistema. Cada tabla tiene su propio archivo JSON en el directorio `hfiles`.

### Comandos Disponibles

La implementación proporciona varios comandos similares a los de HBase para interactuar con las tablas. A continuación, se enumeran los comandos disponibles:

#### Crear Tabla

- `create_table(table_name, families=None)`: Crea una nueva tabla con las familias de columnas especificadas.

#### Listar Tablas

- `list_tables()`: Lista todas las tablas disponibles en el directorio `hfiles`.

#### Habilitar/Deshabilitar Tabla

- `disable_table(hfile)`: Deshabilita una tabla.
- `enable_table(hfile)`: Habilita una tabla.

#### Agregar/Eliminar Familias de Columnas

- `add_column_families(hfile, family_names)`: Agrega familias de columnas a una tabla.
- `delete_column_families(hfile, family_names)`: Elimina familias de columnas de una tabla.

#### Modificar una Tabla

- `alter_table(hfile, family_names, method=None)`: Agrega o elimina familias de columnas según el método especificado.

#### Eliminar Tabla

- `drop_table(name)`: Elimina una tabla.
- `drop_all_tables(param)`: Elimina todas las tablas que coincidan con un patrón.

#### Describir una Tabla

- `describe_table(hfile)`: Muestra los metadatos de una tabla.

#### Insertar Datos

- `put(table_name, row_key, family, column, value)`: Inserta o actualiza datos en una tabla.

#### Obtener Datos

- `get(table_name, row_key, columns=None)`: Obtiene datos de una tabla.

#### Eliminar Datos

- `delete(table_name, row_key, column=None)`: Elimina datos de una tabla.
- `deleteall(table_name, row_prefix)`: Elimina todas las filas que coincidan con un prefijo.

#### Contar Filas

- `count(table_name, **kwargs)`: Cuenta las filas en una tabla.

#### Truncar Tabla

- `truncate(table_name)`: Elimina todas las filas de una tabla.

### Clase Hfile

La clase `Hfile` maneja la interacción con los archivos JSON que representan las tablas en el sistema. Se encarga de cargar y guardar estos archivos, asegurando que los datos y metadatos estén correctamente manejados.

