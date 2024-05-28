import json
import os
import re
import time

from Hfile import Hfile

base = "hfiles/"

errores = []


# Funcion que devuelve los errores. Debe limpiar la lista de errores después de cada uso.
def get_errores():
    global errores
    errores_list = errores
    errores = []
    return errores_list

def create_table(table_name, families=None):
    hfile = load_table(table_name)
    if hfile.data == None or hfile.metadata == None:
        
        fam = {}
        if families is not None:
            for family in families:
                fam[family] = {}

        hfile.metadata = {
            "enabled": True,
            "name": table_name,
            "creation_timeStamp": int(time.time()),
        }
        hfile.data = {
            "index_column": [],
            "families": fam
        }
        
        hfile.save_hfile()
    else:
        print("Table already exists") # Cambiar



def load_table(table_name):
    return Hfile(table_name)

def save_table(hfile):
    clean_table(hfile)
    hfile.save_hfile()
    
def list_tables():
    tables = []
    for filename in os.listdir(base):  # use base directory
        if filename.endswith(".json"):
            tables.append(filename)
            #remove the .json extension
    tables = [table.replace(".json", "") for table in tables]
    
    return tables  # return the list of tables

def disable_table(table_name: str):
    hfile = load_table(table_name)
    hfile.metadata["enabled"] = False
    save_table(hfile)

def enable_table(table_name: str):
    hfile = load_table(table_name)
    hfile.metadata["enabled"] = True
    save_table(hfile)

def is_enable(hfile):
    return hfile.metadata["enabled"]
        
## metodos para usarse en alter_table. 


def add_column_families(hfile, family_names): 
    if hfile.metadata["enabled"]:
        for family_name in family_names:
            hfile.data["families"][family_name] = {}
        save_table(hfile)
        return True
    else:
        errores.append("Table is disabled")
        return False


def delete_column_families(hfile, family_names): 
    if hfile.metadata["enabled"]:
        for family_name in family_names:
            if family_name in hfile.data["families"]:
                del hfile.data["families"][family_name]
            else:
                errores.append(f"Family {family_name} does not exist")
        save_table(hfile)
        return True
    else:
        errores.append("Table is disabled")
        return False


# valdria la pena pensar si alter debería ser manejado desde el frontend, o si se hace un método alter. 

# alter 'my_table', 'new_cf' 
# o 
# alter 'my_table', {NAME => 'old_cf', METHOD => 'delete'}


## CAMBIALO AZURDIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
def alter_table(table_name, family_names, method=None):
    hfile = load_table(table_name)
    if method == "delete":
        return delete_column_families(hfile, family_names)
    else:
        return add_column_families(hfile, family_names)
    


# Drop
def drop_table(name: str):
    table = load_table(name)
    if table.metadata == None or table.data == None:
        errores.append("Table does not exist")
        return False
    else:
        try:
            os.remove(f"{base}{name}.json")
            return True
        except Exception as e:
            errores.append(f"Error removing {name}: {str(e)}")
            return False
    

# drop all
def drop_all_tables(param=None):
    if param is None:
        errores.append("A parameter is required")
        return False
    
    # Compilar el patrón regex
    pattern = re.compile(param)
    
    # Lista de archivos en el directorio base
    files = os.listdir(base)
    
    # Filtrar archivos que coincidan con el patrón regex
    files_to_remove = [f for f in files if pattern.match(f)]
    
    if not files_to_remove:
        errores.append("No files matched the pattern")
        return False
    
    # Intentar eliminar cada archivo coincidente
    for filename in files_to_remove:
        filepath = os.path.join(base, filename)
        try:
            os.remove(filepath)
        except Exception as e:
            errores.append(f"Error removing {filename}: {str(e)}")
            continue

    return True

# describe
def describe_table(hfile):
    return hfile.metadata

# *************************************** EMPIEZA DML **************************************** #

def clean_table(hfile):
    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return hfile

    rows_to_delete = []

    if hfile.data["index_column"] == []:
        return hfile

    # Verificar filas con todas las celdas en null o ""
    for row_index, index_row in enumerate(hfile.data["index_column"]):
        all_empty = True
        for family in hfile.data["families"]:
            for col in hfile.data["families"][family]:
                cell = hfile.data["families"][family][col][row_index]
                if cell["value"] is not None and cell["value"] != "":
                    all_empty = False
                    break
            if not all_empty:
                break
        
        if all_empty:
            rows_to_delete.append(row_index)

    # Eliminar filas que están completamente vacías
    for row_index in sorted(rows_to_delete, reverse=True):
        del hfile.data["index_column"][row_index]
        for family in hfile.data["families"]:
            for col in hfile.data["families"][family]:
                del hfile.data["families"][family][col][row_index]

    return hfile

def put(table_name, row_key, family, column, value):
    hfile = load_table(table_name)
    
    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return False

    if not is_enable(hfile):
        errores.append("Table is disabled")
        return False

    current_time = int(time.time())

    # Verificar si el índice de la fila ya existe
    row_exists = any(index["value"] == row_key for index in hfile.data["index_column"])

    if not row_exists:
        # Agregar la nueva fila al index_column
        hfile.data["index_column"].append({"timeStamp": current_time, "value": row_key})
        row_index = len(hfile.data["index_column"]) - 1
        
        # Inicializar todas las columnas para la nueva fila
        for fam in hfile.data["families"]:
            for col in hfile.data["families"][fam]:
                if fam == family and col == column:
                    hfile.data["families"][fam][col].append({"timeStamp": current_time, "value": value})
                else:
                    hfile.data["families"][fam][col].append({"timeStamp": None, "value": ""})
    else:
        # Obtener el índice de la fila existente
        row_index = next(index for index, index_row in enumerate(hfile.data["index_column"]) if index_row["value"] == row_key)
        
        # Actualizar el valor de la columna existente
        for col in hfile.data["families"][family]:
            if col == column:
                hfile.data["families"][family][col][row_index] = {"timeStamp": current_time, "value": value}
            elif hfile.data["families"][family][col][row_index]["value"] == "":
                hfile.data["families"][family][col][row_index] = {"timeStamp": None, "value": ""}

    # Si la columna no existe, agregarla
    if column not in hfile.data["families"][family]:
        hfile.data["families"][family][column] = [{"timeStamp": None, "value": ""} for _ in hfile.data["index_column"]]
        hfile.data["families"][family][column][row_index] = {"timeStamp": current_time, "value": value}

    save_table(hfile)
    return True

def get(table_name, row_key, columns=None):
    hfile = load_table(table_name)
    
    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return None

    if not is_enable(hfile):
        errores.append("Table is disabled")
        return None

    # Verificar si el índice de la fila existe
    row_index = next((index for index, index_row in enumerate(hfile.data["index_column"]) if index_row["value"] == row_key), None)
    
    if row_index is None:
        errores.append("Row does not exist")
        return None

    # Si no se especifican columnas, devolver todas las familias y columnas para la fila
    if columns is None:
        result = {}
        for family in hfile.data["families"]:
            result[family] = {}
            for column in hfile.data["families"][family]:
                result[family][column] = hfile.data["families"][family][column][row_index]
        return result

    # Si se especifica una familia o columna
    if isinstance(columns, str):
        if ':' in columns:
            family, column = columns.split(':')
            if family in hfile.data["families"] and column in hfile.data["families"][family]:
                return hfile.data["families"][family][column][row_index]
            else:
                errores.append("Family or column does not exist")
                return None
        else:
            family = columns
            if family in hfile.data["families"]:
                result = {}
                for column in hfile.data["families"][family]:
                    result[column] = hfile.data["families"][family][column][row_index]
                return result
            else:
                errores.append("Family does not exist")
                return None

    # Si se especifica una lista de columnas
    if isinstance(columns, list):
        result = {}
        for item in columns:
            if ':' in item:
                family, column = item.split(':')
                if family in hfile.data["families"] and column in hfile.data["families"][family]:
                    if family not in result:
                        result[family] = {}
                    result[family][column] = hfile.data["families"][family][column][row_index]
                else:
                    errores.append(f"Family or column {item} does not exist")
        return result

    errores.append("Invalid columns parameter")
    return None
# hbase(main):001:0> delete 'my_table', 'row1', 'cf1:column1'
# hbase(main):003:0> delete 'my_table', 'row1', 'cf1'

def delete(table_name, row_key, column=None):
    hfile = load_table(table_name)

    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return False
    
    if not is_enable(hfile):
        errores.append("Table is disabled")
        return False
    
    # Verificar si el índice de la fila existe
    row_index = next((index for index, index_row in enumerate(hfile.data["index_column"]) if index_row["value"] == row_key), None)

    if row_index is None:
        errores.append("Row does not exist")
        return False
    
    if column is None:
        # Eliminar toda la fila (poner en null, "")
        for family in hfile.data["families"]:
            for col in hfile.data["families"][family]:
                hfile.data["families"][family][col][row_index] = {"timeStamp": None, "value": ""}
    else:
        if ':' in column:
            family, col = column.split(':')
            if family in hfile.data["families"] and col in hfile.data["families"][family]:
                hfile.data["families"][family][col][row_index] = {"timeStamp": None, "value": ""}
            else:
                errores.append("Family or column does not exist")
                return False
        else:
            family = column
            if family in hfile.data["families"]:
                for col in hfile.data["families"][family]:
                    hfile.data["families"][family][col][row_index] = {"timeStamp": None, "value": ""}
            else:
                errores.append("Family does not exist")
                return False

    save_table(hfile)
    return True

def deleteall(table_name, row_prefix):
    hfile = load_table(table_name)

    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return False

    if not is_enable(hfile):
        errores.append("Table is disabled")
        return False
    
    # compilamos el patrón refex para el prefijo de la fila
    pattern = re.compile(row_prefix)

    # filtrar las filas que coincidan con el prefijo
    matching_indexes = [index for index, index_row in enumerate(hfile.data["index_column"]) if pattern.match(index_row["value"])]

    if not matching_indexes:
        errores.append("No rows matched the prefix")
        return False
    
    # llegando aqui, deberiamos conocer todas las filas que coinciden con el patrón.
    for row_index in matching_indexes:
        for family in hfile.data["families"]:
            for col in hfile.data["families"][family]:
                hfile.data["families"][family][col][row_index] = {"timeStamp": None, "value": ""}

    save_table(hfile)
    return True


# Count
"""
count 'my_table'
count 'my_table', INTERVAL => 100
count 'my_table', LIMIT => 500
"""

def count(table_name, **kwargs):
    hfile = load_table(table_name)

    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return 0

    total_rows = len(hfile.data["index_column"])
    interval = kwargs.get('INTERVAL', None)
    limit = kwargs.get('LIMIT', None)

    if interval is None and limit is None:
        return total_rows

    results = {}
    start_time = time.time()

    if interval is not None:
        intervals = []
        for i in range(0, total_rows, interval):
            end_time = time.time()
            elapsed_time = end_time - start_time
            intervals.append((elapsed_time, min(interval, total_rows - i)))
            time.sleep(0.01)  # Simulamos algún trabajo para poder medir el tiempo

        results['intervals'] = intervals
        results['total_count'] = total_rows
        return results

    if limit is not None:
        limited_count = min(total_rows, limit)
        return limited_count

def truncate(table_name):
    hfile = load_table(table_name)

    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return False

    if not is_enable(hfile):
        errores.append("Table is disabled")
        return False
    
    # eliminamos todas las rows
    hfile.data["index_column"] = []

    # elimnamos todos los datos de las columnas, dejando las columnas
    for family in hfile.data["families"]:
        for col in hfile.data["families"][family]:
            hfile.data["families"][family][col] = []

    save_table(hfile)
    return True

"""
Sintaxis del comando: scan 'nombre_de_la_tabla', { OPTIONS }

scan 'my_table', {STARTROW => 'row1'}
scan 'my_table', {COLUMNS => ['cf1:column1', 'cf2:column2']}
scan 'my_table', {FILTER => "ValueFilter(=, 'binary:value1')"}
scan 'my_table', {LIMIT => 10}

Elementos completos
hbase(main):001:0> scan 'my_table'
hbase(main):002:0> scan 'my_table', {STARTROW => 'row1', STOPROW => 'row10'}
hbase(main):003:0> scan 'my_table', {COLUMNS => ['cf1:column1', 'cf2:column2']}
hbase(main):004:0> scan 'my_table', {FILTER => "ValueFilter(=, 'binary:value1')"}
hbase(main):005:0> scan 'my_table', {LIMIT => 10}
"""
def scan(table_name, **options):
    hfile = load_table(table_name)

    if hfile.data is None or hfile.metadata is None:
        errores.append("Table does not exist")
        return None

    if not is_enable(hfile):
        errores.append("Table is disabled")
        return None

    start_row = options.get('STARTROW', None)
    stop_row = options.get('STOPROW', None)
    columns = options.get('COLUMNS', None)
    filter_expression = options.get('FILTER', None)
    limit = options.get('LIMIT', None)

    result = []
    row_indices = range(len(hfile.data["index_column"]))
    if start_row is not None:
        start_index = next((i for i, row in enumerate(hfile.data["index_column"]) if row["value"] == start_row), None)
        row_indices = range(start_index, len(hfile.data["index_column"])) if start_index is not None else row_indices

    if stop_row is not None:
        stop_index = next((i for i, row in enumerate(hfile.data["index_column"]) if row["value"] == stop_row), None)
        row_indices = range(min(row_indices.start, row_indices.stop, stop_index)) if stop_index is not None else row_indices

    for row_index in row_indices:
        row_data = hfile.data["index_column"][row_index]
        row_result = {"row": row_data["value"], "columns": {}}

        for family in hfile.data["families"]:
            for column in hfile.data["families"][family]:
                if columns is None or f"{family}:{column}" in columns:
                    cell = hfile.data["families"][family][column][row_index]
                    if filter_expression is None or eval_filter(cell, filter_expression):
                        if family not in row_result["columns"]:
                            row_result["columns"][family] = {}
                        row_result["columns"][family][column] = cell

        result.append(row_result)
        if limit is not None and len(result) >= limit:
            break

    return result

def eval_filter(cell, filter_expression):
    if "ValueFilter" in filter_expression:
        match = re.match(r"ValueFilter\(\s*=\s*,\s*'binary:(.+)'\s*\)", filter_expression)
        if match:
            value = match.group(1)
            return cell["value"] == value
    return True




def pretty_print_json(json_data):
    print(json.dumps(json_data, indent=4))

if __name__ == "__main__":
    os.system("cls")

    # Ejemplos de uso
    result1 = scan('my_table', STARTROW='row1')
    print(f"scan my_table, STARTROW='row1'-> {result1}\n\n")

    result2 = scan('my_table', COLUMNS=['cf1:column1', 'cf2:column2'])
    print(f"scan my_table, COLUMNS=['cf1:column1', 'cf2:column2']-> {result2}\n\n")

    result3 = scan('my_table', FILTER="ValueFilter(=, 'binary:value1')")
    print(f"scan my_table, FILTER=\"ValueFilter(=, 'binary:value1') -> {result3}\n\n")

    result4 = scan('my_table', LIMIT=10)
    print(f"scan my_table, LIMIT=10-> {result4}\n\n")

    