import json
import os
import re
import time

from Hfile import Hfile

base = "hfiles/"

errores = []

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
    hfile.save_hfile()
    
def list_tables():
    tables = []
    for filename in os.listdir(base):  # use base directory
        if filename.endswith(".json"):
            tables.append(filename)
            #remove the .json extension
    tables = [table.replace(".json", "") for table in tables]
    
    return tables  # return the list of tables

def disable_table(hfile):
    hfile.metadata["enabled"] = False
    save_table(hfile)

def enable_table(hfile):
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

def alter_table(hfile, family_names, method=None):
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

# Ejemplo de uso
if __name__ == "__main__":
    os.system("cls")

    create_table("table2", ["cf1", "cf2"])
    put("table2", "row1", "cf1", "col1", "value1")
    put("table2", "row1", "cf1", "col2", "value2")
    put("table2", "row1", "cf2", "col1", "value3")

    # Casos de prueba
    print(get("table2", "row1"))
    print()
    print(get("table2", "row1", "cf1:col1"))
    print()
    print(get("table2", "row1", ["cf1:col1", "cf1:col2"]))
    print()
    print(get("table2", "row1", "cf1"))
