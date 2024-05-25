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
def drop_table(hfile):
    if hfile.metadata["enabled"]:
        os.remove(base + hfile.metadata["name"] + ".json")
    else:
        errores.append("Table is disabled")
        return False
    return True

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


if __name__ == "__main__":

    os.system("cls")

    switch = 3

    if switch == 1:

        create_table("test1", ["cf1", "cf2"])
        create_table("test2", ["cf1", "cf2"])
        create_table("test3", ["cf1", "cf2"])

        create_table("log_data1", ["cf1", "cf2"])
        create_table("datalog", ["cf1", "cf2"])
        create_table("log_data_test", ["cf1", "cf2"])

        tables = list_tables()
        print(f"Tables: {tables}")

    elif switch == 2:
        drop_all_tables('.*data.*')  # Eliminar todos los archivos que contienen la palabra "data"
        print(f"tablas: {list_tables()}")
    elif switch == 3:
        drop_all_tables('test.*') # Eliminar todos los archivos que comienzan con "test"
        print(f"tablas: {list_tables()}")


    



    

