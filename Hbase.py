import json
import os
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


if __name__ == "__main__":

    os.system("cls")

    create_table("my_table", ["family1", "family2"])
    hfile = load_table("my_table")
    print(f"Metadata of {hfile.metadata["name"]}: {hfile.metadata}")
    print(f"Data of {hfile.metadata["name"]}: {hfile.data}")

    # probar alter table
    alter_table(hfile, ["family3", "family4"])
    print(f"Data of {hfile.metadata["name"]}: {hfile.data}")
    alter_table(hfile, ["family1", "family4"], "delete")
    print(f"Data of {hfile.metadata["name"]}: {hfile.data}")

    print(f"errores: {errores}")



    

