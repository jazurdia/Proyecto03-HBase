import json
import os
import time

from Hfile import Hfile

base = "hfiles/"


def create_table(table_name):
    hfile = load_table(table_name)
    if hfile.data == None or hfile.metadata == None:
        hfile.metadata = {
            "enabled": True,
            "name": table_name,
            "creation_timeStamp": int(time.time()),
        }
        hfile.data = {
            "index_column": [],
            "families": {}
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


if __name__ == "__main__":

    os.system("cls")

    print(list_tables())


    ob = Hfile("test")
    print(f"Metadata: \n{ob.metadata}")
    print(f"\nData: \n{ob.data}")

    new_name = "test2"
    
    create_table(new_name)
    tablaPrueba = load_table(new_name)
    print(f"Metadata: \n{tablaPrueba.metadata}")
    print(f"\nData: \n{tablaPrueba.data}")
    #save_table(tablaPrueba)

    print(list_tables())
    

