import Hbase as hbase
import time

def limpiar_comando(comando):
    comando = comando.replace("create", "")
    comando = comando.replace("is_enable", "")
    comando = comando.replace("enable", "")
    comando = comando.replace("disable", "")
    comando = comando.replace("drop_all", "")
    comando = comando.replace("drop", "")
    #comando = comando.replace("table", "")
    comando = comando.replace("describe", "")
    comando = comando.replace("alter", "")
    comando = comando.replace("put", "")
    comando = comando.replace("'", "")
    comando = comando.replace("(", "")
    comando = comando.replace(")", "")
    comando = comando.replace(";", "")
    comando = comando.replace("\n", "")
    comando = comando.replace(" ", "")
    return comando

def identificar_comando(comando):
    comando = comando.lower().strip()
    if comando.startswith("create"):
        #crear tabla
        return ejecutar_create(comando)
    elif comando == "list":
        #listar tablas
        return ejecutar_list(comando)
    elif comando.startswith("disable"):
        #deshabilitar tabla
        return ejecutar_disable(comando)
    elif comando.startswith("enable"):
        #habilitar tabla
        return ejecutar_enable(comando)
    elif comando.startswith("is_enable"):
        #verificar si tabla esta habilitada
        return ejecutar_is_enable(comando)
    elif comando.startswith("describe"):
        return ejecutar_describe(comando)
    elif comando.startswith("alter"):
        return ejecutar_alter(comando) 
    elif comando.startswith("drop_all"):
        return ejecutar_drop_all(comando) 
    elif comando.startswith("drop"):
        return ejecutar_drop(comando)      
    elif comando.startswith("put"):
        return ejecutar_put(comando)
    else:
        return False, "ERROR: Command not found"
    
    
def ejecutar_create(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando[0]
        columnas = comando[1:]
        if len(columnas) == 0:
            return False, "ERROR: SyntaxError: No columns specified"
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        hbase.create_table(tabla, columnas)
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error creating table"
    
def ejecutar_list(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        tablas = hbase.list_tables()
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        resultado = ""
        resultado += "TABLE\n"
        rows = 0
        for tabla in tablas:
            rows += 1
            resultado += tabla + "\n"
        return True, resultado + str(rows) + " row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error listing tables"

def ejecutar_disable(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        hbase.disable_table(tabla)
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error disabling table"
    
def ejecutar_enable(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        hbase.enable_table(tabla)
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error enabling table"
    
def ejecutar_is_enable(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        hfile = hbase.load_table(tabla)
        enabled = hbase.is_enable(hfile)
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        return True, str(enabled) + "\n" + "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error checking if table is enabled"

def ejecutar_describe(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        hfile = hbase.load_table(tabla)
        columnas = {}
        columnas = hbase.describe_table(hfile)
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        resultado = ""
        resultado += "DESCRIPTION\n"
        rows = len(columnas)
        for key, value in columnas.items():
            resultado += f"{key}: {value}\n"
        return True, resultado + str(rows) + " row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"

    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error describing table"
    
def ejecutar_alter(comando):
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando.split(",")[0]
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        else:
            columnas = comando.split(",")[1:]
            if len(columnas) == 0:
                return False, "ERROR: SyntaxError: No columns specified"
            
            method = None
            if 'method' in columnas[-1]:
                method = columnas[-1].split('=>')[-1].strip().lower().replace("}", "").replace("{", "")
                columnas = columnas[:-1]
                for i in range(len(columnas)):
                    columnas[i] = columnas[i].replace("{name=>", "")
            
            hbase.alter_table(tabla, columnas, method)
        
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        return True, "Updating all regions with the new schema...\n0/1 regions updated.\n1/1 regions updated.\nDone.\n0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error altering table"
    
def ejecutar_drop(comando):
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        hbase.drop_table(tabla)
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error dropping table"
    
def ejecutar_drop_all(comando):
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        _, files = hbase.drop_all_tables(comando)
        for i in range(len(files)):
            #replace .json
            files[i] = files[i].replace(".json", "")            
        errores = hbase.get_errores()
        cont = len(files)
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        tiempo_final = time.time()
        return True, "\n".join(files) + "\n" + "droping the above " + str(cont) + " tables\n0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error dropping all tables"
    
def ejecutar_put(comando):
    # put 'mi_tabla', 'fila1', 'mi_familia:columna1', 'valor1'
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando.split(",")[0]
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        else:
            fila = comando.split(",")[1]
            if len(fila) == 0:
                return False, "ERROR: SyntaxError: No row specified"
            else:
                columna = comando.split(",")[2]
                familia = columna.split(":")[0]
                columna = columna.split(":")[1]
                if len(columna) == 0:
                    return False, "ERROR: SyntaxError: No column specified"
                else:
                    valor = comando.split(",")[3]
                    if len(valor) == 0:
                        return False, "ERROR: SyntaxError: No value specified"
                    else:
                        hbase.put(tabla, fila, familia, columna, valor)
                        errores = hbase.get_errores()
                        if len(errores) > 0:
                            return False, "ERROR: " + errores[0]
                        tiempo_final = time.time()
                        return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error inserting data"
    
if __name__ == "__main__":
    print("Hola mundo")