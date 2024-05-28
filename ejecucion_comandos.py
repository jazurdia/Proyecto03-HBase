import Hbase as hbase
import time

def limpiar_comando(comando):
    comando = comando.replace("create", "")
    comando = comando.replace("is_enable", "")
    comando = comando.replace("enable", "")
    comando = comando.replace("disable", "")
    comando = comando.replace("table", "")
    comando = comando.replace("describe", "")
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
    
if __name__ == "__main__":
    print("Hola mundo")