import Hbase as hbase
import time

def identificar_comando(comando):
    comando = comando.lower().strip()
    if comando.startswith("create"):
        #crear tabla
        return ejecutar_create(comando)
    elif comando == "list":
        #listar tablas
        return ejecutar_list(comando)
    else:
        return False, "ERROR: Command not found"
    
    
def ejecutar_create(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        comando = comando.replace("create", "")
        comando = comando.replace("table", "")
        comando = comando.replace("'", "")
        comando = comando.replace("(", "")
        comando = comando.replace(")", "")
        comando = comando.replace(";", "")
        comando = comando.replace("\n", "")
        comando = comando.replace(" ", "")
        comando = comando.split(",")
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

if __name__ == "__main__":
    print("Hola mundo")