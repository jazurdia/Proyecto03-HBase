import Hbase as hbase
import time
import random
import re

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
    comando = comando.replace("get", "")
    comando = comando.replace("deleteall", "")
    comando = comando.replace("delete", "")
    comando = comando.replace("count", "")
    comando = comando.replace("truncate", "")
    comando = comando.replace("scan", "")
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
    elif comando.startswith("get"):
        return ejecutar_get(comando)
    elif comando.startswith("delete"):
        return ejecutar_delete(comando)
    elif comando.startswith("deleteall"):
        return ejecutar_deleteall(comando)
    elif comando.startswith("count"):
        return ejecutar_count(comando)
    elif comando.startswith("truncate"):
        return ejecutar_truncate(comando)
    elif comando.startswith("scan"):
        return ejecutar_scan(comando)
    else:
        return False, "ERROR: Command not found"
    
    
def ejecutar_create(comando):
    #tomar tiempo de ejecucion
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
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
        time.sleep(random.uniform(0.1, 0.8))
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
        time.sleep(random.uniform(0.1, 0.8))
        
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
                        time.sleep(random.uniform(0.1, 0.8))
                        
                        tiempo_final = time.time()
                        return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error inserting data"
    
def ejecutar_get(comando):
    # get 'mi_tabla', 'fila1'
    #o 
    #get 'my_table', 'row1', 'cf1'
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
                if len(comando.split(",")) == 2:
                    resultado_get = hbase.get(tabla, fila)
                elif len(comando.split(",")) == 3:
                    columna = comando.split(",")[2]
                    if len(columna) == 0:
                        return False, "ERROR: SyntaxError: No column specified"
                    else:
                        resultado_get = hbase.get(tabla, fila, columna)
                elif len(comando.split(",")) > 3:
                    #tomar como columnas todas las que estan despues de la posicion 1
                    columnas = comando.split(",")[2:]
                    resultado_get = hbase.get(tabla, fila, columnas)

                errores = hbase.get_errores()
                if len(errores) > 0:
                    return False, "ERROR: " + errores[0]
                resultado = "COLUMN                     CELL\n"
                for key, value in resultado_get.items():
                    resultado += f"{key}: {value}\n"
                cont = len(resultado_get)
                time.sleep(random.uniform(0.1, 0.8))

                tiempo_final = time.time()
                return True, resultado + "\n" + str(cont) + " row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error getting data"
    
def ejecutar_delete(comando):
# hbase(main):001:0> delete 'my_table', 'row1', 'cf1:column1'
# hbase(main):003:0> delete 'my_table', 'row1', 'cf1'
# hbase(main):004:0> delete 'my_table', 'row1'
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
                if len(comando.split(",")) == 2:
                    hbase.delete(tabla, fila)
                elif len(comando.split(",")) == 3:
                    columna = comando.split(",")[2]
                    if len(columna) == 0:
                        return False, "ERROR: SyntaxError: No column specified"
                    else:
                        hbase.delete(tabla, fila, columna)
                elif len(comando.split(",")) > 3:
                    #tomar como columnas todas las que estan despues de la posicion 1
                    columnas = comando.split(",")[2:]
                    hbase.delete(tabla, fila, columnas)
                errores = hbase.get_errores()
                if len(errores) > 0:
                    return False, "ERROR: " + errores[0]
                tiempo_final = time.time()
                return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error deleting data"
    
def ejecutar_deleteall(comando):
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando.split(",")[0]
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        else:
            regex = comando.split(",")[1]
            if len(regex) == 0:
                return False, "ERROR: SyntaxError: No regex specified"
            else:
                hbase.deleteall(tabla, regex)
                errores = hbase.get_errores()
                if len(errores) > 0:
                    return False, "ERROR: " + errores[0]
                time.sleep(random.uniform(0.1, 0.8))
                
                tiempo_final = time.time()
                return True, "0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error deleting all data"
    
    # Count
"""
count 'my_table'
count 'my_table', INTERVAL => 100
count 'my_table', LIMIT => 500
"""
def ejecutar_count(comando):
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando.split(",")[0].strip()
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        else:
            resultado_count = None
            resultado_count_interval = None

            if len(comando.split(",")) == 1:
                resultado_count = hbase.count(tabla)
            elif len(comando.split(",")) == 2:
                if 'interval' in comando:
                    interval = int(comando.split(",")[1].split("=>")[-1])
                    resultado_count_interval = hbase.count(tabla, interval=interval)
                elif 'limit' in comando:
                    limit = int(comando.split(",")[1].split("=>")[-1])
                    resultado_count = hbase.count(tabla, limit=limit)

            errores = hbase.get_errores()
            if len(errores) > 0:
                return False, "ERROR: " + errores[0]

           
            tiempo_final = time.time()
            if resultado_count_interval is not None:
                # Formatear el resultado de intervalos para impresión
                interval_logs = resultado_count_interval.get('intervals', [])
                total_count = resultado_count_interval.get('total_count', 0)
                log_str = ""
                for elapsed_time, count in interval_logs:
                    log_str += f"Current count: {count}, time spent: {round(elapsed_time, 2)} seconds\n"
                log_str += f"{total_count} row(s) in {round(tiempo_final - tiempo_inicial, 2)} seconds\n⇒ {total_count}"
                return True, log_str
            else:
                return True, f"{resultado_count} row(s) in {round(tiempo_final - tiempo_inicial, 2)} seconds\n⇒ {resultado_count}"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error counting data"
    
def ejecutar_truncate(comando):
    try:
        tiempo_inicial = time.time()
        comando = limpiar_comando(comando)
        tabla = comando
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        hbase.truncate(tabla)
        errores = hbase.get_errores()
        if len(errores) > 0:
            return False, "ERROR: " + errores[0]
        #un tiempo de espera sleep random entre 0.1 y 0.8
        time.sleep(random.uniform(0.1, 0.8))
        tiempo_final = time.time()
        return True, "Disabling table...\nTruncating table... \nEnabling table...\n0 row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error truncating table"
    
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
def ejecutar_scan(comando):
    try:
        tiempo_inicial = time.time()
        
        # Buscar todo lo que está dentro de comillas dobles
         
        match = re.findall(r'\"(.*?)\"', comando)
        opciones = None
        if match:
            opciones = match
        comando = limpiar_comando(comando)
        tabla = comando.split(",")[0].strip()
        if len(tabla) == 0:
            return False, "ERROR: SyntaxError: No table specified"
        else:
            resultado_scan = None

            if len(comando.split(",")) == 1:
                resultado_scan = hbase.scan(tabla)
            elif len(comando.split(",{")) == 2 or len(comando.split(",filter")) == 2:
                if 'startrow' in comando and 'stoprow' in comando:
                    startrow = comando.split(",")[1].split("=>")[-1].strip()
                    stoprow = comando.split(",")[2].split("=>")[-1].strip().replace("}", "")
                    resultado_scan = hbase.scan(tabla, startrow=startrow, stoprow=stoprow)
                elif 'startrow' in comando:
                    startrow = comando.split(",")[1].split("=>")[-1].strip().replace("}", "")
                    resultado_scan = hbase.scan(tabla, startrow=startrow)
                elif 'columns' in comando:
                    columns = comando.split("=>")[-1].strip().replace("}", "").replace("[", "").replace("]", "").replace("'", "").split(",")
                    resultado_scan = hbase.scan(tabla, columns=columns)
                elif 'filter' in comando:
                    # Extraer el filtro de las comillas dobles
                    filtro = opciones[0]
                    resultado_scan = hbase.scan(tabla, filter=filtro)
                elif 'limit' in comando:
                    limit = int(comando.split(",")[1].split("=>")[-1].strip().replace("}", ""))
                    resultado_scan = hbase.scan(tabla, limit=limit)

            errores = hbase.get_errores()
            if len(errores) > 0:
                return False, "ERROR: " + errores[0]

            resultado = "COLUMN + CELL\n"
            cont = 0

            for item in resultado_scan:
                row_key = item["row"]
                for family, columns in item["columns"].items():
                    for column, cell in columns.items():
                        column_name = f"{family}:{column}"
                        timestamp = cell.get("timeStamp", "None")
                        value = cell.get("value", "None")
                        resultado += f"{row_key} column={column_name}, timestamp={timestamp}, value={value}\n"
                cont += 1

            time.sleep(random.uniform(0.1, 0.8))
            tiempo_final = time.time()

            return True, resultado + "\n" + str(cont) + " row(s) in " + str(round(tiempo_final - tiempo_inicial, 2)) + " seconds"
    except Exception as e:
        print(e)
        return False, "ERROR: Unexpected error scanning data"

if __name__ == "__main__":
    print("Hola mundo")