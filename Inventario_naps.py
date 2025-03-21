import pandas as pd 
import logging
import psycopg2
import json
import time
import os
from functools import lru_cache

#configuracion logging
logging.basicConfig(level=logging.DEBUG)

def conexion_bd():
    try:
        with open("configuracion/conexion.json", "r") as archivo:
            credenciales = json.load(archivo)
        
        conn = psycopg2.connect(
            dbname=credenciales["PostgresSQL"]["database"],
            user=credenciales["PostgresSQL"]["user"],
            password=credenciales["PostgresSQL"]["password"],
            host=credenciales["PostgresSQL"]["host"],
            port=credenciales["PostgresSQL"]["port"]
        )
        
        if conn is not None:
            logging.info("Conexión exitosa a la base de datos")
            return conn
    except Exception as e:
        logging.error(f"Error al conectar a la base de datos: {e}")
        return None

def get_column_case_insensitive(df, column_name):
    """Obtiene una columna del DataFrame independientemente de mayúsculas/minúsculas"""
    for col in df.columns:
        if col.upper() == column_name.upper():
            return col
    return None

def lectura_naps():
    try:
        df = pd.read_excel("Data/data.xlsx", sheet_name="Naps")

        # Mostrar las columnas disponibles para depuración
        logging.info(f"Columnas disponibles en el Excel: {list(df.columns)}")
        
        # Mapeo de nombres de columnas esperadas a nombres reales
        column_mappings = {}
        expected_columns = ["CODIGO_NAP", "HUB", "CLUSTER", "OLT", "FRAME", "SLOT", 
                            "PUERTO", "# PUERTOS NAP", "LATITUD", "LONGITUD"]
        
        for col in expected_columns:
            actual_col = get_column_case_insensitive(df, col)
            if actual_col:
                column_mappings[col] = actual_col
            else:
                logging.warning(f"Columna '{col}' no encontrada en el Excel. Verificar el nombre exacto.")
        
        if len(df) == 0:
            logging.error("El archivo Excel no contiene datos")
            return None
            
        try:
            # Extraer columnas
            codigo_nap_col = column_mappings.get("CODIGO_NAP")
            hub_col = column_mappings.get("HUB")
            cluster_col = column_mappings.get("CLUSTER")
            olt_col = column_mappings.get("OLT")
            frame_col = column_mappings.get("FRAME")
            slot_col = column_mappings.get("SLOT")
            puerto_col = column_mappings.get("PUERTO")
            puertos_nap_col = column_mappings.get("# PUERTOS NAP")
            latitud_col = column_mappings.get("LATITUD")
            longitud_col = column_mappings.get("LONGITUD")
            
            # Verificar que existe la columna de código NAP
            if not codigo_nap_col:
                logging.error("No se encontró la columna de códigos NAP")
                return None
                
            # Extraer todos los NAPs con sus datos correspondientes
            nap_data = []
            for idx, row in df.iterrows():
                if pd.notna(row[codigo_nap_col]):
                    nap_code = row[codigo_nap_col]
                    
                    # Extraer datos generales (usar el valor de la fila actual o valor predeterminado)
                    hub = row[hub_col] if hub_col and pd.notna(row[hub_col]) else "Sin dato"
                    cluster = row[cluster_col] if cluster_col and pd.notna(row[cluster_col]) else "Sin dato"
                    olt = row[olt_col] if olt_col and pd.notna(row[olt_col]) else "Sin dato"
                    frame = int(row[frame_col]) if frame_col and pd.notna(row[frame_col]) else 0
                    slot = int(row[slot_col]) if slot_col and pd.notna(row[slot_col]) else 0
                    puerto = int(row[puerto_col]) if puerto_col and pd.notna(row[puerto_col]) else 0
                    puertos_nap = int(row[puertos_nap_col]) if puertos_nap_col and pd.notna(row[puertos_nap_col]) else 0
                    
                    # Extraer coordenadas de la fila actual
                    latitud = float(row[latitud_col]) if latitud_col and pd.notna(row[latitud_col]) else 0.0
                    longitud = float(row[longitud_col]) if longitud_col and pd.notna(row[longitud_col]) else 0.0
                    
                    # Guardar todos los datos relacionados con este NAP
                    nap_data.append({
                        "codigo_nap": nap_code,
                        "hub": hub,
                        "cluster": cluster,
                        "olt": olt,
                        "frame": frame,
                        "slot": slot,
                        "puerto": puerto,
                        "puertos_nap": puertos_nap,
                        "latitud": latitud,
                        "longitud": longitud
                    })
            
            if not nap_data:
                logging.error("No se encontraron códigos NAP válidos en el Excel")
                return None
                
            # Extraer códigos NAP para mantener compatibilidad
            codigos_nap = [item["codigo_nap"] for item in nap_data]
            
            # Para mantener compatibilidad con el código existente, usamos los valores del primer NAP
            # pero guardamos todos los datos para acceder a coordenadas individuales más adelante
            first_nap = nap_data[0]
            hub = first_nap["hub"]
            cluster = first_nap["cluster"]
            olt = first_nap["olt"]
            frame = first_nap["frame"]
            slot = first_nap["slot"]
            puerto = first_nap["puerto"]
            puertos_nap = first_nap["puertos_nap"]
            latitud = first_nap["latitud"]
            longitud = first_nap["longitud"]
            
            # Logging para depuración
            logging.debug(f"Se encontraron {len(nap_data)} NAPs en el Excel")
            logging.debug(f"Primer NAP - hub: {hub}, cluster: {cluster}, olt: {olt}, frame: {frame}, slot: {slot}, puerto: {puerto}")
            
            # Agregar nap_data como elemento adicional al retorno para usar en busqueda_naps_bd
            return hub, cluster, olt, frame, slot, puerto, codigos_nap, puertos_nap, latitud, longitud, nap_data
        except Exception as e:
            logging.error(f"Error al extraer datos del Excel: {e}")
            return None
            
    except FileNotFoundError as e:
        logging.error(f"Error al leer el archivo Excel: {e}")
        return None
    except Exception as e:
        logging.error(f"Error inesperado al procesar el Excel: {e}")
        return None

def get_region_zone_from_excel():
    """Obtiene la región y zona desde el archivo Excel en la hoja 'Liberacion'"""
    try:
        df = pd.read_excel("Data/data.xlsx", sheet_name="Liberacion")
        
        # Mostrar las columnas disponibles para depuración
        logging.info(f"Columnas disponibles en la hoja Liberacion: {list(df.columns)}")
        
        # Buscar las columnas REGIÓN y ZONA (insensible a mayúsculas/minúsculas)
        region_col = None
        zona_col = None
        
        for col in df.columns:
            if "REGIÓN" == col.upper():
                region_col = col
            elif "ZONA" == col.upper():
                zona_col = col
        
        # Obtener región
        region = "Sin dato"
        if region_col and not df[region_col].empty:
            region = next((str(value) for value in df[region_col] if pd.notna(value)), "Sin dato")
            
        # Obtener zona
        zona = "Sin dato"
        if zona_col and not df[zona_col].empty:
            zona = next((str(value) for value in df[zona_col] if pd.notna(value)), "Sin dato")
            
        logging.info(f"Región y zona obtenidas del Excel: {region}, {zona}")
        return region, zona
    except Exception as e:
        logging.error(f"Error al leer región y zona del Excel: {e}")
        return "Sin dato", "Sin dato"

# Caché para evitar consultas repetidas a la base de datos
@lru_cache(maxsize=128)
def get_region_zone_from_db(cluster):
    """Obtiene la región y zona desde la base de datos según el cluster"""
    # Primero intentamos obtener los datos del Excel
    region, zona = get_region_zone_from_excel()
    
    # Si no hay datos en el Excel, entonces consultamos la BD
    if region == "Sin dato" or zona == "Sin dato":
        conn = conexion_bd()
        
        if conn is not None:
            try:
                cursor = conn.cursor()
                # Consultar región y zona basado en el cluster
                cursor.execute("SELECT region, zona FROM inv_naps WHERE cluster = %s LIMIT 1", (cluster,))
                result = cursor.fetchone()
                if result:
                    region = result[0] if result[0] else "Sin dato"
                    zona = result[1] if result[1] else "Sin dato"
                else:
                    # Buscar patrones R1 o R2 en el código del cluster
                    if "R1" in str(cluster).upper():
                        region = "R1"
                    elif "R2" in str(cluster).upper():
                        region = "R2"
                    
                cursor.close()
                conn.close()
            except Exception as e:
                logging.error(f"Error al obtener región y zona de la BD: {e}")
    
    logging.info(f"Región y zona finales para cluster {cluster}: {region}, {zona}")
    return region, zona

def busqueda_naps_bd():
    result = lectura_naps()
    if result is None:
        return
    
    # Desempaquetar el resultado con el nuevo elemento nap_data
    hub, cluster, olt, frame, slot, puerto, codigos_nap_excel, puertos_nap, latitud, longitud, nap_data = result

    conn = conexion_bd()
    if conn is not None:
        try:
            cursor = conn.cursor()
            # Optimizar consulta para obtener solo los NAPs que necesitamos verificar
            placeholders = ','.join(['%s'] * len(codigos_nap_excel))
            query = f"SELECT nap FROM inv_naps WHERE nap IN ({placeholders})"
            cursor.execute(query, codigos_nap_excel)
            codigos_nap_bd = {row[0] for row in cursor.fetchall()}
            
            codigos_faltantes = set(codigos_nap_excel) - codigos_nap_bd
            if codigos_faltantes:
                print("Códigos NAP faltantes en la BD:")
                print(codigos_faltantes)
                
                # Procesar en lotes para mejor rendimiento
                registros = []
                batch_size = 100
                current_batch = []
                
                # Iterar sobre los datos completos de cada NAP
                for nap_info in nap_data:
                    if nap_info["codigo_nap"] in codigos_faltantes:
                        registro = crear_registro_nap(
                            nap_info["hub"], 
                            nap_info["cluster"], 
                            nap_info["olt"], 
                            nap_info["frame"], 
                            nap_info["slot"], 
                            nap_info["puerto"], 
                            nap_info["codigo_nap"], 
                            nap_info["puertos_nap"], 
                            nap_info["latitud"], 
                            nap_info["longitud"]
                        )
                        current_batch.append(registro)
                        
                        # Procesar lotes para evitar sobrecarga de memoria
                        if len(current_batch) >= batch_size:
                            registros.extend(current_batch)
                            current_batch = []
                
                # Añadir el último lote si existe
                if current_batch:
                    registros.extend(current_batch)
                
                # Exportar todos los registros a un único archivo Excel
                exportar_registros_naps(registros)
                
                return codigos_faltantes
            else:
                print("Todos los códigos NAP del Excel están presentes en la BD.")
        except psycopg2.Error as e:
            logging.error(f"Error al ejecutar la consulta: {e}")
        finally:
            cursor.close()
            conn.close()

def crear_registro_nap(hub, cluster, olt, frame, slot, puerto, nap, puertos_nap, latitud, longitud):
    # Obtener región y zona de la BD si están vacías o son "Sin dato"
    region, zona = get_region_zone_from_db(cluster)
    
    # Procesar coordenadas de manera más eficiente
    coordenadas = "(Sin coordenadas)"
    lat_str = ""
    long_str = ""
    
    # Convertir coordenadas en una sola operación
    try:
        lat_float = float(latitud.replace(',', '.')) if isinstance(latitud, str) else float(latitud) if latitud else 0
        long_float = float(longitud.replace(',', '.')) if isinstance(longitud, str) else float(longitud) if longitud else 0
        
        if lat_float != 0 and long_float != 0:
            coordenadas = f"({long_float:.6f}, {lat_float:.6f})"
            lat_str = f"{lat_float:.6f}".replace('.', ',')
            long_str = f"{long_float:.6f}".replace('.', ',')
    except (ValueError, TypeError):
        logging.warning(f"Error al convertir coordenadas a formato numérico para NAP {nap}")
    
    # Crear un diccionario con los datos de este NAP (usar fecha_hoy() una sola vez)
    fecha_liberacion = fecha_hoy()
    
    return {
        "hub": hub,
        "cluster": cluster,
        "olt": olt,
        "frame": frame,
        "slot": slot,
        "puerto": puerto,
        "nap": nap,
        "puertos_nap": puertos_nap,
        "coordenadas": coordenadas,
        "fecha_de_liberacion": fecha_liberacion,
        "region": region,
        "zona": zona,
        "latitud": lat_str,
        "longitud": long_str
    }

def exportar_registros_naps(registros):
    # Crear el directorio si no existe
    os.makedirs("Registros_Naps", exist_ok=True)
    
    # Nombre del archivo único
    filename = f"Registros_Naps/Inventario_Naps_{fecha_hoy().replace('/', '-')}.xlsx"
    
    # Crear DataFrame con todos los registros
    df = pd.DataFrame(registros)
    
    # Guardar todos los registros en un único archivo Excel
    df.to_excel(filename, index=False)
    
    logging.info(f"Archivo '{filename}' con {len(registros)} NAPs generado correctamente")
    print(f"Se ha creado el archivo '{filename}' con {len(registros)} registros de NAP")

# Función exportacion_data obsoleta, se mantiene para compatibilidad
def exportacion_data(hub, cluster, olt, frame, slot, puerto, nap, puertos_nap, latitud, longitud):
    registro = crear_registro_nap(hub, cluster, olt, frame, slot, puerto, nap, puertos_nap, latitud, longitud)
    exportar_registros_naps([registro])

def fecha_hoy():
    #generar fecha con este formato aaaa-mm-dd
    return time.strftime("%Y-%m-%d")

def presentacion_resultados():
    print("-----------------------------------------------------")
    print("      SISTEMA DE VALIDACIÓN Y REGISTRO DE NAPs       ")
    print("-----------------------------------------------------")
    print("Iniciando validación de NAPs en base de datos...")
    busqueda_naps_bd()
    print("-----------------------------------------------------")
    print("Proceso completado.")

def main():
    presentacion_resultados()

if __name__ == "__main__":
    main()