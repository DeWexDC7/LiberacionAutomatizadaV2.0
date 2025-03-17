import pandas as pd 
import logging
import psycopg2
import json
import time
import os

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
        
        # Extraer valores usando los nombres de columnas reales
        codigos_nap = df[column_mappings.get("CODIGO_NAP")].dropna().tolist() if "CODIGO_NAP" in column_mappings else []
        
        # Para cada valor individual, usar iloc[0] solo si hay datos
        if len(df) == 0:
            logging.error("El archivo Excel no contiene datos")
            return None
            
        # Obtener los valores individuales con manejo de errores
        try:
            # Extraer valores para todos los NAPs
            hub_col = column_mappings.get("HUB")
            cluster_col = column_mappings.get("CLUSTER")
            olt_col = column_mappings.get("OLT")
            frame_col = column_mappings.get("FRAME")
            slot_col = column_mappings.get("SLOT")
            puerto_col = column_mappings.get("PUERTO")
            puertos_nap_col = column_mappings.get("# PUERTOS NAP")
            latitud_col = column_mappings.get("LATITUD")
            longitud_col = column_mappings.get("LONGITUD")
            
            # Usamos el primer valor para mantener compatibilidad con el código existente
            hub = df[hub_col].iloc[0] if hub_col else "Sin dato"
            cluster = df[cluster_col].iloc[0] if cluster_col else "Sin dato"
            olt = df[olt_col].iloc[0] if olt_col else "Sin dato"
            frame = int(df[frame_col].iloc[0]) if frame_col and pd.notna(df[frame_col].iloc[0]) else 0
            slot = int(df[slot_col].iloc[0]) if slot_col and pd.notna(df[slot_col].iloc[0]) else 0
            puerto = int(df[puerto_col].iloc[0]) if puerto_col and pd.notna(df[puerto_col].iloc[0]) else 0
            puertos_nap = int(df[puertos_nap_col].iloc[0]) if puertos_nap_col and pd.notna(df[puertos_nap_col].iloc[0]) else 0
            latitud = float(df[latitud_col].iloc[0]) if latitud_col and pd.notna(df[latitud_col].iloc[0]) else 0.0
            longitud = float(df[longitud_col].iloc[0]) if longitud_col and pd.notna(df[longitud_col].iloc[0]) else 0.0
            
            # Logging para depuración
            logging.debug(f"hub: {hub}, cluster: {cluster}, olt: {olt}, frame: {frame}, slot: {slot}, puerto: {puerto}, puertos_nap: {puertos_nap}, latitud: {latitud}, longitud: {longitud}")

            return hub, cluster, olt, frame, slot, puerto, codigos_nap, puertos_nap, latitud, longitud
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
    
    hub, cluster, olt, frame, slot, puerto, codigos_nap_excel, puertos_nap, latitud, longitud = result

    conn = conexion_bd()
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT nap FROM inv_naps")
            codigos_nap_bd = [row[0] for row in cursor.fetchall()]
            
            codigos_faltantes = set(codigos_nap_excel) - set(codigos_nap_bd)
            if codigos_faltantes:
                print("Códigos NAP faltantes en la BD:")
                print(codigos_faltantes)
                
                # Crear un único archivo Excel para todos los NAPs faltantes
                registros = []
                for nap_faltante in codigos_faltantes:
                    registro = crear_registro_nap(hub, cluster, olt, frame, slot, puerto, nap_faltante, puertos_nap, latitud, longitud)
                    registros.append(registro)
                
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
    
    logging.debug(f"Para NAP {nap}: Región={region}, Zona={zona}")
    
    # Convertir coordenadas a formato numérico si son strings
    try:
        if latitud and isinstance(latitud, str):
            latitud = float(latitud.replace(',', '.'))
        if longitud and isinstance(longitud, str):
            longitud = float(longitud.replace(',', '.'))
    except (ValueError, TypeError):
        logging.warning(f"Error al convertir coordenadas a formato numérico para NAP {nap}")
    
    # Formatear coordenadas con más decimales
    if latitud and longitud and latitud != 0 and longitud != 0:
        coordenadas = f"({longitud:.6f}, {latitud:.6f})"
        # Formatear latitud y longitud para el DataFrame con comas como separador decimal
        lat_str = f"{latitud:.6f}".replace('.', ',')
        long_str = f"{longitud:.6f}".replace('.', ',')
    else:
        coordenadas = "(Sin coordenadas)"
        lat_str = ""
        long_str = ""
    
    # Obtener la fecha actual en formato DD/MM/YYYY
    fecha_liberacion = fecha_hoy()
    
    # Crear un diccionario con los datos de este NAP
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
    return time.strftime("%d/%m/%Y")

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