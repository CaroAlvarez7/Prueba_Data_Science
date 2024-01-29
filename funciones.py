
from sqlalchemy import create_engine
import pandas as pd


def conexion_bd(tabla):
    """Esta función se conecta a una BD SQL Server y ejecuta el query para extraer la información
         Entradas = tabla: corresponde a la tabla que se quiere consultar          
         Salidas  = Retorna un dataframe en caso de conexión exitosa, en otro caso retorna None"""

    server = 'prueba.formap.co'
    database = 'PruebaBI'
    username = 'Ises_CDatos'
    password = '1s3sDat0s2024*+*'
    driver = 'SQL Server'

    # Cadena de conexión SQLAlchemy
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'

    # Crear un objeto de conexión SQLAlchemy
    engine = create_engine(connection_string)

    # Consulta SQL
    query = f"""select * from {tabla}"""

    # Almacenar en un DataFrame los datos a Leer en la tabla de la BD
    df = pd.read_sql(query, engine)
    
    print(f'Tamaño del dataframe {tabla}: {df.shape}')
            
    return df



def clean_data(df):
    """Estandarizar los nombres de las columnas del DataFrame
        eliminar nans, y espacios de las columnas tipo texto
         Reemplazar espacios en blancos por 0 en las columnas númericas
           Entradas:
                df = dataframe  
            Salidas:
                Dataframe depurado"""
        
    #Eliminar espacios en los nombres de las columnas y conertir todas en mayúsculas
    df.columns = [x.strip().replace(" ", "_").upper() for x in df.columns] 
   
    #almacenar en una variable las columnas de tipo texto 
    df_obj = df.select_dtypes(include="object")

    #Almacenar en una variable las columnas numéricas
    num_columns = df.select_dtypes(include="number").columns

    #aplicar la función lambda se aplica a todas las columnas tipo texto, y elimina los espacios en blanco
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    
    #eliminar los na de las columnas tipo texto
    df[df_obj.columns] = df[df_obj.columns].fillna("")

    #llenar los espacios en blanco con 0 para para las columnas numericas
    df[num_columns] = df[num_columns].fillna(0)

    #Eliminar duplicados del df y se apliquen los cambios
    df.drop_duplicates(inplace=True, ignore_index=True)

    return df



def strip_accents(s):
   """Función para eliminar acentos 
         entrada: cadena de string a procesar
         salida: cadena de string sin acentos"""
   import unicodedata
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')


    

def boxplot_numeric_variables_plotly(df,column):
    """Genera un gráfico de caja (boxplot) para las variables numéricas en el DataFrame.
    Entrada
        df (pd.DataFrame): El DataFrame de entrada.
    Salida:
        None"""
    
    # Filtrar las columnas numéricas sin tener en cuenta, nis, ni, nif
    columnas_numericas = df.select_dtypes(include=['int', 'float']).columns.tolist()

    columnas_numericas.remove(column)

    if columnas_numericas.empty:
        print("No se encontraron variables numéricas en el DataFrame.")
        return
        
    return columnas_numericas.describe().round(2).T


