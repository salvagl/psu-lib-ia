from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os
import seaborn as sns
from DataCleaner import DataCleaner
from DataCLFReader import DataCLFReader
from DataTransformer import DataTransformer
from dotenv import load_dotenv
import dtale
import time
from settings import IPINFO_TOKEN, CACHE_FILE


def main():
    load_dotenv() 
    print("Hello from psu-lib-ia!")
    
    cleaner = DataCleaner()
    reader = DataCLFReader()
    session_min =20
    transformer = DataTransformer(token=IPINFO_TOKEN,cache_file=CACHE_FILE,session_minutes=session_min)

    df = reader.logs_to_df(logfile='./data/logs_10k.log', output_dir='temp_dir/', errors_file='errors.txt')
    print (df.shape)
    print (df)
    #lectura desde directorio de ficheros parquet: 
    #logs_df = pd.read_parquet('df_dir/')
 

    print("--------------------------------------------------------------------------------")
    print ("Transform Data:")
    #Transformación de datos:
    # - añadir country_code: código ISO de pais en función de la IP
    # - añadir datetime_delta_ms: tiempo entre requests
    # - añadir session_global_id: identificador de sesiones en un rango de tiempo. (peticiones desde una misma IP en un rango definido)
    # - añadir datetime_delta_ms_in_session: tiempo entre request de una misma sesión.
    # - eliminar userid: 
    transformed_df = transformer.transform_add_countrycode(df,"client","country_code" )
    
    transformed_df = transformer.transform_add_datetime_delta_between_requests(transformed_df)
    
    transformed_df = transformer.transform_add_session_info (transformed_df)

    transformed_df['log_datetime_delta_ms'] = np.log1p(transformed_df['datetime_delta_ms'])
    transformed_df['log_datetime_delta_ms_in_session'] = np.log1p(transformed_df['datetime_delta_ms_in_session'])
    
    #Limpieza de datos: 
    print("--------------------------------------------------------------------------------")
    print ("Clean Data:")
    
    cleaned_df = cleaner.delete_rows_with_faulting_category(transformed_df)
    
    cleaned_df = cleaner.delete_column (cleaned_df,'userid')

    cleaned_df = cleaner.delete_column (cleaned_df,'client')

    print (cleaner.info_missing_values(cleaned_df))
    print (cleaned_df.shape)

    #print("--------------------------------------------------------------------------------")
    #print ("Normalize Data:")
    #Normalizar valores numéricos:
    #normalized_df = transformer.transform_normalize (cleaned_df,['datetime_delta_ms','datetime_delta_ms_in_session','size_in_bytes'])

    #OneHotEncoder sobre categoricas de baja cardinalidad
    #normalized_df = transformer.transform_one_hot_encoder(normalized_df,['method'])
    
    # print (normalized_df.head())
    # print (normalized_df.describe())
    # print (normalized_df.info())
    
    #print (f"Se muestra actividad de la sesión que más peticiones ha realizado: ")
    #print (cleaned_and_transformed_df[cleaned_and_transformed_df['client'] == '66.249.66.194'])
   
    #Se obtiene numero de peticiones por "session_global_id"
    # requests_per_session = normalized_df.groupby('session_global_id').size().reset_index(name='num_requests')

    #---------------------------------------------------------------------------------------------
    #Gráfica:
    # Visualización de las sesiones más activas (es decir, con más peticiones)
    # requests_per_session_sorted = requests_per_session.sort_values(by='num_requests', ascending=False).head(50)

    # print(requests_per_session_sorted.shape)
    # plt.figure(figsize=(12, 6))
    # sns.barplot(x='session_global_id', y='num_requests', data=requests_per_session_sorted)
    # plt.xticks(rotation=90)
    # plt.title('Top 20 sesiones por número de peticiones')
    # plt.xlabel('ID de Sesión')
    # plt.ylabel('Número de Peticiones')
    # plt.tight_layout()
    # plt.show()


    # D-Tale
    server = dtale.show(cleaned_df)
    try:
        server.open_browser()
    except Exception:
        pass  

    url = server.main_url()
    print(f"D-Tale está corriendo en: {url}")
    print("Pulsa Ctrl+C para detenerlo cuando hayas terminado.")

    # Mantener el servidor en marcha hasta Ctrl+C
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Deteniendo D-Tale…")
        server.kill()

if __name__ == "__main__":
    main()
