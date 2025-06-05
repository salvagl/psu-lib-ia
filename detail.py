import pandas as pd
import dtale
import time
from DataCleaner import DataCleaner
from DataCLFReader import DataCLFReader
from DataTransformer import DataTransformer
from dotenv import load_dotenv
from settings import IPINFO_TOKEN, CACHE_FILE

def main():
    
    print("Hello from psu-lib-ia!")
    load_dotenv() 

    cleaner = DataCleaner()
    reader = DataCLFReader()
    session_min =20
    transformer = DataTransformer(token=IPINFO_TOKEN,cache_file=CACHE_FILE,session_minutes=session_min)

    df = reader.logs_to_df(logfile='./data/logs_50M.log', output_dir='temp_dir/', errors_file='errors.txt')
    
    print("--------------------------------------------------------------------------------")
    print ("Transform Data:")
    #Transformación de datos:
    # - añadir country_code: código ISO de pais en función de la IP
    # - añadir datetime_delta_ms: tiempo entre requests
    # - añadir session_global_id: identificador de sesiones en un rango de tiempo. (peticiones desde una misma IP en un rango definido)
    # - añadir datetime_delta_ms_in_session: tiempo entre request de una misma sesión.
    # - añadir request_len y referer_len : longitud de la petición y el referer de la petición.
    # - añadir flag que indica si la request contiene comandos típicos de SO que pueden indicar ataque.
    # - añadir flag que indica si la request contiene caracteres Hexadecimales
    # - añadir columna con conteo de caracteres extraños para una URL
    
    transformed_df = transformer.transform_add_countrycode(df,"client","country_code" )
    
    transformed_df = transformer.transform_add_datetime_delta_between_requests(transformed_df)
    
    transformed_df = transformer.transform_add_session_info (transformed_df)

    transformed_df = transformer.transform_add_length_columns (transformed_df,['request','referer'])
    
    transformed_df = transformer.transform_add_os_command_flag(transformed_df,['raw_request'])

    transformed_df = transformer.transform_add_hex_flag(transformed_df,['raw_request'])

    transformed_df = transformer.transform_add_weird_char_freq(transformed_df,['raw_request'])

    print (transformed_df.info())

    #Limpieza de datos: 
    # - eliminar filas con datos faltantes: en el proceso de lectura ya se realiza.
    # - eliminar userid
    # - eliminar client (ip) 

    print("--------------------------------------------------------------------------------")
    print ("Clean Data:")
    
    cleaned_df = cleaner.delete_rows_with_faulting_category(transformed_df)
    
    cleaned_df = cleaner.delete_column (cleaned_df,'userid')

    cleaned_df = cleaner.delete_column (cleaned_df,'client')

    print (cleaner.info_missing_values(cleaned_df))
    print (cleaned_df.shape)

    print("--------------------------------------------------------------------------------")
    print ("Normalize Data:")
    #Normalizar valores numéricos:
    normalized_df = transformer.transform_normalize (cleaned_df,['datetime_delta_ms','datetime_delta_ms_in_session','size_in_bytes','raw_request_weird_char_freq','raw_request_has_hex','raw_request_has_os_command'])

    #OneHotEncoder sobre categoricas de baja cardinalidad
    normalized_df = transformer.transform_one_hot_encoder(normalized_df,['method','status'])
    normalized_df = transformer.transform_feature_hashing(normalized_df,'country_code')
    sparseMatrix_user_agent, v1 = transformer.transform_vectorize_categorical_text(normalized_df,'user_agent')
    print("informacion de user_agent: ")
    print(sparseMatrix_user_agent.shape)
    sparseMatrix_request, v2 = transformer.transform_vectorize_url(normalized_df,'request')
    print("informacion de request: ")
    print(sparseMatrix_request.shape)
    sparseMatrix_referer, v3 = transformer.transform_vectorize_url(normalized_df,'referer')
    print("informacion de referer: ")
    print(sparseMatrix_referer.shape)
    sparseMatrix_rawRequest, v4 = transformer.transform_vectorize_raw_request(normalized_df,'raw_request')
    print("informacion de raw_request:")
    print(sparseMatrix_rawRequest.shape)

    final_df = transformer.transform_combine_numeric_and_sparse(normalized_df,[sparseMatrix_rawRequest,sparseMatrix_user_agent,sparseMatrix_referer,sparseMatrix_request])
    
    print("DF final shape: " )
    print(final_df.shape)

    print (final_df.head())
    #print (final_df.describe())
    print (final_df.info())
    
    #print (f"Se muestra actividad de la sesión que más peticiones ha realizado: ")
    #print (cleaned_and_transformed_df[cleaned_and_transformed_df['client'] == '66.249.66.194'])
   
    #Se obtiene numero de peticiones por "session_global_id"
    requests_per_session = normalized_df.groupby('session_global_id').size().reset_index(name='num_requests')

    # D-Tale
    server = dtale.show(final_df)
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