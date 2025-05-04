from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
import os
import seaborn as sns
from DataCleaner import DataCleaner
from DataCLFReader import DataCLFReader
from DataTransformer import DataTransformer
from dotenv import load_dotenv
from settings import IPINFO_TOKEN, CACHE_FILE


def main():
    load_dotenv() 
    print("Hello from psu-lib-ia!")
    
    cleaner = DataCleaner()
    reader = DataCLFReader()
    transformer = DataTransformer(token=IPINFO_TOKEN,cache_file=CACHE_FILE,session_minutes=20)

    df = reader.logs_to_df(logfile='./data/logs_1k.log', output_dir='temp_dir/', errors_file='errors.txt')
    print (df.shape)
    print (df)
    #lectura desde directorio de ficheros parquet: 
    #logs_df = pd.read_parquet('df_dir/')
    print ("Datos faltantes en DF")
    print (cleaner.info_missing_values(df))
    print (df.shape)
    cleaned_df = cleaner.delete_rows_with_faulting_category(df)
    
    print ("Datos faltantes en DF despues de limpieza")
    print (cleaner.info_missing_values(cleaned_df))
    print (cleaned_df.shape)

    cleaned_and_transformed_df = transformer.transform_add_countrycode(cleaned_df,"client","country_code" )
    transformer.save_cache()

    print (cleaned_and_transformed_df.head())
    cleaned_and_transformed_df = transformer.transform_add_datetime_delta_between_requests(cleaned_and_transformed_df)
    print (cleaned_and_transformed_df.head())
    cleaned_and_transformed_df = transformer.transform_add_session_info (cleaned_and_transformed_df)
    print (cleaned_and_transformed_df.head())

    print (cleaned_and_transformed_df.describe())
    print (cleaned_and_transformed_df.info())
    print (cleaned_and_transformed_df[cleaned_and_transformed_df['client'] == '157.55.39.245'])
   

    requests_per_session = cleaned_and_transformed_df.groupby('session_global_id').size().reset_index(name='num_requests')

    #---------------------------------------------------------------------------------------------
    #Gráfica:
    # Visualización de las sesiones más activas
    requests_per_session_sorted = requests_per_session.sort_values(by='num_requests', ascending=False).head(1000)

    print(requests_per_session_sorted.shape)
    plt.figure(figsize=(12, 6))
    sns.barplot(x='session_global_id', y='num_requests', data=requests_per_session_sorted)
    plt.xticks(rotation=90)
    plt.title('Top 20 sesiones por número de peticiones')
    plt.xlabel('ID de Sesión')
    plt.ylabel('Número de Peticiones')
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
