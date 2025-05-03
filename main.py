import pandas as pd
import numpy as np
import os
from DataCleaner import DataCleaner
from DataCLFReader import DataCLFReader
from DataIPTransformer import DataIPTransformer
from dotenv import load_dotenv
from settings import IPINFO_TOKEN, CACHE_FILE


def main():
    load_dotenv() 
    print("Hello from psu-lib-ia!")
    
    cleaner = DataCleaner()
    reader = DataCLFReader()
    ip_transformer = DataIPTransformer(token=IPINFO_TOKEN,cache_file=CACHE_FILE)

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

    cleaned_and_transformed_df = ip_transformer.transform(cleaned_df,"client","country_code" )
    ip_transformer.save_cache()
    print (cleaned_and_transformed_df.head())


if __name__ == "__main__":
    main()
