import pandas as pd
import numpy as np
from DataCleaner import DataCleaner
from DataCLFReader import DataCLFReader

def main():
    print("Hello from psu-lib-ia!")
    cleaner = DataCleaner()
    reader = DataCLFReader()
    df = reader.logs_to_df(logfile='./data/logs_50Mb.log', output_dir='temp_dir/', errors_file='errors.txt')
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




if __name__ == "__main__":
    main()
