import pandas as pd
import dtale
import time
from DataCleaner import DataCleaner
from DataCLFReader import DataCLFReader
from DataTransformer import DataTransformer
from dotenv import load_dotenv
from AIlibrary import DataCleaner, DataCLFReader, DataTransformer
from AIlibrary import IPINFO_TOKEN, CACHE_FILE,SESSION_MIN, IsolationForestModel,KMeansModel
from sklearn.compose import ColumnTransformer
from AIlibrary import IpAddressToISOCountryCodeTransformer,DeltaTimeBetweenDatetimesTransformer
from AIlibrary import CalculateLengthTransformer,GetInfoSessionTransformer,AddOsCommandFlagTransformer,PandasCompatibleHashingVectorizer,PandasCompatibleCountVectorizer
from AIlibrary import AddHexadecimalCharactersFlagTransformer,AddWeirdCharactersFrequencyTransformer,PandasCompatibleTfidfVectorizer
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from AIlibrary import NORMALIZED_NUM,COUNTRY_CODE_PIPE,NORMALIZED_DELTATIME_BETWEEN_REQUEST_PIPE,NORMALIZED_LENGTH_PIPE,NORMALIZED_DELTATIME_BETWEEN_REQUEST_IN_SESSION_PIPE
def main2():
    
    print("Hello from psu-lib-ia!")
    load_dotenv() 

    cleaner = DataCleaner()
    reader = DataCLFReader()
    session_min =20
    transformer = DataTransformer(token=IPINFO_TOKEN,cache_file=CACHE_FILE,session_minutes=session_min)

    df = reader.logs_to_df(logfile='./data/extracto200M.log', output_dir='temp_dir/', errors_file='errors.txt')
    
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
    
    transformation = ColumnTransformer([
         ("t00", "drop",['userid']),
         ("pipe01",COUNTRY_CODE_PIPE.set_output(transform="pandas"),['client'] ),
         ("pipe02", NORMALIZED_DELTATIME_BETWEEN_REQUEST_PIPE.set_output(transform="pandas"),['datetime']),
         ("pipe03", NORMALIZED_LENGTH_PIPE.set_output(transform="pandas"),['request','referer']),
         ("pipe04", NORMALIZED_DELTATIME_BETWEEN_REQUEST_IN_SESSION_PIPE.set_output(transform="pandas"),['client','datetime']),
         ('pipe05', NORMALIZED_NUM.set_output(transform='pandas'),['size_in_bytes']),
         ("t05", AddOsCommandFlagTransformer(),['raw_request']),
         ("t06", AddHexadecimalCharactersFlagTransformer(),['raw_request']),
         ("t07", AddWeirdCharactersFrequencyTransformer(),['raw_request']),
         ("t08", OneHotEncoder(handle_unknown='ignore',sparse_output=False, drop=None),['method','status']),
         ("t09", PandasCompatibleCountVectorizer(token_pattern=r'[^/]+',max_features=100,output_transform="pandas"),['request']),  
         ("t10", PandasCompatibleCountVectorizer(token_pattern=r'[^/]+',max_features=100,output_transform="pandas"),['referer']),               #tokenizado por "/"
         ("t11", PandasCompatibleCountVectorizer(token_pattern=r'\b\w+\b',max_features=100,output_transform="pandas"),['user_agent']),                 #tokenizado por palabra
         ("t12", PandasCompatibleTfidfVectorizer(analyzer='char_wb', ngram_range=(3, 6),max_features=200,output_transform="pandas"), ['raw_request'])  #tokenizado por datagramas entre 3 y 6 char para detectar patrones de comandos    
    ], remainder='passthrough', verbose_feature_names_out=True)
    transformation.set_output(transform="pandas")
    final_df = transformation.fit_transform(df)

    # selected_model='mi_modelo_1k_isolation_forest'
    # model = IsolationForestModel()
    # model.load_model(selected_model.replace('_isolation_forest', ''))
    # predictions, scores, pca_data = model.test_model(df)
    # final_df = model.transformer.transform(df)

    print("--------------------------------------------------------------------------------")
    print ("final_df Data:")
    print (final_df.describe())
    print (final_df.shape)
    print (final_df.info())
    print (final_df.head())
    
  
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
    main2()