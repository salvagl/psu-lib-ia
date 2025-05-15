import pandas as pd
import numpy as np
import requests
import time
import json
import os
from tqdm.auto import tqdm
from sklearn.preprocessing import MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

class DataTransformer:
    """
    Clase para enriquecer un DataFrame de pandas con:
     1. transform_add_countrycode: añade el código ISO del país de origen de las IPs usando la API Lite de IPinfo y un sistema de caché. 
     2. transform_add_datetime_delta_between_requests: añade columna con el tiempo transcurrido desde la anterior petición a la actual en milisengundos
                                                       (la primera petición contiene NaN en esta columna)

    Ejemplo de uso:
        transformer = DataTransformer(token="TU_TOKEN", cache_file="cache.json")
        df = pd.DataFrame({'ip': ['8.8.8.8', '1.1.1.1']})
        df_transformed = transformer.transform_add_countrycode(df, ip_col='ip')
        transformer.save_cache()
    """

    def __init__(self, token: str, cache_file: str = None, delay: float = 0.005, session_minutes = 30):
        """
        Inicializa el transformer.

        :param token: Token de API de IPinfo
        :param cache_file: Ruta al fichero JSON para persistir cache (opcional)
        :param delay: Segundos a esperar entre peticiones 
        :param session_minutes: tiempo en el que se tomaran las peticiones como si perteneciesen a la misma sesión de usuario.
        """
        self.token = token
        self.cache_file = cache_file
        self.delay = delay
        self.session_minutes = session_minutes
        self.ip_cache = {}
        if cache_file:
            self._load_cache()
        else:
            print(f"No se ha especificado guardar cache en disco")

    def _load_cache(self):
        """
        Carga la caché desde disco si el fichero existe.
        """
        if os.path.isfile(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.ip_cache = json.load(f)
            except Exception:
                # Si hay error leyendo, iniciamos caché vacía
                self.ip_cache = {}
        else:
            print(f"No se localiza fichero de cache con nombre: {self.cache_file}")

    def save_cache(self):
        """
        Guarda la caché en formato JSON en disco si se proporcionó cache_file.
        """
        if not self.cache_file:
            return
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.ip_cache, f, ensure_ascii=False, indent=2)
        except Exception:
            print(f"Error al guardar fichero de caché: {Exception.with_traceback}")
            pass

    def get_country_from_ip(self, ip: str) -> str:
        """
        Devuelve el código ISO 3166-1 alfa-2 del país de la IP.
        Usa la API de IPinfo y un sistema de caché en memoria.

        :param ip: Dirección IP en formato string
        :return: Código ISO de dos letras o 'Unknown'/'Error'
        """
        # Devuelve del caché si ya existe
        if ip in self.ip_cache:
            return self.ip_cache[ip]

        #url = f"https://ipinfo.io/{ip}/json"
        url = f"https://api.ipinfo.io/lite/{ip}"
        headers = {'Authorization': f'Bearer {self.token}'}
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            data = resp.json()
            country_code = data.get('country_code', 'Unknown')
            # Guardamos en caché y esperamos un poco
            self.ip_cache[ip] = country_code
            time.sleep(self.delay)
            return country_code
        except Exception:
            # En caso de error guardamos y devolvemos 'Error'
            self.ip_cache[ip] = 'Error'
            return 'Error'

    def transform_add_countrycode(self, df: pd.DataFrame, ip_col: str = 'ip', new_col: str = 'country_code') -> pd.DataFrame:
        """
        Aplica la geolocalización a todas las IPs en el DataFrame.

        :param df: DataFrame de pandas con la columna de IPs
        :param ip_col: Nombre de la columna que contiene las IPs
        :param new_col: Nombre de la columna a añadir con el país
        :return: DataFrame enriquecido (modifica copia)
        """
        print ("- Adding col.: country_code from IP (external service: IpInfo-Lite)")
        df_copy = df.copy()
        tqdm.pandas(desc="Geolocalizando IPs")  # Configura tqdm para pandas
        df_copy[new_col] = df_copy[ip_col].progress_apply(self.get_country_from_ip)

        self.save_cache()
        return df_copy
   
    def transform_add_datetime_delta_between_requests(self, df: pd.DataFrame, new_col: str = 'datetime_delta_ms') -> pd.DataFrame:
       """
        Aplica función diff a la columna [datetime] para calcular el tiempo transcurrido en milisengundos
        desde la anterior petición. Añade columna con tiempo (en ms) transcurridos desde anterior request. 
        
        :param df: DataFrame de pandas con la columna de IPs
        :return: DataFrame enriquecido (modifica copia)
       """
       print ("transform_add_datetime_delta_between_requests: añadiendo col. datetime_delta_ms (tiempo en ms. transcurrido entre peticioens consecutivas")
       df_copy = df.copy()
       df_copy['datetime'] = pd.to_datetime(df_copy['datetime'], format='%d/%b/%Y:%H:%M:%S %z')
       # diferencia como objetos Timedelta de Pandas y paso a milisegundos para tener un dato númerico
       df_copy[new_col] = df_copy['datetime'].diff().dt.total_seconds() * 1000
       return df_copy

    def transform_add_session_info(self,df:pd.DataFrame)-> pd.DataFrame:
       """
        Aplica agrupación por iP en el rango de tiempo definido en "self.session_minutes" para obtener el conjunto de requests 
        que serán interpretados como una sesión de usuario (por defecto self.session_minutes = 30) Se añade una columna 
        "session_global_id" con identificador único (autogenerado) y "datetime_delta_ms_in_session" para informar de la diferencia de tiempo
        entre peticiones de la misma sesión. 
        
        :param df: DataFrame de pandas con la columna de IPs
        :return: DataFrame enriquecido (modifica copia)
       """
       print (f"- Adding col. session_global_id and datetime_delta_ms_in_session (id de sesión: request from same IP in range {self.session_minutes} min.")
       df_copy = df.copy()
       df_copy = df_copy.sort_values(by=['client', 'datetime'])
       df_copy['datetime'] = pd.to_datetime(df_copy['datetime'])
    
       #Se obtiene el listado de sesiones basado en IP en rangos de tiempo de 'session_minutes' min. (Una misma IP puede 
       #tener distintas sesiones si ha tenido actividad en rangos de tiempo superiores a 'session_minutes')
       df_copy['session_id'] = (df_copy.groupby('client')['datetime']
                          .diff().fillna(pd.Timedelta(seconds=0))
                          .gt(pd.Timedelta(minutes=self.session_minutes))
                          .cumsum())
    
       df_copy['session_global_id'] = df_copy['client'] + '_' + df_copy['session_id'].astype(str)
    
       df_copy['datetime_delta_ms_in_session'] = (df_copy.groupby('session_global_id')['datetime']
                                            .diff().dt.total_seconds().fillna(0) * 1000)
       
       #Se elimina la columna temporal 'session_id':
       df_copy.drop(columns=['session_id'],inplace=True)
    
       return df_copy
    
    def transform_normalize(self,df:pd.DataFrame, columns_to_normalize: list[str])-> pd.DataFrame:
        """
        Aplica Normalización/Escalado a las columnas numericas de un DataFrame
        
        :param df: DataFrame de pandas con la columna de IPs
        :return: DataFrame Normalizado (modifica copia)
        """
        print (f"- Normalizing numeric columns...")

        # Crear el transformador de columnas
        preprocesador = ColumnTransformer(
            transformers=[
               ('num', MinMaxScaler(), columns_to_normalize)
            ],
            remainder='passthrough'  # Deja el resto de columnas sin transformar
        )

        # Aplicar la transformación
        df_normalized = pd.DataFrame(
            preprocesador.fit_transform(df),
            columns=columns_to_normalize + [col for col in df.columns if col not in columns_to_normalize]
        )
        # Convertir explícitamente a tipos numéricos las columnas normalizadas
        for col in columns_to_normalize:
           df_normalized[col] = pd.to_numeric(df_normalized[col])
        return df_normalized
    
    def transform_one_hot_encoder(self,df:pd.DataFrame, columns_to_ohe: list[str])-> pd.DataFrame:
        """
        Aplica OneHotEncoder a las columnas categoricas de baja cardinalidad de un DataFrame
        
        :param df: DataFrame de pandas con la columna de IPs
        :param columns_to_ohe: columnas sobre las que se aplica OneHotEncoder
        :return: DataFrame Normalizado (modifica copia)
        """
        print (f"- Normalizing categoric columns...")

        # Crear el transformador de columnas
        preprocessor = ColumnTransformer(
           transformers=[
              ('cat', OneHotEncoder(sparse_output=False, drop=None), columns_to_ohe)
           ],
           remainder='passthrough'  # Mantener columnas numéricas
        )

        #transformar
        encoded_array = preprocessor.fit_transform(df[columns_to_ohe])

        # Obtener nombres de las nuevas columnas
        encoder = preprocessor.named_transformers_['cat']

        new_col_names = encoder.get_feature_names_out(columns_to_ohe)

        # Crear un nuevo DataFrame
        #all_col_names = np.append(new_col_names,  [col for col in df.columns if col not in columns_to_ohe])
        #df_encoded = pd.DataFrame(encoded_array, columns=all_col_names, index=df.index)
        df_encoded = pd.DataFrame(encoded_array, columns=new_col_names, index=df.index)

        df_final = pd.concat([df_encoded, df.drop(columns=columns_to_ohe)], axis=1)

        return df_final
