import pandas as pd
import requests
import time
import json
import os

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

    def __init__(self, token: str, cache_file: str = None, delay: float = 0.1, session_minutes = 30):
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

        url = f"https://ipinfo.io/{ip}/json"
        headers = {'Authorization': f'Bearer {self.token}'}
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            data = resp.json()
            country_code = data.get('country', 'Unknown')
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
        df_copy = df.copy()
        df_copy[new_col] = df_copy[ip_col].apply(self.get_country_from_ip)
        return df_copy
   
    def transform_add_datetime_delta_between_requests(self, df: pd.DataFrame, new_col: str = 'datetime_delta_ms') -> pd.DataFrame:
       """
        Aplica función diff a la columna [datetime] para calcular el tiempo transcurrido en milisengundos
        desde la anterior petición. Añade columna con tiempo (en ms) transcurridos desde anterior request. 
        
        :param df: DataFrame de pandas con la columna de IPs
        :return: DataFrame enriquecido (modifica copia)
       """
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
       df_copy = df.copy()
       df_copy = df_copy.sort_values(by=['client', 'datetime'])
       df_copy['datetime'] = pd.to_datetime(df_copy['datetime'])
    
       df_copy['session_id'] = (df_copy.groupby('client')['datetime']
                          .diff().fillna(pd.Timedelta(seconds=0))
                          .gt(pd.Timedelta(minutes=self.session_minutes))
                          .cumsum())
    
       df_copy['session_global_id'] = df_copy['client'] + '_' + df_copy['session_id'].astype(str)
    
       df_copy['datetime_delta_ms_in_session'] = (df_copy.groupby('session_global_id')['datetime']
                                            .diff().dt.total_seconds().fillna(0) * 1000)
    
       return df_copy