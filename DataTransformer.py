import pandas as pd
import numpy as np
import requests
import time
import json
import os
import re
from tqdm.auto import tqdm
from sklearn.preprocessing import MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import FeatureHasher
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.sparse import hstack


class DataTransformer:
    """
    Clase para enriquecer un DataFrame de pandas con:
     1. transform_add_countrycode: añade el código ISO del país de origen de las IPs usando la API Lite de IPinfo y un sistema de caché. 
     2. transform_add_datetime_delta_between_requests: añade columna con el tiempo transcurrido desde la anterior petición a la actual en milisengundos
                                                       (la primera petición contiene NaN en esta columna)

    Ejemplo de uso:
        transformer = DataTransformer(token="IPINFO_TOKEN", cache_file="cache.json")
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

       # para el primer registro se instancia al valor mas frecuente en la columna
       df_copy[new_col] = df_copy[new_col].fillna(df_copy[new_col].mode()[0])

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
    
    def transform_add_length_columns(self,df:pd.DataFrame, columns: list[str]):
        """
        Añade columnas con la longitud de cadenas para cada columna indicada.

        Parámetros:
            df (pd.DataFrame): El DataFrame original.
            columns (list): Lista de nombres de columnas sobre las que calcular la longitud.

        Retorna:
            pd.DataFrame: El DataFrame con columnas adicionales *_len.
        """
        for col in columns:
            if col in df.columns:
                df[f'{col}_len'] = df[col].astype(str).apply(len)
            else:
                print(f'Advertencia: la columna "{col}" no existe en el DataFrame.')
        return df
    
    def transform_add_os_command_flag(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Añade columnas con indicador 1/0 si el texto contiene comandos comunes de sistema operativo (Linux/Windows).

        Parámetros:
            df (pd.DataFrame): El DataFrame original.
            columns (list): Lista de nombres de columnas a analizar.

        Retorna:
            pd.DataFrame: El DataFrame con columnas adicionales *_has_os_command.
        """
        os_commands = [
            'wget', 'curl', 'chmod', 'rm', 'ls', 'sh', 'bash', 'nc', 'netcat', 'scp',
            'python', 'perl', 'php', 'telnet', 'tftp', 'powershell', 'cmd', 'whoami',
            'netstat', 'ifconfig','cd'
        ]

        pattern = re.compile(r'\b(?:' + '|'.join(re.escape(cmd) for cmd in os_commands) + r')\b', re.IGNORECASE)

        for col in columns:
            if col in df.columns:
                df[f'{col}_has_os_command'] = df[col].astype(str).apply(lambda x: 1 if pattern.search(x) else 0)
            else:
                print(f'Advertencia: la columna "{col}" no existe en el DataFrame.')
        return df

    def transform_add_hex_flag(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Añade columnas con indicador 1/0 si el texto contiene secuencias hexadecimales tipo '\\xHH'.

        Parámetros:
            df (pd.DataFrame): El DataFrame original.
            columns (list): Lista de nombres de columnas a analizar.

        Retorna:
            pd.DataFrame: El DataFrame con columnas adicionales *_has_hex.
        """
        hex_pattern = re.compile(r'\\x[0-9a-fA-F]{2}')

        for col in columns:
            if col in df.columns:
                df[f'{col}_has_hex'] = df[col].astype(str).apply(lambda x: 1 if hex_pattern.search(x) else 0)
            else:
                print(f'Advertencia: la columna "{col}" no existe en el DataFrame.')
        return df

    def transform_add_weird_char_freq(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Añade columnas con la frecuencia de caracteres sospechosos como ';', '&', '|', '`', '"', etc.

        Parámetros:
            df (pd.DataFrame): El DataFrame original.
            columns (list): Lista de columnas a analizar.

        Retorna:
            pd.DataFrame: El DataFrame con columnas adicionales *_weird_char_freq.
        """
        weird_chars = set(";|&`'\"<>$\\(){}[]")

        def compute_freq(s):
            s = str(s)
            if not s:
                return 0
            total = len(s)
            weird_count = sum(1 for c in s if c in weird_chars)
            return weird_count / total if total > 0 else 0

        for col in columns:
            if col in df.columns:
                df[f'{col}_weird_char_freq'] = df[col].astype(str).apply(compute_freq)
            else:
                print(f'Advertencia: la columna "{col}" no existe en el DataFrame.')
        return df

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

    def transform_vectorize_url(self, df:pd.DataFrame, column:str):
        """
        Vectoriza una columna de URLs separando por '/' y devolviendo la matriz dispersa.
        """
        df[column] = df[column].astype(str).apply(lambda x: x.split('?')[0])
        vectorizer = CountVectorizer(token_pattern=r'[^/]+',max_features=100)
        return vectorizer.fit_transform(df[column]), vectorizer
    
    def transform_vectorize_categorical_text(self,df: pd.DataFrame, column: str):
        """
        Vectoriza texto categórico de una única columna del DataFrame usando tokenización simple por palabra.

        :param df: DataFrame de entrada
        :param column: Nombre de la columna a vectorizar (string)
        :return: matriz dispersa (sparse matrix) y el vectorizador usado
        """
       
        vectorizer = CountVectorizer(token_pattern=r'\b\w+\b',max_features=100)
        matrix = vectorizer.fit_transform(df[column].astype(str))
        return matrix, vectorizer

    def transform_combine_numeric_and_sparse(self,df:pd.DataFrame, sparse_matrices)-> pd.DataFrame:
        """
        Toma un DataFrame con columnas numéricas y una lista de matrices dispersas,
        y devuelve una única matriz combinada lista para entrenamiento.
        param: array de nombres de columnas numéricas
        param: array de matrices dispersas
        return: DataFrame
        """
       # Seleccionar columnas numéricas automáticamente
        numeric_df = df.select_dtypes(include=['number'])
        #scaler = StandardScaler()
        #numeric_scaled = scaler.fit_transform(numeric_df)
        #df_numeric_scaled = pd.DataFrame(numeric_scaled, columns=numeric_df.columns, index=df.index)

        # Combinar todas las matrices dispersas
        combined_sparse = hstack(sparse_matrices).tocsr()

        # Convertir sparse matrix a DataFrame
        sparse_df = pd.DataFrame.sparse.from_spmatrix(combined_sparse, index=df.index)

        # Concatenar y devolver
        return pd.concat([numeric_df, sparse_df], axis=1)
    
    def transform_feature_hashing(self, df:pd.DataFrame, column_name, n_features=10, preserve_original=False)->pd.DataFrame:
        """
        Aplica Feature Hashing a una columna categórica de un DataFrame.
        
        Parámetros:
        -----------
        param df : pandas.DataFrame. DataFrame que contiene los datos
        param column_name : str.  Nombre de la columna categórica a transformar 
        param n_features : int, default=10 Número de dimensiones (features) para la matriz hash resultante
        param preserve_original : bool, default=False. Si True, mantiene la columna original en el DataFrame resultante
        
        return: pandas.DataFrame
            DataFrame con las nuevas columnas de feature hashing añadidas y
            opcionalmente, la columna original eliminada
        """
        # Verificamos que la columna exista en el DataFrame
        if column_name not in df.columns:
            raise ValueError(f"La columna '{column_name}' no existe en el DataFrame")
        
        # Creamos una copia del DataFrame para no modificar el original
        result_df = df.copy()
        
        # Convertimos los valores a strings (por si acaso no lo fueran)
        values = result_df[column_name].astype(str).values
        
        # Creamos el feature hasher
        hasher = FeatureHasher(n_features=n_features, input_type='string')
        
        # Aplicamos feature hashing - transformamos los valores a formato requerido
        values_formatted = [[val] for val in values]  # Lista de listas para FeatureHasher
        hashed_features = hasher.transform(values_formatted)
        
        # Convertimos la matriz dispersa en DataFrame
        hashed_df = pd.DataFrame(
            hashed_features.toarray(),
            columns=[f"{column_name}_hash_{i}" for i in range(n_features)]
        )
        
        # Concatenamos con el DataFrame original
        result_df = pd.concat([result_df, hashed_df], axis=1)
        
        # Eliminamos la columna original si se especifica
        if not preserve_original:
            result_df = result_df.drop(columns=[column_name])
        
        return result_df

    def transform_vectorize_raw_request(self,df: pd.DataFrame, column: str)->pd.DataFrame:
        """
        Aplica TFIdfVectorizez a una columna categórica de un DataFrame. 
        Se aplica este tipo de vectorización a la request RAW para ponderar y penalizar las palabras del diccionario más frecuetnes.
        
        Vectoriza texto categórico de una única columna del DataFrame usando tokenización de ngramas para detectar comandos 
        cortos de entre 3 y 6 caracteres (la tokenización se realiza en bloques de 3, 4, 5 y 6 caracteres)

        :param df: DataFrame de entrada
        :param column: Nombre de la columna a vectorizar (string)
        :return: matriz dispersa (sparse matrix) y el vectorizador usado
        """
        
        vectorizer = TfidfVectorizer(analyzer='char_wb', ngram_range=(3, 6),max_features=200)
        matrix = vectorizer.fit_transform(df[column].astype(str))
        return matrix, vectorizer
        
