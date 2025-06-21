import os
import numpy as np
import pickle
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
import pandas as pd
import numpy as np
import requests
import time
import json
import re
from tqdm.auto import tqdm
from sklearn.preprocessing import MinMaxScaler
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import FunctionTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import FeatureHasher
from sklearn.feature_extraction.text import TfidfVectorizer,HashingVectorizer
from sklearn.base import BaseEstimator,TransformerMixin
from sklearn.utils.validation import check_array, check_is_fitted
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
import time
import inspect

from scipy.sparse import hstack
from abc import ABC, abstractmethod
from dotenv import load_dotenv

#**********************************************************************************************
#                                       SETTING 
#**********************************************************************************************
load_dotenv()

IPINFO_TOKEN = os.getenv("IPINFO_TOKEN")
CACHE_FILE = os.getenv("CACHE_FILE", "cache.json")
SESSION_MIN = os.getenv("SESSION_MIN", 20)
#**********************************************************************************************
#                                       INTERFAZ 
#**********************************************************************************************
class AIModelInterface(ABC):
    @abstractmethod
    def train_model(self, data, trainParams):
        """
        Entrena el modelo con los datos proporcionados.
        :param data: datos de entrenamiento
        :param trainParams: parámetros de entrenamiento
        :return: matriz de confusión
        """
        pass

    @abstractmethod
    def save_model(self, modelName):
        """
        Guarda el modelo entrenado.
        :param modelName: nombre del archivo del modelo
        :return: path donde se ha guardado
        """
        pass

    @abstractmethod
    def load_model(self, modelName):
        """
        Carga un modelo previamente entrenado.
        :param modelName: nombre del archivo del modelo
        :return: modelo cargado
        """
        pass

    @abstractmethod
    def test_model(self, data):
        """
        Testea el modelo con nuevos datos.
        :param data: datos de entrada
        :return: predicciones, clasificaciones, etc.
        """
        pass

    def get_transformation(self) -> ColumnTransformer:
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
        return transformation

#**********************************************************************************************
#                                       MODELOS IMPLEMENTADOS 
#**********************************************************************************************
# Implementación para Isolation Forest
class IsolationForestModel(AIModelInterface):
    def __init__(self):
        self.model = None
        self.transformer = self.get_transformation()
        self.pca = PCA(n_components=2)
        self.timer = Timer()
        self.training_time=0
        
    def train_model(self, data, trainParams):
        self.timer.start()

        test_size = trainParams.get('validation_split', 0.2)  # 20% por defecto para validación
        
        train_indices, validation_indices = train_test_split(
            data.index, 
            test_size=test_size, 
            random_state=42
            )
    
        train_data = data.loc[train_indices].copy()
        validation_data = data.loc[validation_indices].copy()
        
        # Resetear índices
        train_data = train_data.reset_index(drop=True)
        validation_data = validation_data.reset_index(drop=True)

        # Escalar datos y transformar
        scaled_train_data  = self.transformer.fit_transform(train_data)
        self.timer.elapsed("transformer.fit_transform over training-dataset done")

        # Entrenar modelo
        self.model = IsolationForest(
            contamination=trainParams.get('contamination', 0.1),
            n_estimators=trainParams.get('n_estimators', 100),
            random_state=42
        )
        
        predictions = self.model.fit(scaled_train_data )
        self.timer.elapsed("model.fit over training-dataset done")
        
        # PCA para visualización
        pca_data = self.pca.fit(scaled_train_data )
        self.timer.elapsed("pca.fit over training-dataset done")

        scaled_validation_data = self.transformer.transform(validation_data)
        validation_predictions = self.model.predict(scaled_validation_data)
        self.timer.elapsed("predict over validation-dataset done")

        # PCA para datos de validación
        validation_pca_data = self.pca.transform(scaled_validation_data)
        self.timer.elapsed("validation pca.transform over validation-dataset done")

        # Calcular métricas
        anomaly_count = np.sum(validation_predictions  == -1)
        normal_count = np.sum(validation_predictions  == 1)
        self.timer.elapsed("calculate metrics over validation-dataset done")

        validation_confusion_matrix = {
            'anomalies': anomaly_count,
            'normal': normal_count,
            'contamination_rate': anomaly_count / len(validation_predictions)            
        }
        self.training_time=self.timer.end()
        return validation_confusion_matrix, validation_predictions, validation_pca_data, validation_data
    
    def save_model(self, modelName):
        model_data = {
            'model': self.model,
            'transformer': self.transformer,
            'pca': self.pca,
            'training_time':self.training_time
        }
        
        if not os.path.exists('models'):
            os.makedirs('models')
            
        path = f'models/{modelName}_isolation_forest.pkl'
        with open(path, 'wb') as f:
            pickle.dump(model_data, f)
        return path
    
    def load_model(self, modelName):
        path = f'models/{modelName}_isolation_forest.pkl'
        with open(path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.transformer = model_data['transformer']
        self.pca = model_data['pca']
        return self.model
    
    def test_model(self, data):
        if self.model is None:
            raise ValueError("Modelo no entrenado")
        
        self.timer.start()

        scaled_data = self.transformer.transform(data)
        self.timer.elapsed("transform done")
        
        predictions = self.model.predict(scaled_data)
        self.timer.elapsed("predict done")

        scores = self.model.decision_function(scaled_data)
        self.timer.elapsed("decision_fucntion done")
        
        pca_data = self.pca.transform(scaled_data)
        self.timer.elapsed("pca done")
        
        self.timer.end()
        
        return predictions, scores, pca_data
# Implementación para K-Means
class KMeansModel(AIModelInterface):
    def __init__(self):
        self.model = None
        self.transformer = self.get_transformation()
        self.pca = PCA(n_components=2)
        self.timer = Timer()
        self.training_time=0
        
    def train_model(self, data, trainParams):
        self.timer.start()

        # Escalar datos
        scaled_data = self.transformer.fit_transform(data)
        self.timer.elapsed("fit_transform done")
        # Entrenar modelo
        k = trainParams.get('n_clusters', 8)
        self.model = KMeans(
            n_clusters=k,
            random_state=42,
            n_init=10
        )
        
        cluster_labels = self.model.fit_predict(scaled_data)
        self.timer.elapsed("fit_predict done")

        # PCA para visualización
        pca_data = self.pca.fit_transform(scaled_data)
        self.timer.elapsed("pca done")
        
        # Calcular métricas
        inertia = self.model.inertia_
        silhouette_avg = silhouette_score(scaled_data, cluster_labels)
        self.timer.elapsed("silhouette_score done")
        
        # Detectar anomalías basándose en distancia a centroides
        distances = self.model.transform(scaled_data)
        min_distances = np.min(distances, axis=1)
        threshold = np.percentile(min_distances, 90)  # Top 10% como anomalías
        anomalies = min_distances > threshold
        self.timer.elapsed("anomalies dectection done")
        
        confusion_matrix = {
            'inertia': inertia,
            'silhouette_score': silhouette_avg,
            'n_clusters': k,
            'anomalies': np.sum(anomalies),
            'normal': len(anomalies) - np.sum(anomalies)
        }
        self.training_time=self.self.timer.end()
        return confusion_matrix, cluster_labels, pca_data, anomalies, min_distances
    
    def save_model(self, modelName):
        model_data = {
            'model': self.model,
            'transformer': self.transformer,
            'pca': self.pca,
            'training_time':self.training_time
        }
        
        if not os.path.exists('models'):
            os.makedirs('models')
            
        path = f'models/{modelName}_kmeans.pkl'
        with open(path, 'wb') as f:
            pickle.dump(model_data, f)
        return path
    
    def load_model(self, modelName):
        path = f'models/{modelName}_kmeans.pkl'
        with open(path, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.transformer = model_data['transformer']
        self.pca = model_data['pca']
        return self.model
    
    def test_model(self, data):
        if self.model is None:
            raise ValueError("Modelo no entrenado")
        self.timer.start()
        scaled_data = self.transformer.transform(data)
        self.timer.elapsed("transform done")

        cluster_labels = self.model.predict(scaled_data)
        self.timer.elapsed("predict done")

        pca_data = self.pca.transform(scaled_data)
        self.timer.elapsed("pca done")
        
        # Calcular distancias para detección de anomalías
        distances = self.model.transform(scaled_data)
        min_distances = np.min(distances, axis=1)
        self.timer.elapsed("calculate distances done")

        return cluster_labels, min_distances, pca_data

#**********************************************************************************************
#                CLASES AUXILIARES: LECTURA, LIMPIEZA Y TRANSFORMACIÓN DE DATOS 
#**********************************************************************************************

#lectura
class DataCLFReader:
    def logs_to_df(self, logfile, output_dir, errors_file):
        regex = r'^(?P<client>\S+) \S+ (?P<userid>\S+) \[(?P<datetime>[^\]]+)\] "(?P<method>[A-Z]+) (?P<request>[^ "]+)? HTTP/[0-9.]+" (?P<status>[0-9]{3}) (?P<size>[0-9]+|-) "(?P<referrer>[^"]*)" "(?P<useragent>[^"]*)"'

        parsed_lines = []
        malformed_lines = []
        linenumber = 0

        with open(logfile, encoding='utf-8', errors='replace') as source_file:
            for line in tqdm(source_file):
                match = re.match(regex, line)
                if match:
                    group = match.groupdict()
                    parsed_lines.append({
                        'client': group['client'],
                        'userid': group['userid'],
                        'datetime': group['datetime'],
                        'method': group['method'],
                        'request': group['request'],
                        'status': int(group['status']),
                        'size_in_bytes': int(group['size']) if group['size'].isdigit() else 0,
                        'referer': group['referrer'],
                        'user_agent': group['useragent'],
                        'parsed_ok': 1,
                        'raw_request': line.strip()
                    })
                else:
                    malformed_lines.append({
                        'client': self._extract_ip(line),
                        'userid': '-',
                        'datetime': self._extract_datetime(line),
                        'method': 'unknown',
                        'request': 'unknown',
                        'status': -100,
                        'size_in_bytes': -100,
                        'referer': 'unknown',
                        'user_agent': 'unknown',
                        'parsed_ok': 0,
                        'raw_request': line.strip()
                    })
                    with open(errors_file, 'at') as errfile:
                        print(line.strip(), file=errfile)

                linenumber += 1
                if linenumber % 250_000 == 0:
                    self._save_chunk(parsed_lines + malformed_lines, output_dir, linenumber)
                    parsed_lines.clear()
                    malformed_lines.clear()

            # guardar lo que queda
            if parsed_lines or malformed_lines:
                self._save_chunk(parsed_lines + malformed_lines, output_dir, linenumber)

        df = pd.read_parquet(output_dir)
        return df

    def _save_chunk(self, data, output_dir, linenumber):
        df = pd.DataFrame(data)
        df.to_parquet(f'{output_dir}/file_{linenumber}.parquet', index=False)

    def _extract_ip(self, line):
        # Intenta extraer la IP al principio
        match = re.match(r'^(\d+\.\d+\.\d+\.\d+)', line)
        return match.group(1) if match else 'unknown'

    def _extract_datetime(self, line):
        # Intenta extraer la fecha entre corchetes
        match = re.search(r'\[(.*?)\]', line)
        return match.group(1) if match else 'unknown'

#limpieza
class DataCleaner:
    """
    Clase que contiene métodos para la limpieza y preprocesamiento de DataFrames de pandas.
    
    Esta clase proporciona varias funcionalidades para tratar con datos faltantes,
    valores atípicos, y otras operaciones comunes de limpieza de datos.
    """
    
    def __init__(self):
        """
        Inicializa una instancia de la clase DataCleaner.
        """
        pass
    
    def delete_rows_with_faulting_category(self, data):
        """
        Elimina filas con datos faltantes.
        
        Este método elimina cualquier fila del DataFrame que contenga al menos un valor
        faltante (NaN, None, etc.). Es útil cuando se requiere un conjunto de datos completo
        para el análisis o entrenamiento de modelos.
        
        Args:
            data (pandas.DataFrame): DataFrame de pandas con los datos a limpiar.
            
        Returns:
            pandas.DataFrame: DataFrame sin las filas que contienen valores faltantes.
            
        Examples:
            >>> cleaner = DataCleaner()
            >>> df = pd.DataFrame({'A': [1, 2, np.nan], 'B': [4, np.nan, 6]})
            >>> cleaned_df = cleaner.delete_rows_with_faulting_category(df)
            >>> print(cleaned_df)
               A    B
            0  1.0  4.0
        """
        print("- Deleting rows with faulting categories ...")
        # se verifica que el parámetro sea un DataFrame de pandas
        if not isinstance(data, pd.DataFrame):
            raise TypeError("- El parámetro 'data' debe ser pandas.DataFrame")
        
        # se realiza una copia del DataFrame para no modificar el original
        df_copy = data.copy()
        
        # se elimina las filas con al menos un valor faltante
        df_clean = df_copy.dropna()
        
        filas_eliminadas = df_copy[~df_copy.index.isin(df_clean.index)]
        if not filas_eliminadas.empty:
            print("\nFilas eliminadas por dropna():")
            print(filas_eliminadas)
            col_con_null = filas_eliminadas.columns[filas_eliminadas.isna().any()]
            print('columna con null:')
            print(col_con_null)


        # se resetean los índices para que sean consecutivos
        df_clean = df_clean.reset_index(drop=True)
        
        return df_clean
    
    def info_missing_values(self, data):
        """
        Proporciona información sobre valores faltantes en el DataFrame.
        
        Args:
            data (pandas.DataFrame): DataFrame a analizar.
            
        Returns:
            pandas.DataFrame: DataFrame con información sobre valores faltantes.
        """
        # Contamos valores faltantes por columna
        missing_values = data.isnull().sum()
        
        # Calculamos el porcentaje de valores faltantes
        missing_percentage = 100 * missing_values / len(data)
        
        # Creamos un DataFrame con la información
        missing_info = pd.DataFrame({
            'Valores faltantes': missing_values,
            'Porcentaje': missing_percentage.round(2)
        })
        
        return missing_info
    
    def delete_column(self,df:pd.DataFrame, column:str)-> pd.DataFrame:
       """
        Elimina la columna indicada en el parámetro 'column' 
        
        :param df: DataFrame de pandas 
        :param column: columna a eliminar
        :return: DataFrame modificado con una columna menos
       """
       print(f"- Deleting columns with irrelevant values: [{column}] ...")
       df.drop(columns=[column], inplace=True)
       return df

#transformación
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

    def apply_log(self, X):
        return np.log1p(X[:,1])
    
    def replace_negative_with_mean(self,X):
        X_flat = X.flatten()
        non_negative = X_flat[X_flat >= 0]
        mean_val = np.mean(non_negative) if len(non_negative) > 0 else 0
        return np.where(X_flat < 0, mean_val, X_flat).reshape(-1, 1)
    
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
        tqdm.pandas(desc="- Geolocalizando IPs")  # Configura tqdm para pandas
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
       print ("- Adding col. datetime_delta_ms (tiempo en ms. transcurrido entre peticiones consecutivas")
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
              ('cat', OneHotEncoder(handle_unknown='ignore',sparse_output=False, drop=None), columns_to_ohe)
           ],
           remainder='passthrough'  # Mantener columnas numéricas
        )

        #transformar
        encoded_array = preprocessor.fit_transform(df[columns_to_ohe])

        # Obtener nombres de las nuevas columnas
        encoder = preprocessor.named_transformers_['cat']

        new_col_names = encoder.get_feature_names_out(columns_to_ohe)

        # Crear un nuevo DataFrame
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

class Timer:
    def __init__(self):
        self.start_time = None
        self.last_elapsed_time = None
        self.is_started = False
    
    def _get_caller_function_name(self):
        """Obtiene el nombre de la función que llamó al método actual"""
        # Obtenemos el stack de llamadas
        # [0] es este método, [1] es el método de Timer que lo llamó, [2] es la función del usuario
        frame = inspect.currentframe()
        try:
            caller_frame = frame.f_back.f_back
            if caller_frame:
                return caller_frame.f_code.co_name
            return "unknown"
        finally:
            del frame
    
    def start(self, message=""):
        """Inicializa el timer y muestra mensaje de inicio"""
        self.start_time = time.time()
        self.last_elapsed_time = self.start_time
        self.is_started = True
        
        function_name = self._get_caller_function_name()
        print(f"- [{function_name}] 🕐 START: {message}")
    
    def elapsed(self, message="")->float:
        """Muestra el tiempo transcurrido desde el último elapsed"""
        if not self.is_started:
            print("❌ Timer no ha sido iniciado. Llama a start() primero.")
            return 0
        
        current_time = time.time()
        elapsed_seconds = current_time - self.last_elapsed_time
        self.last_elapsed_time = current_time
        
        function_name = self._get_caller_function_name()
        print(f"- [{function_name}] ✅ {message} ({elapsed_seconds:.3f}s)")
        return elapsed_seconds
    
    def end(self, message="")->float:
        """Finaliza el timer y muestra mensaje final"""
        if not self.is_started:
            print("❌ Timer no ha sido iniciado. Llama a start() primero.")
            return
        
        current_time = time.time()
        total_time = current_time - self.start_time
        
        function_name = self._get_caller_function_name()
        print(f"- [{function_name}] 🏁 END: {message} (Total: {total_time:.3f}s)")
        
        # Reset del timer
        self.start_time = None
        self.last_elapsed_time = None
        self.is_started = False

        return total_time


'''
Heredar de BaseEstimator y TransformerMixin:
- BaseEstimator: proporciona impl. para get/set_params()
- TransformMixin: impl. el método fit_transform()
'''
class IpAddressToISOCountryCodeTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, ip_info_service_token: str, cache_file: str = None, delay: float = 0.005):
        self.ip_info_service_token = ip_info_service_token
        self.cache_file = cache_file
        self.delay = delay
        self.ip_cache_ = {}
        if cache_file:
            self._load_cache()
        else:
            print("  - No se ha especificado guardar cache en disco")
    
    def fit(self, X, y=None):
        """
        Ajusta el transformador (no hace nada en este caso, pero es requerido por sklearn)
        """
        # Convertir a array si es necesario para validación
        if hasattr(X, 'iloc'):  # Es un DataFrame
            X_array = X.values
        else:
            X_array = check_array(X, dtype=object)
        
        self.n_features_in_ = X_array.shape[1]
        return self
    
    def transform(self, X):
        """
        Transforma las direcciones IP a códigos de país
        """
        check_is_fitted(self, 'n_features_in_')
        
        # Manejar tanto DataFrames como arrays
        if hasattr(X, 'iloc'):  # Es un DataFrame
            ip_series = X.iloc[:, 0]  # Se asume que la IP está en la primera columna
            country_codes = []
            
            print("  - Adding col.: country_code from IP (external service: IpInfo-Lite)")
            tqdm.pandas(desc="- Geolocalizando IPs")
            
            for ip in tqdm(ip_series, desc="  - Processing IPs"):
                country_code = self._get_country_from_ip(str(ip))
                country_codes.append(country_code)
            
            self._save_cache()
            
            # Retornar como array numpy para compatibilidad con sklearn
            return np.array(country_codes).reshape(-1, 1)
        
        else:  # Es un array
            X_array = check_array(X, dtype=object)
            assert self.n_features_in_ == X_array.shape[1]
            
            country_codes = []
            print("  - Adding col.: country_code from IP (external service: IpInfo-Lite)")
            
            for row in tqdm(X_array, desc="Processing IPs"):
                ip = str(row[0])  
                country_code = self._get_country_from_ip(ip)
                country_codes.append(country_code)
            
            self._save_cache()
            return np.array(country_codes).reshape(-1, 1)
    
    def _load_cache(self):
        """
        Carga la caché desde disco si el fichero existe.
        """
        if os.path.isfile(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.ip_cache_ = json.load(f)
                print(f"  - Cache cargado desde: {self.cache_file}")
            except Exception as e:
                print(f"  - Error leyendo cache: {e}")
                self.ip_cache_ = {}
        else:
            print(f"  - No se localiza fichero de cache con nombre: {self.cache_file}")
    
    def _save_cache(self):
        """
        Guarda la caché en formato JSON en disco si se proporcionó cache_file.
        """
        if not self.cache_file:
            return
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.ip_cache_, f, ensure_ascii=False, indent=2)
            print(f"  - Cache guardado en: {self.cache_file}")
        except Exception as e:
            print(f"  - Error al guardar fichero de caché: {e}")
    
    def _get_country_from_ip(self, ip: str) -> str:
        """
        Devuelve el código ISO 3166-1 alfa-2 del país de la IP.
        Usa la API de IPinfo y un sistema de caché en memoria.
        
        :param ip: Dirección IP en formato string
        :return: Código ISO de dos letras o 'Unknown'/'Error'
        """
        # Devuelve del caché si ya existe
        if ip in self.ip_cache_:
            return self.ip_cache_[ip]
        
        url = f"https://api.ipinfo.io/lite/{ip}"
        headers = {'Authorization': f'Bearer {self.ip_info_service_token}'}
        
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            data = resp.json()
            country_code = data.get('country_code', 'Unknown')
            
            # Guardamos en caché y esperamos por delay
            self.ip_cache_[ip] = country_code
            time.sleep(self.delay)
            return country_code
            
        except Exception as e:
            print(f"  - Error consultando IP {ip}: {e}")
            # En caso de error guardamos y devolvemos 'Error'
            self.ip_cache_[ip] = 'Error'
            return 'Error'
    
    def get_feature_names_out(self, input_features=None):
        """
        Devuelve los nombres de las características de salida (requerido por sklearn >= 1.0)
        """
        if input_features is None:
            return np.array([f'country_code_{i}' for i in range(self.n_features_in_)])
        else:
            return np.array([f'{feature}_to_contry_code' for feature in input_features])
class DeltaTimeBetweenDatetimesTransformer(BaseEstimator, TransformerMixin):
    
    def fit(self, X, y=None):
        """
        Ajusta el transformador (no hace nada en este caso, pero es requerido por sklearn)
        """
        # Convertir a array si es necesario para validación
        if hasattr(X, 'iloc'):  # Es un DataFrame
            X_array = X.values
        else:
            X_array = check_array(X, dtype=object)
        
        self.n_features_in_ = X_array.shape[1]
        return self
    
    def transform(self, X):
        """
        Aplica función diff a la columna [datetime] para calcular el tiempo transcurrido en milisengundos
        desde la anterior petición. Añade columna con tiempo (en ms) transcurridos desde anterior request. 
        
        :param df: DataFrame de pandas con la columna de IPs
        :return: DataFrame enriquecido (modifica copia)
        """
        check_is_fitted(self, 'n_features_in_')
        
        # Manejar tanto DataFrames como arrays
        if hasattr(X, 'iloc'):  # Es un DataFrame
            datetime_series = X.iloc[:, 0]  # Se asume que datetime está en la primera columna   
            datetime_delta_between_request = []
            print ("  - Adding col. datetime_delta_ms (tiempo en ms. transcurrido entre peticiones consecutivas")
            
            datetime_series= pd.to_datetime(datetime_series, format='%d/%b/%Y:%H:%M:%S %z', errors='coerce')
       
            # diferencia como objetos Timedelta de Pandas y paso a milisegundos para tener un dato númerico
            datetime_delta_between_request = datetime_series.diff().dt.total_seconds() * 1000

            # para el primer registro se instancia al valor mas frecuente en la columna o cero si es negativo
            datetime_delta_between_request = datetime_delta_between_request.fillna(datetime_delta_between_request.mode()[0])
            datetime_delta_between_request = datetime_delta_between_request.apply(lambda x: 0 if x < 0 else x)

            # Retornar como array numpy para compatibilidad con sklearn
            datetime_delta_between_request= np.array(datetime_delta_between_request).reshape(-1, 1)
            return datetime_delta_between_request
            #datetime_series= np.array(datetime_series).reshape(-1, 1)
            #return np.concatenate([datetime_series,datetime_delta_between_request],axis=1)
        
        else:  # Es un array
            X_array = check_array(X, dtype=object)
            assert self.n_features_in_ == X_array.shape[1]
            print ("  - Adding col. datetime_delta_ms (tiempo en ms. transcurrido entre peticiones consecutivas")
            
            datetime_delta_between_request = []
            datetime_series= pd.to_datetime(X_array, format='%d/%b/%Y:%H:%M:%S %z')
            # diferencia como objetos Timedelta de Pandas y paso a milisegundos para tener un dato númerico
            datetime_delta_between_request = datetime_series.diff().dt.total_seconds() * 1000

            # para el primer registro se instancia al valor mas frecuente en la columna
            datetime_delta_between_request = datetime_delta_between_request.fillna(datetime_delta_between_request.mode()[0])

           # Retornar como array numpy para compatibilidad con sklearn
            datetime_delta_between_request= np.array(datetime_delta_between_request).reshape(-1, 1)
            return datetime_delta_between_request
            #datetime_series= np.array(datetime_series).reshape(-1, 1)
            #return np.concatenate([datetime_series,datetime_delta_between_request],axis=1)
        
        
    def get_feature_names_out(self, input_features=None):
        """
        Devuelve los nombres de las características de salida (requerido por sklearn >= 1.0)
        """
        #return np.array(['datetime','datetime_delta_ms'])
        return np.array(['datetime_delta_ms'])        
class CalculateLengthTransformer(BaseEstimator,TransformerMixin):
    def __init__(self):
        self.features_names=None

    def fit(self, X, y=None):
        """
        Ajusta el transformador (no hace nada en este caso, pero es requerido por sklearn)
        """
        # Convertir a array si es necesario para validación
        if hasattr(X, 'iloc'):  # Es un DataFrame
            X_array = X.values
        else:
            X_array = check_array(X, dtype=object)
        self.features_names= X.columns
        self.n_features_in_ = X_array.shape[1]
        return self
    
    def transform(self, X):
        """
        Añade columnas con la longitud de cadenas para cada columna indicada
        """

        check_is_fitted(self, 'n_features_in_')
        
        # Manejar tanto DataFrames como arrays
        if hasattr(X, 'iloc'):  # Es un DataFrame
            results = []
            self.features_names=X.columns
            for col_idx in range(X.shape[1]):
                column_as_series = X.iloc[:, col_idx]
                lengths = column_as_series.astype(str).apply(len)
                results.append(lengths.values.reshape(-1, 1))

            return np.concatenate(results, axis=1)
        else:
            return None
        
        
    def get_feature_names_out(self, input_features=None):
        """
        Devuelve los nombres de las características de salida (requerido por sklearn >= 1.0)
        """
        if input_features is None:
            return np.array([f'len_{self.features_names[i]}' for i in range(self.n_features_in_)])
        else:
            return np.array([f'{feature}_len' for feature in input_features])  
class GetInfoSessionTransformer(BaseEstimator,TransformerMixin):
    def __init__(self, session_minutes=20):
        self.session_minutes= session_minutes

    def fit(self, X, y=None):
        """
        Ajusta el transformador (no hace nada en este caso, pero es requerido por sklearn)
        """
        # Convertir a array si es necesario para validación
        if hasattr(X, 'iloc'):  # Es un DataFrame
            X_array = X.values
        else:
            X_array = check_array(X, dtype=object)
        
        self.n_features_in_ = X_array.shape[1]
        return self
    
    def transform(self, X):
        """
        Aplica agrupación por iP en el rango de tiempo definido en "self.session_minutes" para obtener el conjunto de requests 
        que serán interpretados como una sesión de usuario (por defecto self.session_minutes = 30) Se añade una columna 
        "session_global_id" con identificador único (autogenerado) y "datetime_delta_ms_in_session" para informar de la diferencia de tiempo
        entre peticiones de la misma sesión. 
        
        :param df: DataFrame de pandas con la columna de IPs
        :return: DataFrame enriquecido (modifica copia)
        """
        check_is_fitted(self, 'n_features_in_')
        
        # Manejar tanto DataFrames como arrays
        if hasattr(X, 'iloc'):  # Es un DataFrame
            #datetime_series = X.iloc[:, 0]  # Se asume que la columna a modificar es la primera columna: datetime
            #ip_series = X.iloc[:, 1]        # la segunda columna es la que se necesita para agrupar, en este caso las IPs
            
            returned_df=pd.DataFrame(X)

            #transform temp_series
            #------------------------------
            print (f"  - Adding col. session_global_id and datetime_delta_ms_in_session (id de sesión: request from same IP in range {self.session_minutes} min.")
            
            X = X.sort_values(by=['client', 'datetime'])
            X['datetime'] = pd.to_datetime(X['datetime'],format='%d/%b/%Y:%H:%M:%S %z',errors='coerce')
            
            #Se obtiene el listado de sesiones basado en IP en rangos de tiempo de 'session_minutes' min. (Una misma IP puede 
            #tener distintas sesiones si ha tenido actividad en rangos de tiempo superiores a 'session_minutes')
            returned_df['session_id'] = (X.groupby('client')['datetime']
                                .diff().fillna(pd.Timedelta(seconds=0))
                                .gt(pd.Timedelta(minutes=self.session_minutes))
                                .cumsum())
            
            returned_df['session_global_id'] = X['client'] + '_' + returned_df['session_id'].astype(str)
            returned_df['datetime'] = X['datetime']
            returned_df['datetime_delta_ms_in_session'] = (returned_df.groupby('session_global_id')['datetime']
                                                    .diff().dt.total_seconds().fillna(0) * 1000)
            
            #Se elimina la columna temporal 'session_id':
            returned_df.drop(columns=['session_id','datetime'],inplace=True)
            

            # para el primer registro se instancia al valor mas frecuente en la columna o cero si es negativo
            returned_df["datetime_delta_ms_in_session"] = returned_df["datetime_delta_ms_in_session"].fillna(0)
            returned_df["datetime_delta_ms_in_session"] = returned_df["datetime_delta_ms_in_session"].apply(lambda x: max(x, 0))
            
            return returned_df['datetime_delta_ms_in_session'].values.reshape(-1, 1)
        else:  
            return None
        
    def get_feature_names_out(self, input_features=None):
        """
        Devuelve los nombres de las características de salida (requerido por sklearn >= 1.0)
        """
        #return np.array(['session_global_id','datetime_delta_ms_in_session'])   
        return np.array(['datetime_delta_ms_in_session']) 
class AddOsCommandFlagTransformer(BaseEstimator,TransformerMixin):
    def __init__(self, os_commands:list[str]|None = None):

        if not os_commands:
            self.os_commands = ['wget', 'curl', 'chmod', 'rm', 'ls', 'sh', 'bash', 'nc', 'netcat', 'scp',
                'python', 'perl', 'php', 'telnet', 'tftp', 'powershell', 'cmd', 'whoami',
                'netstat', 'ifconfig','cd']
        else:
            self.os_commands = os_commands

    def fit(self, X, y=None):
        """
        Ajusta el transformador (no hace nada en este caso, pero es requerido por sklearn)
        """
        # Convertir a array si es necesario para validación
        if hasattr(X, 'iloc'):  # Es un DataFrame
            X_array = X.values
        else:
            X_array = check_array(X, dtype=object)
        
        self.n_features_in_ = X_array.shape[1]
        return self
    
    def transform(self, X):
        """
        Añade columnas con indicador 1/0 si el texto contiene comandos comunes de sistema operativo (Linux/Windows).

        Parámetros:
            df (pd.DataFrame): El DataFrame original.
            columns (list): Lista de nombres de columnas a analizar.
        """
        check_is_fitted(self, 'n_features_in_')
        
        # Manejar  DataFrames 
        if hasattr(X, 'iloc'):  # Es un DataFrame

            results = []

            os_pattern = re.compile(r'\b(?:' + '|'.join(re.escape(cmd) for cmd in self.os_commands) + r')\b', re.IGNORECASE)

            for col_idx in range(X.shape[1]):
                feature_as_series = X.iloc[:, col_idx]
                feature_has_command = feature_as_series.astype(str).apply(lambda x: 1 if os_pattern.search(x) else 0)
                results.append(feature_has_command.values.reshape(-1, 1))

            return np.concatenate(results, axis=1)

        else:  # Es un array
            return None
        
    def get_feature_names_out(self, input_features=None):
        """
        Devuelve los nombres de las características de salida (requerido por sklearn >= 1.0)
        """
        if input_features is None:
            return np.array([f'has_os_command_{i}' for i in range(self.n_features_in_)])
        else:
            return np.array([f'{feature}_has_os_command' for feature in input_features])  
class AddHexadecimalCharactersFlagTransformer(BaseEstimator,TransformerMixin):
    def __init__(self):
        pass

    def fit(self, X, y=None):
        """
        Ajusta el transformador (no hace nada en este caso, pero es requerido por sklearn)
        """
        # Convertir a array si es necesario para validación
        if hasattr(X, 'iloc'):  # Es un DataFrame
            X_array = X.values
        else:
            X_array = check_array(X, dtype=object)
        
        self.n_features_in_ = X_array.shape[1]
        return self
    
    def transform(self, X):
        """
        Añade columnas con indicador 1/0 si el texto contiene secuencias hexadecimales tipo '\\xHH'.
        """
        check_is_fitted(self, 'n_features_in_')
        
        # Manejar  DataFrames 
        if hasattr(X, 'iloc'):  # Es un DataFrame

            results = []

            hex_pattern = re.compile(r'\\x[0-9a-fA-F]{2}')

            for col_idx in range(X.shape[1]):
                feature_as_series = X.iloc[:, col_idx]
                feature_has_hexadecimal_char = feature_as_series.astype(str).apply(lambda x: 1 if hex_pattern.search(x) else 0)
                results.append(feature_has_hexadecimal_char.values.reshape(-1, 1))

            return np.concatenate(results, axis=1)

        else:  # Es un array
            return None
        
    def get_feature_names_out(self, input_features=None):
        """
        Devuelve los nombres de las características de salida (requerido por sklearn >= 1.0)
        """
        if input_features is None:
            return np.array([f'has_hex_{i}' for i in range(self.n_features_in_)])
        else:
            return np.array([f'{feature}_has_hex' for feature in input_features])
class AddWeirdCharactersFrequencyTransformer(BaseEstimator,TransformerMixin):
    def __init__(self, weird_chars:str|None = None):
        if not weird_chars:
            self.weird_chars = set(";|&`'\"<>$\\(){}[]")
        else:
            self.weird_chars = weird_chars

    def fit(self, X, y=None):
        """
        Ajusta el transformador (no hace nada en este caso, pero es requerido por sklearn)
        """
        # Convertir a array si es necesario para validación
        if hasattr(X, 'iloc'):  # Es un DataFrame
            X_array = X.values
        else:
            X_array = check_array(X, dtype=object)
        
        self.n_features_in_ = X_array.shape[1]
        return self
    
    def transform(self, X):
        """
        Añade columnas con la frecuencia de caracteres sospechosos como ';', '&', '|', '`', '"', etc.
        """
        check_is_fitted(self, 'n_features_in_')
        
        # Manejar  DataFrames 
        if hasattr(X, 'iloc'):  # Es un DataFrame
            results = []

            for col_idx in range(X.shape[1]):
                feature_as_series = X.iloc[:, col_idx]
                feature_weird_char_freq =  feature_as_series.astype(str).apply(self._compute_freq)
                results.append(feature_weird_char_freq.values.reshape(-1, 1))

            return np.concatenate(results, axis=1)

        else:  # Es un array
            return None
        
    def get_feature_names_out(self, input_features=None):
        """
        Devuelve los nombres de las características de salida (requerido por sklearn >= 1.0)
        """
        if input_features is None:
            return np.array([f'weird_char_freq_{i}' for i in range(self.n_features_in_)])
        else:
            return np.array([f'{feature}_weird_char_freq' for feature in input_features])

    def _compute_freq(self,s):
            s = str(s)
            if not s:
                return 0
            total = len(s)
            weird_count = sum(1 for c in s if c in self.weird_chars)
            return weird_count / total if total > 0 else 0
class PandasCompatibleTfidfVectorizer(BaseEstimator, TransformerMixin):
    """
    Wrapper para TfidfVectorizer que es compatible con set_output(transform="pandas")
    """
    def __init__(self, input='content', encoding='utf-8', decode_error='strict',
                 strip_accents=None, lowercase=True, preprocessor=None,
                 tokenizer=None, analyzer='word', stop_words=None,
                 token_pattern=r"(?u)\b\w\w+\b", ngram_range=(1, 1),
                 max_df=1.0, min_df=1, max_features=None, vocabulary=None,
                 binary=False, dtype=None, norm='l2', use_idf=True,
                 smooth_idf=True, sublinear_tf=False,output_transform=None):
        
        # Copiar todos los parámetros
        self.input = input
        self.encoding = encoding
        self.decode_error = decode_error
        self.strip_accents = strip_accents
        self.lowercase = lowercase
        self.preprocessor = preprocessor
        self.tokenizer = tokenizer
        self.analyzer = analyzer
        self.stop_words = stop_words
        self.token_pattern = token_pattern
        self.ngram_range = ngram_range
        self.max_df = max_df
        self.min_df = min_df
        self.max_features = max_features
        self.vocabulary = vocabulary
        self.binary = binary
        self.dtype = dtype
        self.norm = norm
        self.use_idf = use_idf
        self.smooth_idf = smooth_idf
        self.sublinear_tf = sublinear_tf
        
        self.output_transform = output_transform
        self.feature=None
        
        self.vectorizer_ = TfidfVectorizer(
            input=self.input, encoding=self.encoding, decode_error=self.decode_error,
            strip_accents=self.strip_accents, lowercase=self.lowercase,
            preprocessor=self.preprocessor, tokenizer=self.tokenizer,
            analyzer=self.analyzer, stop_words=self.stop_words,
            token_pattern=self.token_pattern, ngram_range=self.ngram_range,
            max_df=self.max_df, min_df=self.min_df, max_features=self.max_features,
            vocabulary=self.vocabulary, binary=self.binary, dtype=self.dtype,
            norm=self.norm, use_idf=self.use_idf, smooth_idf=self.smooth_idf,
            sublinear_tf=self.sublinear_tf
        )
    
    
    def set_output(self, *, transform=None):
        """Implementa set_output para compatibilidad con pandas"""
        self.output_transform = transform  
        return self
    
    def get_params(self, deep=True):
        """Override para incluir output_transform"""
        params = super().get_params(deep=deep)
        params['output_transform'] = getattr(self, 'output_transform', None)
        return params
    
    def set_params(self, **params):
        """Override para manejar output_transform"""
        if 'output_transform' in params:
            self.output_transform = params['output_transform']
        return super().set_params(**params)
    
    
    def fit(self, X, y=None):
        if isinstance(X, pd.DataFrame):
            self.feature=X.columns[0]
            X = X.iloc[:, 0]  # Extraer la Serie de la única columna
        else:
            X = X.apply(lambda row: ' '.join(row.astype(str)), axis=1) # Si hay múltiples columnas, concatenarlas
            self.feature=X.columns[0]
        
        self.vectorizer_.fit(X, y)
        return self
    
    def transform(self, X):
        check_is_fitted(self.vectorizer_)
        
        # Convertir DataFrame/Series a formato adecuado para TfidfVectorizer
        if isinstance(X, pd.DataFrame):
            # Si es DataFrame, tomar la primera columna si solo hay una
            if X.shape[1] == 1:
                self.feature=X.columns[0]
                X = X.iloc[:, 0]
            else:
                # Si hay múltiples columnas, concatenarlas
                X = X.apply(lambda row: ' '.join(row.astype(str)), axis=1)
                self.feature=X.columns[0]
        
        result = self.vectorizer_.transform(X)
        
        if self.output_transform == "pandas":
            if hasattr(result, 'toarray'):
                result = result.toarray()
            feature_names = self.get_feature_names_out(self.vectorizer_.get_feature_names_out())
           
            return pd.DataFrame(result, columns=feature_names)
        
        return result
    
    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)
    
    def get_feature_names_out(self, input_features=None):
        check_is_fitted(self.vectorizer_)
        return [f'TfIdf_{self.feature}_{i}' for i in range(len(input_features))]
        #return self.vectorizer_.get_feature_names_out(input_features)
    
    @property
    def vocabulary_(self):
        return self.vectorizer_.vocabulary_
    
    @property
    def idf_(self):
        return self.vectorizer_.idf_   
class PandasCompatibleCountVectorizer(BaseEstimator, TransformerMixin):
    """
    Wrapper para CountVectorizer que es compatible con set_output(transform="pandas")
    """
    def __init__(self, input='content', encoding='utf-8', decode_error='strict',
                 strip_accents=None, lowercase=True, preprocessor=None,
                 tokenizer=None, stop_words=None, token_pattern=r"(?u)\b\w\w+\b",
                 ngram_range=(1, 1), analyzer='word', max_df=1.0, min_df=1,
                 max_features=None, vocabulary=None, binary=False, dtype=None,output_transform=None):
        
        self.input = input
        self.encoding = encoding
        self.decode_error = decode_error
        self.strip_accents = strip_accents
        self.lowercase = lowercase
        self.preprocessor = preprocessor
        self.tokenizer = tokenizer
        self.stop_words = stop_words
        self.token_pattern = token_pattern
        self.ngram_range = ngram_range
        self.analyzer = analyzer
        self.max_df = max_df
        self.min_df = min_df
        self.max_features = max_features
        self.vocabulary = vocabulary
        self.binary = binary
        self.dtype = dtype
        
        # Configuración de output
        self.output_transform = output_transform
        self.feature=None
        # Crear el CountVectorizer interno
        self.vectorizer_ = CountVectorizer(
            input=self.input,
            encoding=self.encoding,
            decode_error=self.decode_error,
            strip_accents=self.strip_accents,
            lowercase=self.lowercase,
            preprocessor=self.preprocessor,
            tokenizer=self.tokenizer,
            stop_words=self.stop_words,
            token_pattern=self.token_pattern,
            ngram_range=self.ngram_range,
            analyzer=self.analyzer,
            max_df=self.max_df,
            min_df=self.min_df,
            max_features=self.max_features,
            vocabulary=self.vocabulary,
            binary=self.binary,
            dtype=self.dtype
        )
    
    
    def set_output(self, *, transform=None):
        """Implementa set_output para compatibilidad con pandas"""
        self.output_transform = transform  
        return self
    
    def get_params(self, deep=True):
        """Override para incluir output_transform"""
        params = super().get_params(deep=deep)
        params['output_transform'] = getattr(self, 'output_transform', None)
        return params
    
    def set_params(self, **params):
        """Override para manejar output_transform"""
        if 'output_transform' in params:
            self.output_transform = params['output_transform']
        return super().set_params(**params)
    
    def fit(self, X, y=None):
        """Fit del CountVectorizer"""
        
        if isinstance(X, pd.DataFrame):
            self.feature=X.columns[0]
            X = X.iloc[:, 0]  # Extraer la Serie de la única columna
        else:
            X = X.apply(lambda row: ' '.join(row.astype(str)), axis=1) # Si hay múltiples columnas, concatenarlas
            self.feature=X.columns[0]
        
        self.vectorizer_.fit(X, y)
        return self
    
    def transform(self, X):
        """Transform que devuelve DataFrame si se configuró pandas output"""
        check_is_fitted(self.vectorizer_)
        
        # Convertir DataFrame/Series a formato adecuado para CountVectorizer
        if isinstance(X, pd.DataFrame):
            # Si es DataFrame, tomar la primera columna si solo hay una
            if X.shape[1] == 1:
                self.feature=X.columns[0]
                X = X.iloc[:, 0]
            else:
                # Si hay múltiples columnas, concatenarlas
                X = X.apply(lambda row: ' '.join(row.astype(str)), axis=1)
                self.feature=X.columns[0]
        
        # Aplicar la transformación del CountVectorizer
        result = self.vectorizer_.transform(X)
        
        # Si se configuró output pandas, convertir a DataFrame
        if self.output_transform == "pandas":
            if hasattr(result, 'toarray'):
                result = result.toarray()
            
            # Usar los nombres de features del vocabulario del CountVectorizer
            # Esto es mejor que usar números porque son más descriptivos
            #feature_names = self.vectorizer_.get_feature_names_out()
            feature_names = self.get_feature_names_out(self.vectorizer_.get_feature_names_out())
            
            return pd.DataFrame(result, columns=feature_names)
        
        return result
    
    def fit_transform(self, X, y=None):
        """Fit y transform en un solo paso"""
        return self.fit(X, y).transform(X)
    
    def get_feature_names_out(self, input_features=None):
        """Método para compatibilidad con sklearn pipelines"""
        check_is_fitted(self.vectorizer_)
        return [f'CV_{self.feature}_{i}' for i in range(len(input_features))]
        #return self.vectorizer_.get_feature_names_out(input_features)
    
    @property
    def vocabulary_(self):
        """Acceso al vocabulario del CountVectorizer interno"""
        return self.vectorizer_.vocabulary_   
class PandasCompatibleFeatureHasher(BaseEstimator, TransformerMixin):
    def __init__(self, n_features=10, input_type='string', dtype=None, alternate_sign=True, output_transform=None):
        self.n_features = n_features
        self.input_type = input_type
        self.dtype = dtype
        self.alternate_sign = alternate_sign
        self.output_transform = output_transform
        
        self.hasher_ = FeatureHasher(
            n_features=self.n_features,
            input_type=self.input_type,
            dtype=self.dtype,
            alternate_sign=self.alternate_sign
        )
    
    def set_output(self, *, transform=None):
        """Implementa set_output para compatibilidad con pandas"""
        self.output_transform = transform  
        return self
    
    def get_params(self, deep=True):
        """Override para incluir output_transform"""
        params = super().get_params(deep=deep)
        params['output_transform'] = getattr(self, 'output_transform', None)
        return params
    
    def set_params(self, **params):
        """Override para manejar output_transform"""
        if 'output_transform' in params:
            self.output_transform = params['output_transform']
        return super().set_params(**params)
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        # Convertir diferentes tipos de input a formato adecuado para FeatureHasher
        if isinstance(X, pd.DataFrame):
            # Si es DataFrame, tomar la primera columna y convertir a array
            if X.shape[1] == 1:
                X = X.iloc[:, 0].values
            else:
                # Si tiene múltiples columnas, concatenarlas
                X = X.apply(lambda row: ' '.join(row.astype(str)), axis=1).values
        elif isinstance(X, pd.Series):
            X = X.values
        elif hasattr(X, 'flatten'):
            X = X.flatten()
        
        # FeatureHasher espera un iterable de strings
        result = self.hasher_.transform(X)
        
        if self.output_transform == "pandas":
            if hasattr(result, 'toarray'):
                result = result.toarray()
            column_names = [f"hash_{i}" for i in range(result.shape[1])]
            return pd.DataFrame(result, columns=column_names)
        
        return result
class PandasCompatibleHashingVectorizer(BaseEstimator, TransformerMixin):
    """
    Wrapper para HashingVectorizer que es compatible con set_output(transform="pandas")
    """
    def __init__(self, n_features=2**20, input='content', encoding='utf-8',
                 decode_error='strict', strip_accents=None, lowercase=True,
                 preprocessor=None, tokenizer=None, stop_words=None,
                 token_pattern=r"(?u)\b\w\w+\b", ngram_range=(1, 1),
                 analyzer='word', norm='l2', alternate_sign=True,
                 dtype=None, output_transform=None):
        self.n_features = n_features
        self.input = input
        self.encoding = encoding
        self.decode_error = decode_error
        self.strip_accents = strip_accents
        self.lowercase = lowercase
        self.preprocessor = preprocessor
        self.tokenizer = tokenizer
        self.stop_words = stop_words
        self.token_pattern = token_pattern
        self.ngram_range = ngram_range
        self.analyzer = analyzer
        self.norm = norm
        self.alternate_sign = alternate_sign
        self.dtype = dtype
        
        # Configuración de output
        self.output_transform = output_transform
        self.feature=None

        # Crear el HashingVectorizer interno
        self.vectorizer_ = HashingVectorizer(
            n_features=self.n_features,
            input=self.input,
            encoding=self.encoding,
            decode_error=self.decode_error,
            strip_accents=self.strip_accents,
            lowercase=self.lowercase,
            preprocessor=self.preprocessor,
            tokenizer=self.tokenizer,
            stop_words=self.stop_words,
            token_pattern=self.token_pattern,
            ngram_range=self.ngram_range,
            analyzer=self.analyzer,
            norm=self.norm,
            alternate_sign=self.alternate_sign,
            dtype=self.dtype
        )
    
    def set_output(self, *, transform=None):
        """Implementa set_output para compatibilidad con pandas"""
        self.output_transform = transform  
        return self
    
    def get_params(self, deep=True):
        """Override para incluir output_transform"""
        params = super().get_params(deep=deep)
        params['output_transform'] = getattr(self, 'output_transform', None)
        return params
    
    def set_params(self, **params):
        """Override para manejar output_transform"""
        if 'output_transform' in params:
            self.output_transform = params['output_transform']
        return super().set_params(**params)
    
    def fit(self, X, y=None):
        """Fit del HashingVectorizer (no hace nada pero es necesario)"""
        self.feature=X.columns[0]
        self.vectorizer_.fit(X, y)
        return self
    
    def transform(self, X):
        """Transform que devuelve DataFrame si se configuró pandas output"""
        feature_name=''
        # Convertir DataFrame/Series a formato adecuado para HashingVectorizer
        if isinstance(X, pd.DataFrame):
            
            # Si es DataFrame, tomar la primera columna si solo hay una
            if X.shape[1] == 1:
                feature_name=X.columns[0]
                X = X.iloc[:, 0]
                
            else:
                # Si hay múltiples columnas, concatenarlas
                feature_name=X.columns.apply(lambda col: ' '.join(col.astype(str)), axis=1 )
                X = X.apply(lambda row: ' '.join(row.astype(str)), axis=1)
        
        # Aplicar la transformación del HashingVectorizer
        result = self.vectorizer_.transform(X)
        
        # Si se configuró output pandas, convertir a DataFrame
        
        if self.output_transform == "pandas":
            if hasattr(result, 'toarray'):
                result = result.toarray()
            
            # Crear nombres de columnas
            n_cols = result.shape[1]
            column_names = [f"hash_{feature_name}_{i}" for i in range(n_cols)]
            
            return pd.DataFrame(result, columns=column_names)
        
        return result
    
    def fit_transform(self, X, y=None):
        """Fit y transform en un solo paso"""
        return self.fit(X, y).transform(X)

def safe_log1p_zero(x):
    if not x.mode().empty:
        mode_value = x.mode().iloc[0]
    else:
        mode_value = 0 

    x = x.fillna(mode_value)
    x = np.where(x <= -1, 0, x)
    return np.log1p(x)

log_transformer = FunctionTransformer(safe_log1p_zero, feature_names_out='one-to-one')

COUNTRY_CODE_PIPE = Pipeline([
        ("country_code", IpAddressToISOCountryCodeTransformer(IPINFO_TOKEN,CACHE_FILE)),
        ("HashingVectorizer", PandasCompatibleHashingVectorizer(n_features=10, alternate_sign=False,output_transform="pandas"))
    ])

NORMALIZED_DELTATIME_BETWEEN_REQUEST_PIPE = Pipeline([
    ("delta",DeltaTimeBetweenDatetimesTransformer()),
    ("mixmax", MinMaxScaler())
])

NORMALIZED_LENGTH_PIPE = Pipeline([
    ("len",CalculateLengthTransformer()),
    ("log", log_transformer),
    ("mixmax", MinMaxScaler())
])

NORMALIZED_NUM = Pipeline([
    ("log", log_transformer),
    ("mixmax", MinMaxScaler())
])

NORMALIZED_DELTATIME_BETWEEN_REQUEST_IN_SESSION_PIPE = Pipeline([
    ("delta_session",GetInfoSessionTransformer(SESSION_MIN)),
    ("log", log_transformer),
    ("mixmax", MinMaxScaler())
])