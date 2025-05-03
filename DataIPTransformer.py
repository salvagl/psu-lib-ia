import pandas as pd
import requests
import time
import json
import os

class DataIPTransformer:
    """
    Clase para enriquecer un DataFrame de pandas con el código ISO del país
    de origen de las IPs usando la API Lite de IPinfo y un sistema de caché.

    Ejemplo de uso:
        transformer = DataIPTransformer(token="TU_TOKEN", cache_file="cache.json")
        df = pd.DataFrame({'ip': ['8.8.8.8', '1.1.1.1']})
        df_transformed = transformer.transform(df, ip_col='ip')
        transformer.save_cache()
    """

    def __init__(self, token: str, cache_file: str = None, delay: float = 0.1):
        """
        Inicializa el transformer.

        :param token: Token de API de IPinfo
        :param cache_file: Ruta al fichero JSON para persistir cache (opcional)
        :param delay: Segundos a esperar entre peticiones 
        """
        self.token = token
        self.cache_file = cache_file
        self.delay = delay
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

    def transform(self, df: pd.DataFrame, ip_col: str = 'ip', new_col: str = 'country_code') -> pd.DataFrame:
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
