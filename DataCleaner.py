
"""
DataCleaner.py - Clase para la limpieza y preprocesamiento de datos

Esta clase proporciona métodos para limpiar y preparar datos en DataFrames de pandas
para su uso en análisis o modelos de aprendizaje automático.

Autor: Equipo de trabajo formado por Bryan Silva, Julio Gonzalez, Armando Sánchez, Salvador Galiano
Fecha: 01/05/2025
"""

import pandas as pd
import numpy as np


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
        # se verifica que el parámetro sea un DataFrame de pandas
        if not isinstance(data, pd.DataFrame):
            raise TypeError("El parámetro 'data' debe ser pandas.DataFrame")
        
        # se realiza una copia del DataFrame para no modificar el original
        df_copy = data.copy()
        
        # se elimina las filas con al menos un valor faltante
        df_clean = df_copy.dropna()
        
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
