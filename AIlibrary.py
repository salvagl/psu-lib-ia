import os
import numpy as np
import pickle
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from abc import ABC, abstractmethod

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

#**********************************************************************************************
#                                       MODELOS IMPLEMENTADOS 
#**********************************************************************************************
# Implementación para Isolation Forest
class IsolationForestModel(AIModelInterface):
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=2)
        
    def train_model(self, data, trainParams):
        # Escalar datos
        scaled_data = self.scaler.fit_transform(data)
        
        # Entrenar modelo
        self.model = IsolationForest(
            contamination=trainParams.get('contamination', 0.1),
            n_estimators=trainParams.get('n_estimators', 100),
            random_state=42
        )
        
        predictions = self.model.fit_predict(scaled_data)
        
        # PCA para visualización
        pca_data = self.pca.fit_transform(scaled_data)
        
        # Calcular métricas
        anomaly_count = np.sum(predictions == -1)
        normal_count = np.sum(predictions == 1)
        
        confusion_matrix = {
            'anomalies': anomaly_count,
            'normal': normal_count,
            'contamination_rate': anomaly_count / len(predictions)
        }
        
        return confusion_matrix, predictions, pca_data
    
    def save_model(self, modelName):
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'pca': self.pca
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
        self.scaler = model_data['scaler']
        self.pca = model_data['pca']
        return self.model
    
    def test_model(self, data):
        if self.model is None:
            raise ValueError("Modelo no entrenado")
        
        scaled_data = self.scaler.transform(data)
        predictions = self.model.predict(scaled_data)
        scores = self.model.decision_function(scaled_data)
        pca_data = self.pca.transform(scaled_data)
        
        return predictions, scores, pca_data
# Implementación para K-Means
class KMeansModel(AIModelInterface):
    def __init__(self):
        self.model = None
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=2)
        
    def train_model(self, data, trainParams):
        # Escalar datos
        scaled_data = self.scaler.fit_transform(data)
        
        # Entrenar modelo
        k = trainParams.get('n_clusters', 8)
        self.model = KMeans(
            n_clusters=k,
            random_state=42,
            n_init=10
        )
        
        cluster_labels = self.model.fit_predict(scaled_data)
        
        # PCA para visualización
        pca_data = self.pca.fit_transform(scaled_data)
        
        # Calcular métricas
        inertia = self.model.inertia_
        silhouette_avg = silhouette_score(scaled_data, cluster_labels)
        
        # Detectar anomalías basándose en distancia a centroides
        distances = self.model.transform(scaled_data)
        min_distances = np.min(distances, axis=1)
        threshold = np.percentile(min_distances, 90)  # Top 10% como anomalías
        anomalies = min_distances > threshold
        
        confusion_matrix = {
            'inertia': inertia,
            'silhouette_score': silhouette_avg,
            'n_clusters': k,
            'anomalies': np.sum(anomalies),
            'normal': len(anomalies) - np.sum(anomalies)
        }
        
        return confusion_matrix, cluster_labels, pca_data, anomalies, min_distances
    
    def save_model(self, modelName):
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'pca': self.pca
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
        self.scaler = model_data['scaler']
        self.pca = model_data['pca']
        return self.model
    
    def test_model(self, data):
        if self.model is None:
            raise ValueError("Modelo no entrenado")
        
        scaled_data = self.scaler.transform(data)
        cluster_labels = self.model.predict(scaled_data)
        pca_data = self.pca.transform(scaled_data)
        
        # Calcular distancias para detección de anomalías
        distances = self.model.transform(scaled_data)
        min_distances = np.min(distances, axis=1)
        
        return cluster_labels, min_distances, pca_data
