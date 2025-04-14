# AIlibrary.py

from abc import ABC, abstractmethod

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


