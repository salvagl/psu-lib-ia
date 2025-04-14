import AIModelInterface

class BasicAIModel(AIModelInterface):
    def __init__(self):
        self.model = None

    def train_model(self, data, trainParams):
        # TODO: implementar lógica de entrenamiento
        pass

    def save_model(self, modelName):
        # TODO: implementar lógica de guardado
        pass

    def load_model(self, modelName):
        # TODO: implementar lógica de carga
        pass

    def test_model(self, data):
        # TODO: implementar lógica de testeo
        pass
