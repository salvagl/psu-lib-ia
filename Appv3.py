import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
import os
import tempfile
from AIlibrary import AIModelInterface, KMeansModel,IsolationForestModel,DataTransformer,DataCleaner,DataCLFReader,IPINFO_TOKEN, CACHE_FILE,SESSION_MIN

#**********************************************************************************************
#                         FUNC. AUXILIARES DE LOS MODELOS
#**********************************************************************************************
# Función para calcular método del codo para K-Means
def calculate_elbow_method(data, max_k=10):
    scaled_data = StandardScaler().fit_transform(data)
    inertias = []
    k_range = range(1, max_k + 1)
    
    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        kmeans.fit(scaled_data)
        inertias.append(kmeans.inertia_)
    
    return k_range, inertias
# Funciones de limpieza y transformación de los datos originales
def clean_data(data:pd.DataFrame)->pd.DataFrame:      
    # - eliminar filas con datos faltantes: en el proceso de lectura ya se realiza.
    cleaner = DataCleaner()
    #print(data.shape)
    cleaned_df = cleaner.delete_rows_with_faulting_category(data)
    #print(cleaned_df.shape)
    return cleaned_df
def transform_data(data:pd.DataFrame)->pd.DataFrame:
   transformer = DataTransformer(token=IPINFO_TOKEN,cache_file=CACHE_FILE,session_minutes=SESSION_MIN)
   cleaner = DataCleaner()
   
   #Transformación de datos:
   # - añadir country_code: código ISO de pais en función de la IP
   # - añadir datetime_delta_ms: tiempo entre requests
   # - añadir session_global_id: identificador de sesiones en un rango de tiempo. (peticiones desde una misma IP en un rango definido)
   # - añadir datetime_delta_ms_in_session: tiempo entre request de una misma sesión.
   # - añadir request_len y referer_len : longitud de la petición y el referer de la petición.
   # - añadir flag que indica si la request contiene comandos típicos de SO que pueden indicar ataque.
   # - añadir flag que indica si la request contiene caracteres Hexadecimales
   # - añadir columna con conteo de caracteres extraños para una URL
   transformed_df = transformer.transform_add_countrycode(data,"client","country_code" )
   transformed_df = transformer.transform_add_datetime_delta_between_requests(transformed_df)
   transformed_df = transformer.transform_add_session_info (transformed_df)    
   transformed_df = transformer.transform_add_length_columns (transformed_df,['request','referer'])
   transformed_df = transformer.transform_add_os_command_flag(transformed_df,['raw_request'])    
   transformed_df = transformer.transform_add_hex_flag(transformed_df,['raw_request'])
   transformed_df = transformer.transform_add_weird_char_freq(transformed_df,['raw_request'])

    # - eliminar filas con datos faltantes: en el proceso de lectura ya se realiza.
    # - eliminar userid
    # - eliminar client (ip) 
   cleaned_df = cleaner.delete_rows_with_faulting_category(transformed_df)
   cleaned_df = cleaner.delete_column (cleaned_df,'userid')   
   cleaned_df = cleaner.delete_column (cleaned_df,'client')

   #Normalizar valores numéricos:
   normalized_df = transformer.transform_normalize (cleaned_df,['datetime_delta_ms','datetime_delta_ms_in_session','size_in_bytes','raw_request_weird_char_freq'])   
   
   #OneHotEncoder sobre categoricas de baja cardinalidad: +interpretabilidad
   normalized_df = transformer.transform_one_hot_encoder(normalized_df,['method','status'])
   
   #FeatureHashing sobre categoricas de cardinalidad media: -interpretabilidad
   normalized_df = transformer.transform_feature_hashing(normalized_df,'country_code')

   #Tokenizacion+Vectorización categóricas/texto alta cardinalidad
   sparseMatrix_user_agent, v1 = transformer.transform_vectorize_categorical_text(normalized_df,'user_agent')
   sparseMatrix_request, v2 = transformer.transform_vectorize_url(normalized_df,'request')
   sparseMatrix_referer, v3 = transformer.transform_vectorize_url(normalized_df,'referer')
   sparseMatrix_rawRequest, v4 = transformer.transform_vectorize_raw_request(normalized_df,'raw_request') 
   final_df = transformer.transform_combine_numeric_and_sparse(normalized_df,[sparseMatrix_rawRequest,sparseMatrix_user_agent,sparseMatrix_referer,sparseMatrix_request])

   #Establece todos los nombres de columnas de tipo String.
   final_df.columns = final_df.columns.astype(str) 
   return final_df

#**********************************************************************************************
#                              FUNCIONES AUX. DE LA UI
#********************************************************************************************** 
#Funciones de visualización en UI
def render_dataframe_sample(df:pd.DataFrame):
    print(f"- [render_dataframe_sample] printing loaded data (procesed={st.session_state["data_processed_flag"]})")
    
    #solo mostramos preview de un 10% del dataframe
    subset_size = int(len(df) * 0.10)
    df_dense_subset = df.head(subset_size).copy()
    
    # Convierte columnas sparse a densas
    for col in df_dense_subset.columns:
        if  isinstance(df_dense_subset[col].dtype, pd.SparseDtype):
            df_dense_subset[col] = df_dense_subset[col].sparse.to_dense()

    # Información de los datos sin procesar
    col1, col2 = st.columns(2)
    with col1:
        st.subheader(f"Vista Previa de los Datos (sampling: 10%)")
        st.dataframe(df_dense_subset, height=315)
    
    with col2:
        st.subheader("Estadísticas Básicas (datos numéricos)")
        st.dataframe(df.describe())
def render_anomalies_grid(original_df, predictions):
    """
    Muestra los registros originales donde se detectaron anomalías
    
    Args:
        original_df: DataFrame original con todos los datos
        predictions: Array con las predicciones (-1 para anomalías, 1 para normales)
    """
    print("- [render_anomalies_grid] UI printing Grid with anomalies")
    # Crear una copia del DataFrame original
    df_with_predictions = original_df.copy()
    
    # Agregar columna con las predicciones
    df_with_predictions['Anomaly'] = predictions
    df_with_predictions['Is_Anomaly'] = predictions == -1
    
    # Filtrar solo las anomalías
    anomalies_df = df_with_predictions[df_with_predictions['Is_Anomaly'] == True].copy()
    
    # Eliminar las columnas auxiliares para mostrar solo datos originales
    anomalies_display = anomalies_df.drop(['Anomaly', 'Is_Anomaly'], axis=1)
    
    # Mostrar información general
    st.subheader("📊 Resumen de Anomalías Detectadas")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total de Registros", len(original_df))
    with col2:
        st.metric("Anomalías Detectadas", len(anomalies_df))
    with col3:
        st.metric("Porcentaje de Anomalías", f"{len(anomalies_df)/len(original_df)*100:.2f}%")
    
    # Mostrar el grid con las anomalías
    st.subheader("🚨 Registros con Anomalías Detectadas")
    
    if len(anomalies_df) > 0:
        # Opción 1: Grid básico con st.dataframe
        st.dataframe(
            anomalies_display,
            use_container_width=True,
            height=400
        )
        
        # Opción 2: Grid editable con st.data_editor (comentado)
        # st.data_editor(
        #     anomalies_display,
        #     use_container_width=True,
        #     height=400,
        #     disabled=True  # Solo lectura
        # )
        
        # Botón para descargar las anomalías
        csv_data = anomalies_display.to_csv(index=False)
        st.download_button(
            label="📥 Descargar Anomalías (CSV)",
            data=csv_data,
            file_name="anomalias_detectadas.csv",
            mime="text/csv"
        )
        
    else:
        st.info("No se detectaron anomalías en los datos.")
def clean_ui():
    print('- [clean_ui] Atención: LIMPIANDO DATOS DE SESION')
    st.session_state['data_processed_flag']=False
    st.session_state.pop('model',None)
    st.session_state.pop('result_ready',None)
    st.session_state.pop('ui_mode',None)
def select_ui_mode(selected:str):
    """
    Selecciona entre modo ENTRENAMIENTO o MODO PREDICCIÓN
    """
    print(f"-[select_ui_mode] Selected UI Mode: {selected}")
    st.session_state.pop('model',None)
    st.session_state["result_ready"]=False
    st.session_state.ui_mode=selected
def load_pretrained_model(selected_model:str, algorithm:str):
    """
    Carga un modelo ya pre-entrenado. 
    """
    try:
        if algorithm == "Isolation Forest":
            model = IsolationForestModel()
            model.load_model(selected_model.replace('_isolation_forest', ''))
        else:
            model = KMeansModel()
            model.load_model(selected_model.replace('_kmeans', ''))
        
        st.session_state['model'] = model
        st.success("Modelo cargado exitosamente!")
    except Exception as e:
        st.error(f"Error al cargar: {str(e)}")

def render_training_results_graphic(uiMode:str,algorithm:str):
    print(f"- [render_training_results_graphic] UI Mode: {uiMode}-{algorithm}")
    st.markdown("---")
    st.header("Resultados del Entrenamiento")

    # Métricas
    col1, col2, col3 = st.columns(3)
    confusion_matrix = st.session_state['confusion_matrix']
    
    if algorithm == "Isolation Forest":
        with col1:
            st.metric("Anomalías Detectadas", confusion_matrix['anomalies'])
        with col2:
            st.metric("Registros Normales", confusion_matrix['normal'])
        with col3:
            st.metric("Tasa de Contaminación", f"{confusion_matrix['contamination_rate']:.2%}")
            
    else:  # K-Means
        with col1:
            st.metric("Inercia", f"{confusion_matrix['inertia']:.2f}")
        with col2:
            st.metric("Silhouette Score", f"{confusion_matrix['silhouette_score']:.3f}")
        with col3:
            st.metric("Número de Clusters", confusion_matrix['n_clusters'])
    
    # Visualizaciones
    st.subheader("Visualizaciones")
    
    if algorithm == "Isolation Forest":
        predictions = st.session_state['predictions']
        pca_data = st.session_state['pca_data']
        
        # Crear gráfico de dispersión con PCA
        fig = px.scatter(
            x=pca_data[:, 0],
            y=pca_data[:, 1],
            color=['Anomalía' if p == -1 else 'Normal' for p in predictions],
            title="Detección de Anomalías (Isolation Forest) - Vista PCA",
            labels={'x': 'Primera Componente Principal', 'y': 'Segunda Componente Principal'},
            color_discrete_map={'Normal': 'blue', 'Anomalía': 'red'}
        )
        
        fig.update_traces(marker=dict(size=8, opacity=0.7))
        fig.update_layout(template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

        df_original = st.session_state['data']

        render_anomalies_grid(df_original,predictions)
        
    else:  # K-Means
        cluster_labels = st.session_state['cluster_labels']
        pca_data = st.session_state['pca_data']
        anomalies = st.session_state['anomalies']
        distances = st.session_state['distances']
        
        # Gráfico de clusters
        fig1 = px.scatter(
            x=pca_data[:, 0],
            y=pca_data[:, 1],
            color=cluster_labels.astype(str),
            title="Clustering K-Means - Vista PCA",
            labels={'x': 'Primera Componente Principal', 'y': 'Segunda Componente Principal'},
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig1.update_traces(marker=dict(size=8, opacity=0.7))
        fig1.update_layout(template="plotly_white")
        st.plotly_chart(fig1, use_container_width=True)
        
        # Gráfico de anomalías basado en distancia
        fig2 = px.scatter(
            x=pca_data[:, 0],
            y=pca_data[:, 1],
            color=['Anomalía' if a else 'Normal' for a in anomalies],
            title="Detección de Anomalías basada en Distancia a Centroides",
            labels={'x': 'Primera Componente Principal', 'y': 'Segunda Componente Principal'},
            color_discrete_map={'Normal': 'blue', 'Anomalía': 'red'}
        )
        
        fig2.update_traces(marker=dict(size=8, opacity=0.7))
        fig2.update_layout(template="plotly_white")
        st.plotly_chart(fig2, use_container_width=True)
        
        # Histograma de distancias
        fig3 = px.histogram(
            x=distances,
            nbins=50,
            title="Distribución de Distancias a Centroides",
            labels={'x': 'Distancia Mínima al Centroide', 'y': 'Frecuencia'}
        )
        fig3.update_layout(template="plotly_white")
        st.plotly_chart(fig3, use_container_width=True)

def render_prediction_results_graphic(uiMode:str,algorithm:str):
    print(f"- [render_prediction_results_graphic] UI Mode: {uiMode}-{algorithm}")
    st.markdown("---")
    st.header("Resultado de Predicción")

    # Visualizaciones
    st.subheader("Visualizaciones")
    
    if algorithm == "Isolation Forest":
        predictions = st.session_state['predictions']
        pca_data = st.session_state['pca_data']
        
        # Crear gráfico de dispersión con PCA
        fig = px.scatter(
            x=pca_data[:, 0],
            y=pca_data[:, 1],
            color=['Anomalía' if p == -1 else 'Normal' for p in predictions],
            title="Detección de Anomalías (Isolation Forest) - Vista PCA",
            labels={'x': 'Primera Componente Principal', 'y': 'Segunda Componente Principal'},
            color_discrete_map={'Normal': 'blue', 'Anomalía': 'red'}
        )
        
        fig.update_traces(marker=dict(size=8, opacity=0.7))
        fig.update_layout(template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

        df_original = st.session_state['data']

        render_anomalies_grid(df_original,predictions)
        
    else:  # K-Means
        cluster_labels = st.session_state['cluster_labels']
        pca_data = st.session_state['pca_data']
        anomalies = st.session_state['anomalies']
        distances = st.session_state['distances']
        
        # Gráfico de clusters
        fig1 = px.scatter(
            x=pca_data[:, 0],
            y=pca_data[:, 1],
            color=cluster_labels.astype(str),
            title="Clustering K-Means - Vista PCA",
            labels={'x': 'Primera Componente Principal', 'y': 'Segunda Componente Principal'},
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig1.update_traces(marker=dict(size=8, opacity=0.7))
        fig1.update_layout(template="plotly_white")
        st.plotly_chart(fig1, use_container_width=True)
        
        # Gráfico de anomalías basado en distancia
        fig2 = px.scatter(
            x=pca_data[:, 0],
            y=pca_data[:, 1],
            color=['Anomalía' if a else 'Normal' for a in anomalies],
            title="Detección de Anomalías basada en Distancia a Centroides",
            labels={'x': 'Primera Componente Principal', 'y': 'Segunda Componente Principal'},
            color_discrete_map={'Normal': 'blue', 'Anomalía': 'red'}
        )
        
        fig2.update_traces(marker=dict(size=8, opacity=0.7))
        fig2.update_layout(template="plotly_white")
        st.plotly_chart(fig2, use_container_width=True)
        
        # Histograma de distancias
        fig3 = px.histogram(
            x=distances,
            nbins=50,
            title="Distribución de Distancias a Centroides",
            labels={'x': 'Distancia Mínima al Centroide', 'y': 'Frecuencia'}
        )
        fig3.update_layout(template="plotly_white")
        st.plotly_chart(fig3, use_container_width=True)

def render_training_ui_mode() -> str:
    col1, col2 = st.columns(2)

    with col1:
        # Selección de algoritmo
        algorithm = st.selectbox(
            "Seleccionar Algoritmo",
            ["Isolation Forest", "K-Means Clustering"]
        )
        # Botón para entrenar modelo
        if st.button("Entrenar Modelo", type="primary") and 'data' in st.session_state:
            with st.spinner("Entrenando modelo..."):
                
                df=st.session_state['data']
                
                # Seleccionar solo columnas numéricas
                #numeric_df = df.select_dtypes(include=[np.number])
                train_params = st.session_state.train_params
                print ("********** train_params*************")
                print (st.session_state.train_params)

                if algorithm == "Isolation Forest":
                    model = IsolationForestModel()
                    confusion_matrix, predictions, pca_data = model.train_model(df, train_params)
                    
                    st.session_state['model'] = model
                    st.session_state['predictions'] = predictions
                    st.session_state['pca_data'] = pca_data
                    st.session_state['confusion_matrix'] = confusion_matrix
                    
                else:  # K-Means
                    model = KMeansModel()
                    confusion_matrix, cluster_labels, pca_data, anomalies, distances = model.train_model(df, train_params)
                    
                    st.session_state['model'] = model
                    st.session_state['cluster_labels'] = cluster_labels
                    st.session_state['pca_data'] = pca_data
                    st.session_state['confusion_matrix'] = confusion_matrix
                    st.session_state['anomalies'] = anomalies
                    st.session_state['distances'] = distances
            
            st.success("¡Modelo entrenado exitosamente!")
            st.session_state["result_ready"]=True
    with col2:
        # Configuración específica del algoritmo
        st.text("Hiperparámetros")
    
        if algorithm == "Isolation Forest":
            contamination = st.slider(
                "Contaminación (% de anomalías esperadas)",
                min_value=0.001,
                max_value=0.5,
                value=0.1,
                step=0.001
            )
        
            n_estimators = st.slider(
                "Número de Estimadores",
                min_value=50,
                max_value=300,
                value=100,
                step=10
            )
        
            train_params = {
                'contamination': contamination,
                'n_estimators': n_estimators
            }
            
        
        else:  # K-Means
            # Mostrar método del codo
            if st.button("Calcular Método del Codo"):
                with st.spinner("Calculando método del codo..."):
                    k_range, inertias = calculate_elbow_method(df.select_dtypes(include=[np.number]))
                    
                    fig_elbow = go.Figure()
                    fig_elbow.add_trace(go.Scatter(
                        x=list(k_range),
                        y=inertias,
                        mode='lines+markers',
                        name='Inercia',
                        line=dict(color='blue', width=2),
                        marker=dict(size=8)
                    ))
                    
                    fig_elbow.update_layout(
                        title="Método del Codo para K-Means",
                        xaxis_title="Número de Clusters (K)",
                        yaxis_title="Inercia",
                        template="plotly_white"
                    )
                    
                    st.plotly_chart(fig_elbow, use_container_width=True)
            
            n_clusters = st.slider(
                "Número de Clusters (K)",
                min_value=2,
                max_value=15,
                value=8,
                step=1
            )
            
            train_params = {
                'n_clusters': n_clusters
            }

    st.session_state.train_params = train_params
    return algorithm

def render_model_saving_management():
    if 'model' in st.session_state:

        st.subheader("Gestión del Modelo")
        
        model_name = st.text_input("Nombre del modelo", value="mi_modelo")
        if st.button("Guardar Modelo"):
            try:
                path = st.session_state.model.save_model(model_name)
                st.success(f"Modelo guardado en: {path}")
            except Exception as e:
                st.error(f"Error al guardar: {str(e)}")

def render_predict_ui_mode()-> tuple [AIModelInterface, str]:
    model = None
    algorithm=None
    # Listar modelos guardados
    if os.path.exists('models'):
        saved_models = [f.replace('.pkl', '') for f in os.listdir('models') if f.endswith('.pkl')]
        col1, col2=st.columns(2)
        if saved_models:
            selected_model = col1.selectbox("Cargar modelo guardado", saved_models)
            
            if selected_model.__contains__("_isolation_forest"):
                algorithm="Isolation Forest"
            else:
                algorithm="KMeans"
            if col1.button("Cargar Modelo Pre-entrenado"):
                print("- Botón Cargar Modelo Pre-entrenado renderizado")
                load_pretrained_model(selected_model,algorithm)
               
                if algorithm is not None and algorithm=="Isolation Forest":
                    render_isolation_forest_model_loaded_info(col2,st.session_state.model)
                else:
                    render_K_means_model_loaded_info(col2,st.session_state.model)
            
            if "model" in st.session_state:
                if st.button("Realizar Predicción", type="primary"):
                    print("- Botón predecir renderizado")
  
                    df=st.session_state.data
                    model=st.session_state.model
                    
                    # Seleccionar solo columnas numéricas
                    # numeric_df = df.select_dtypes(include=[np.number])
                    
                    if algorithm == "Isolation Forest":
                        predictions,score, pca_data = model.test_model(df)
                        
                        st.session_state['predictions'] = predictions
                        st.session_state['pca_data'] = pca_data
                        st.session_state['score'] = score
                        
                        
                    else:  # K-Means                       
                        #confusion_matrix, cluster_labels, pca_data, anomalies, distances = model.train_model(numeric_df, train_params)
                        cluster_labels, min_distances, pca_data = model.test_model(df)
                        
                        st.session_state['cluster_labels'] = cluster_labels
                        st.session_state['pca_data'] = pca_data
                        st.session_state['distances'] = min_distances
                        #st.session_state['anomalies'] = anomalies
                    
                    st.session_state["result_ready"]=True
                    st.success("Predicción realizada exitosamente!")
            else:
                st.info("🔽 No hay ningún modelo cargado en memoria. Por favor, selecciona una opción de la lista y carga un modelo para realizar la predicción")
    return model,algorithm

def render_isolation_forest_model_loaded_info(st_element, model:AIModelInterface ):
    print("*********render_isolation_forest_model_loaded_info*************")
    st_element.text("")
    st_element.text("Parámetros de entrenamiento: ISOLATION FOREST")
    st_element.text(f"- n_estimators: {model.model.n_estimators}")
    st_element.text(f"- contamination: {model.model.contamination}")
    st_element.text(f"- n_features_in: {model.model.n_features_in_}")
    st_element.text(f"- max_features per tree: {model.model.max_features}")

def render_K_means_model_loaded_info(st_element, model:AIModelInterface ):
    print("*********render_K_means_model_loaded_info*************")
    st_element.text("")
    st_element.text("Parámetros de entrenamiento: K-MEAN")
    st_element.text(f"- n_clusters: {model.model.n_clusters}")
    st_element.text(f"- max_iter: {model.model.max_iter}")
    st_element.text(f"- algorithm: {model.model.algorithm}")
    st_element.text(f"- tol: {model.model.tol}")

def initialize_session_state():
    st.session_state.render_count+=1
    #- data_processed_flag: controla que la información sumarizada del dataset cambie entre 
    #                       datos originales o datos transformados/procesados
    if 'data_processed_flag' not in st.session_state:
        st.session_state['data_processed_flag'] = False

    #- ui_mode: Posibles valores TRAIN/PREDICT o None
    if 'ui_mode' not in st.session_state:
        st.session_state['ui_mode'] = None
    #- result_ready: indica si existen en sesión resultados preparados para mostrar ya sea por un entrenamiento nuevo
    #                o por una predicción   
    if 'result_ready' not in st.session_state:
        st.session_state["result_ready"]=False

    #- inicializa el estado del expander con la info sobre nosotros y los modelos implementados la primera vez que se carga la app
    if "expander_open" not in st.session_state:
        st.session_state.expander_open = True
    else:
        st.session_state.expander_open = False  # en las siguientes interacciones, aparece contraído

    
    return

#**********************************************************************************************
#                                RENDERIZADO DE LA UI
#**********************************************************************************************
if 'render_count' not in st.session_state:
    st.session_state.render_count = 0
print("...................................................................................")
print(f"                               RENDERING:UI [{st.session_state.render_count}]")
print("...................................................................................")
#VAR. GLOBALES:
TRAIN_MODE = "TRAIN_MODE"
PREDICT_MODE = "PREDICT_MODE"

#Inicialización de FLAGS y var.  que controlan la UI:
initialize_session_state()

# Configuración de la página
st.set_page_config(
    page_title="Detección de Anomalías en Logs",
    page_icon="🔍",
    layout="wide"
)

# Título principal
st.title(f"Detección de Anomalías en WebServer Logs")
st.markdown("---")

# Carga de datos
st.sidebar.header("Cargar Datos")
uploaded_file = st.sidebar.file_uploader("Subir archivo de logs (.log) en formato CLF", type=['log'],
                                         help="Selecciona un fichero log en formato CLF",
                                        )

# Gestión de la lectura de datos del fichero de logs seleccionado en formato CLF
if uploaded_file is not None:
    # si hay seleccionado un fichero en el uploader y NO ha sido cargado en sesión O 
    # el fichero seleccionado en el uploader es distinto del fichero en sesión >> realizar NUEVA lectura.
    if not "file_read" in st.session_state or st.session_state.file_read != uploaded_file.name:
        try:
            # Crear archivo temporal para guardar el log
            with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.log') as temp_file:
                temp_file.write(uploaded_file.getvalue())
                temp_logfile_path = temp_file.name
            
            # Crear directorios temporales para salida y errores
            with tempfile.TemporaryDirectory() as temp_output_dir:
                temp_errors_file = os.path.join(temp_output_dir, 'parsing_errors.txt')
                
                # Instanciar el lector CLF y procesar el archivo
                clf_reader = DataCLFReader()
                
                with st.spinner("Leyendo archivo de logs CLF..."):
                    df = clf_reader.logs_to_df(
                        logfile=temp_logfile_path,
                        output_dir=temp_output_dir,
                        errors_file="parsing_errors.txt"
                    )
                
                    print('DF recien leido (shape):')
                    print(df.shape)
                    
                # Limpiar archivo temporal
                os.unlink(temp_logfile_path)
                
                if df is not None and not df.empty:
                    st.session_state['data'] = df
                    st.sidebar.success(f"Registros de logs cargados: {len(df)} ")
                    
                    # Mostrar información sobre errores de parsing si existen
                    if os.path.exists(temp_errors_file):
                        try:
                            with open(temp_errors_file, 'r') as f:
                                errors_content = f.read().strip()
                            if errors_content:
                                st.sidebar.warning(f"Se encontraron algunos errores de parsing. Revisa el archivo de errores.")
                                with st.sidebar.expander("Ver errores de parsing"):
                                    st.text(errors_content)
                        except:
                            pass
                    #En cada lectura correcta de fichero de logs se deben resetear 
                    #var. de sessión que controlan la visualización de datos y modelos.
                    clean_ui()
                    st.session_state.file_read=uploaded_file.name
                    print (f"- en session_state: {uploaded_file.name}")
                else:
                    st.sidebar.error("No se pudo leer el fichero de logs. Verifica que el archivo tenga formato CLF válido.")
                    
        except Exception as e:
            st.sidebar.error(f"Error al procesar el archivo: {str(e)}")
            # Limpiar archivo temporal en caso de error
            try:
                if 'temp_logfile_path' in locals():
                    os.unlink(temp_logfile_path)
            except:
                pass

# Mostrar datos si están disponibles
if 'data' in st.session_state:
    df = st.session_state['data']
    algorithm=None

    # Inicializar el estado de la pestaña activa si no existe
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = 0

    if "last_selected_tab" not in st.session_state:
        st.session_state.last_selected_tab = st.session_state.active_tab

    #tabs:  - Visualización de datos originales
    #       - Entrenar nuevo modelo
    #       - Predecir con modelo pre-entrenado

    tab_names = ['VISUALIZAR datos originales', 'ENTRENAR nuevo modelo', 'PREDECIR con modelo pre-entrenado']

    # Crear radio buttons para controlar las pestañas manualmente
    selected_tab = st.radio(
        "",
        options=range(len(tab_names)),
        format_func=lambda x: tab_names[x],
        index=st.session_state.active_tab,
        horizontal=True,
        key="tab_selector"
    )
    # Detectar cambio de selección
    if selected_tab != st.session_state.last_selected_tab:
        clean_ui()  # Ejecuta tu función
        st.session_state.last_selected_tab = selected_tab

    # Actualizar el estado de la pestaña activa
    st.session_state.active_tab = selected_tab

    # Mostrar el contenido basado en la pestaña seleccionada
    if st.session_state.active_tab == 0:
        st.subheader("Visualizar datos originales:")
        if st.session_state.data_processed_flag == False:
            render_dataframe_sample(df)

    #RENDERIZADO DE UI EN MODO: ENTRENAMIENTO:
    elif st.session_state.active_tab == 1:
        # Entrenar nuevo modelo y ajuste de hiperparámetros
        st.subheader("Entrenar nuevo modelo")
        algorithm = render_training_ui_mode()
        render_model_saving_management()

    #RENDERIZADO DE UI EN MODO: PREDICCIÓN:
    elif st.session_state.active_tab == 2:
        # Carga y selección de modelo previamente entrenado
        st.subheader("Predecir usando modelo pre-entrenado")
        model,algorithm = render_predict_ui_mode()

    #VISUALIZAR RESULTADOS SI HAY DISPONIBLES EN SESIÓN
    if 'model' in st.session_state and st.session_state.result_ready:
        print (f"- Hay model en sesión y resultados: {st.session_state.ui_mode}{algorithm}")
        
        if st.session_state.ui_mode==TRAIN_MODE:
            print("- TRAIN MODE: se muestran gráficos, grid y gestión para guardar el modelo")
            # Mostrar resultados si el modelo está entrenado
            render_training_results_graphic(st.session_state.ui_mode, algorithm)
           
        else:
            print("- PREDICT MODE: se muestran gráficos, grid anomalías")
            # Mostrar resultados si el modelo está cargado
            render_prediction_results_graphic(st.session_state.ui_mode, algorithm)
            
        

else:
    st.sidebar.info("👆 Por favor, carga un archivo .log en formato CLF o genera datos de ejemplo desde el panel lateral para comenzar.")



# Información adicional
with st.expander("ℹ️ Información adicional", expanded=st.session_state.expander_open):
    st.markdown("""
    ### Autores
    - **Bryan Silva** - bryannsilva3@gmail.com 
    - **Julio González** - isc.julio.gonzalez@gmail.com 
    - **Armando Sánchez** -  armandosanchezperez1986@gmail.com 
    - **Salvador Galiano** - salvador.galiano@gmail.com 
    ## Modelos implementados:
    ### Isolation Forest
    - **Uso**: Detección de anomalías no supervisada
    - **Principio**: Aísla anomalías mediante particionamiento aleatorio
    - **Parámetros clave**:
        - *Contaminación*: Proporción esperada de anomalías en los datos
        - *N estimadores*: Número de árboles en el bosque
    
    ### K-Means Clustering
    - **Uso**: Clustering y detección de anomalías basada en distancia
    - **Principio**: Agrupa datos en K clusters y detecta puntos lejanos a centroides
    - **Parámetros clave**:
        - *K*: Número de clusters
        - *Método del codo*: Ayuda a determinar el K óptimo
    - **Métricas**:
        - *Inercia*: Suma de distancias cuadráticas a centroides (menor es mejor)
        - *Silhouette Score*: Calidad del clustering (-1 a 1, mayor es mejor)
    """)