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
from AIlibrary import KMeansModel,IsolationForestModel,DataTransformer,DataCleaner,DataCLFReader,IPINFO_TOKEN, CACHE_FILE,SESSION_MIN

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
def toggle_state_data_processed_flag():
    print(f"-[toggle_state_data_processed_flag] changing flag: data_processed_flag to: {not st.session_state['data_processed_flag']}")
    st.session_state['data_processed_flag'] = not st.session_state['data_processed_flag'] 
def render_dataframe_sample(df:pd.DataFrame):
    print(f"- [render_dataframe_sample] printing loaded data (procesed={st.session_state["data_processed_flag"]})")
    df_dense = df.copy()
    
    # Convierte columnas sparse a densas
    for col in df_dense.columns:
        if  isinstance(df_dense[col].dtype, pd.SparseDtype):
            df_dense[col] = df_dense[col].sparse.to_dense()

    # Información de los datos sin procesar
    col1, col2 = st.columns(2)
    with col1:
        st.subheader(f"Vista Previa de los Datos")
        st.dataframe(df_dense.head())
    
    with col2:
        st.subheader("Estadísticas Básicas")
        st.dataframe(df_dense.describe())
def show_anomalies_grid(original_df, predictions):
    """
    Muestra los registros originales donde se detectaron anomalías
    
    Args:
        original_df: DataFrame original con todos los datos
        predictions: Array con las predicciones (-1 para anomalías, 1 para normales)
    """
    print("-[show_anomalies_grid] UI printing Grid with anomalies")
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
    print('[clean_ui] Atención: LIMPIANDO DATOS DE SESION')
    st.session_state['data_processed_flag']=False
    st.session_state.pop('data_transformed',None)
    st.session_state.pop('model',None)

#**********************************************************************************************
#                                RENDERIZADO DE LA UI
#**********************************************************************************************
if 'render_count' not in st.session_state:
    st.session_state.render_count = 0
print("...................................................................................")
print(f"                               RENDERING:UI [{st.session_state.render_count}]")
print("...................................................................................")

st.session_state.render_count+=1

# Configuración de la página
st.set_page_config(
    page_title="Detección de Anomalías en Logs",
    page_icon="🔍",
    layout="wide"
)

if 'data_processed_flag' not in st.session_state:
    st.session_state['data_processed_flag'] = False

# Título principal
st.title(f"🔍 Detección de Anomalías en Logs de Servidores Web (data_processed_flag={st.session_state['data_processed_flag']})")
st.markdown("---")

# Sidebar para configuración
st.sidebar.header("Configuración")

# Carga de datos
st.sidebar.subheader("Cargar Datos")
uploaded_file = st.sidebar.file_uploader("Subir archivo de logs (.log) en formato CLF", type=['log'],
                                         help="Selecciona un fichero log en formato CLF",
                                        )

# Generar datos de ejemplo si no se carga archivo
if uploaded_file is None:
    if st.sidebar.button("Generar Datos de Ejemplo"):
        # Generar datos sintéticos que simulan logs de servidor
        np.random.seed(42)
        n_samples = 1000
        
        # Datos normales
        normal_data = np.random.multivariate_normal(
            [100, 200, 0.5, 10], 
            [[50, 0, 0, 0], [0, 100, 0, 0], [0, 0, 0.1, 0], [0, 0, 0, 25]], 
            int(n_samples * 0.9)
        )
        
        # Datos anómalos
        anomaly_data = np.random.multivariate_normal(
            [500, 1000, 2.0, 100], 
            [[200, 0, 0, 0], [0, 500, 0, 0], [0, 0, 0.5, 0], [0, 0, 0, 100]], 
            int(n_samples * 0.1)
        )
        
        data = np.vstack([normal_data, anomaly_data])
        df = pd.DataFrame(data, columns=['response_time', 'bytes_sent', 'error_rate', 'cpu_usage'])
        
        st.session_state['data'] = df
        st.sidebar.success("Datos de ejemplo generados!")

# Leer datos del fichero de logs seleccionado
if uploaded_file is not None:

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
                
                    print('DF recien leido:')
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
    
    # Botón para transformar datos:
    if st.sidebar.button("Procesar Datos",type="primary"):
        with st.spinner("Procesando datos..."):
            df_transformed=transform_data(df)
            #print ('Data Transformed: ')
            #print (df_transformed.shape)
            st.session_state['data_transformed'] = df_transformed
            st.session_state.data_processed_flag=True

    #Datos procesados: visualizo información agregada del dataset transformado
    if st.session_state.data_processed_flag==True and 'data_transformed' in st.session_state and not st.session_state.data_transformed.empty:
        st.subheader("Datos procesados:")
        render_dataframe_sample(st.session_state['data_transformed'])

    #Lectura inicial de datos y aun no procesados: visualizo información agregada del
    #dataset original
    if st.session_state.data_processed_flag==False :
        st.subheader("Datos originales:")
        render_dataframe_sample(df)   

    # Selección de algoritmo
    algorithm = st.sidebar.selectbox(
        "Seleccionar Algoritmo",
        ["Isolation Forest", "K-Means Clustering"]
        )
    
    # Configuración específica del algoritmo
    st.sidebar.subheader("Hiperparámetros")
    
    if algorithm == "Isolation Forest":
        contamination = st.sidebar.slider(
            "Contaminación (% de anomalías esperadas)",
            min_value=0.001,
            max_value=0.5,
            value=0.1,
            step=0.001
        )
        
        n_estimators = st.sidebar.slider(
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
        if st.sidebar.button("Calcular Método del Codo"):
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
        
        n_clusters = st.sidebar.slider(
            "Número de Clusters (K)",
            min_value=2,
            max_value=15,
            value=8,
            step=1
        )
        
        train_params = {
            'n_clusters': n_clusters
        }

    # Botón para entrenar modelo
    if st.sidebar.button("Entrenar Modelo", type="secondary") and 'data_transformed' in st.session_state:
        with st.spinner("Entrenando modelo..."):
            
            df=st.session_state['data_transformed']
            
            # Seleccionar solo columnas numéricas
            numeric_df = df.select_dtypes(include=[np.number])
            
            if algorithm == "Isolation Forest":
                model = IsolationForestModel()
                confusion_matrix, predictions, pca_data = model.train_model(numeric_df, train_params)
                
                st.session_state['model'] = model
                st.session_state['predictions'] = predictions
                st.session_state['pca_data'] = pca_data
                st.session_state['confusion_matrix'] = confusion_matrix
                
                
            else:  # K-Means
                model = KMeansModel()
                confusion_matrix, cluster_labels, pca_data, anomalies, distances = model.train_model(numeric_df, train_params)
                
                st.session_state['model'] = model
                st.session_state['cluster_labels'] = cluster_labels
                st.session_state['pca_data'] = pca_data
                st.session_state['confusion_matrix'] = confusion_matrix
                st.session_state['anomalies'] = anomalies
                st.session_state['distances'] = distances
        
        st.success("¡Modelo entrenado exitosamente!")
    
    # Mostrar resultados si el modelo está entrenado
    if 'model' in st.session_state:
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

            show_anomalies_grid(df_original,predictions)
            
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
    
    # Funciones de guardar/cargar modelo
    if 'model' in st.session_state:
        st.markdown("---")
        st.subheader("Gestión del Modelo")
        
        col1, col2 = st.columns(2)
        
        with col1:
            model_name = st.text_input("Nombre del modelo", value="mi_modelo")
            if st.button("Guardar Modelo"):
                try:
                    path = st.session_state['model'].save_model(model_name)
                    st.success(f"Modelo guardado en: {path}")
                except Exception as e:
                    st.error(f"Error al guardar: {str(e)}")
        
        with col2:
            # Listar modelos guardados
            if os.path.exists('models'):
                saved_models = [f.replace('.pkl', '') for f in os.listdir('models') if f.endswith('.pkl')]
                if saved_models:
                    selected_model = st.selectbox("Cargar modelo guardado", saved_models)
                    if st.button("Cargar Modelo"):
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

else:
    st.info("👆 Por favor, carga un archivo .log en formato CLF o genera datos de ejemplo desde el panel lateral para comenzar.")



# Información adicional
with st.expander("ℹ️ Información adicional"):
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