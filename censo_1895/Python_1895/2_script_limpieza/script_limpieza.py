#### Importamos liberias
import pandas as pd
import os
import re
import numpy as np

##############################################################################
# Definimos input y output directories
##############################################################################

input_directory = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/1_script_censo/output'
output_directory = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/2_script_limpieza/output'

##############################################################################
# Iteramos sobre los archivos .xlsx en el directorio de input y describimos las hojas y su longitud 
##############################################################################
xlsx_files = [f for f in os.listdir(input_directory) if f.endswith('.xlsx')]

#######################################################
# Proceamos los archivos y filtramos con un for loop
#######################################################

for file in xlsx_files:
    print("Processing file:", file)
    file_path = os.path.join(input_directory, file)

    # Read the Excel file into a DataFrame
    df = pd.read_excel(file_path)
    print("Numero de filas de archivo original:", len(df))

    df_filtro_titular = df[df.iloc[:, 0].notna()]
    print("Numero de filas con titular no nulo", len(df_filtro_titular))

    df_filtro_extension_nonulo = df_filtro_titular[(df_filtro_titular.iloc[:, 2].notna())]  
    print("Numero de filas con extensión ", len(df_filtro_extension_nonulo))

    df_filtro_tipo_nonulo = df_filtro_extension_nonulo[(df_filtro_extension_nonulo.iloc[:, 1].notna())]  
    print("Numero de filas con tipo de tenencia no nulo", len(df_filtro_tipo_nonulo))

    df_filtro_tipo = df_filtro_tipo_nonulo[(df_filtro_tipo_nonulo.iloc[:, 1].str.upper().isin(['A', 'M', 'P']))]  
    print("Numero de filas con tipo de tenencia A, M o P:", len(df_filtro_tipo))
    
    df_filtro_cultivo = df_filtro_tipo[(df_filtro_tipo.iloc[:, 7] >= 1) | (df_filtro_tipo.iloc[:, 8] >= 1) | (df_filtro_tipo.iloc[:, 9] >= 1) | (df_filtro_tipo.iloc[:, 6] >= 10)]
    print("Numero de filas con cultivos extension de cultivo > 1 h:", len(df_filtro_cultivo))

    # Preparamos los dataframes para escribir en el excel
    dfs = [df, df_filtro_titular, df_filtro_extension_nonulo , df_filtro_tipo_nonulo ,df_filtro_tipo, df_filtro_cultivo]
    sheet_names = ['0_tabla_original','1_filtro_titular_(nonulo)', '2_filtro_extension_(nonulo)' ,'3_filtro_tipo_(nonulos)','4_filtro_tipo_AMP' ,'5_filtro_cultivo_(_1)']

    # Creamos un nuevo output basado en el nombre original del archivo
    base_name = os.path.splitext(file)[0]  # Removemos extension del archivo
    output_file_name = f'{base_name}_tabla_final.xlsx'
    output_path = os.path.join(output_directory, output_file_name)

    # Creamos un script paraa escribir en el excel
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        for df, sheet_name in zip(dfs, sheet_names):
            # Escribimos un excel con las hojas con diferente nombre
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
    print("Excel guardado:", output_path)