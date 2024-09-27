#### Importamos liberias
import os
import pandas as pd
import numpy as np
import re

##############################################################################
# Definimos input y output directories
##############################################################################

input_directory = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/1_script_censo/input'
output_directory = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/1_script_censo/output'

# Nos aseguramos la existencia del directorio
os.makedirs(output_directory, exist_ok=True)

# Obtenemos el listado del directorio input
excel_files = [f for f in os.listdir(input_directory) if f.endswith('.xlsx')]

#######################################################
# Proceamos los archivos con un for loop
#######################################################

for file in excel_files:
    file_path = os.path.join(input_directory, file)
    df = pd.read_excel(file_path)

    # Convertimos los nombres de columnas a minusculas
    df.columns = df.columns.str.lower()

    # Dropeamos cuartel si existe
    if 'cuartel' in df.columns:
        df.drop('cuartel', axis=1, inplace=True)

    original_column_names = df.columns.tolist()

    # renombramos las columnas por la posición
    df = df.rename(columns={old_name: str(index) for index, old_name in enumerate(df.columns)})

    # Extraemos la primera letra de la columna para normalizar la tenencia
    def extract_first_letter(df, column_index):
        df.iloc[:, column_index] = df.iloc[:, column_index].astype(str).str[0].str.upper()
        return df
    
    df = extract_first_letter(df, 1)

    # Modificamos la columna 3 para que sea H, traansformando los valores de las columnas 7 a 18
    # Definimos columnas a transformar
    columns_to_transform = df.columns[2:3].tolist() + df.columns[7:18].tolist()  # Third column, fifth column onwards

    # Creamos una mascara con los errores
    error_mask = pd.DataFrame(False, index=df.index, columns=df.columns)

    # Convertimos a numerico y evitamos el coerce
    def safe_to_numeric(value, col, idx):
        try:
            return pd.to_numeric(value)
        except ValueError:
            error_mask.loc[idx, col] = True
            return value  # Mantenemos el valor si arroja error

    # Apply
    for col in columns_to_transform:
        if col in df.columns: 
            df[col] = df.apply(lambda row: safe_to_numeric(row[col], col, row.name), axis=1)
            
    # Transformamos las celdas con valores no numericos con las condiciones
    def cell_checker(row):
        conditions = {
            'MC': lambda x: float(x) / 1000,
            'H': lambda x: float(x),
            'h': lambda x: float(x),
            '1/2': lambda x: 0.5,
            '1 1/2': lambda x: 1.5,
            'CC': lambda x: float(x) * 1.68,
            'cc': lambda x: float(x) * 1.68,
            'METROS': lambda x: float(x) / 1000,
            'M': lambda x: float(x) / 1000,
            'm': lambda x: float(x) / 1000,
            'mt': lambda x: float(x) / 1000
        }

        for col in range(5, 19):
            if col >= len(row):  # Nos mantenemos siempre dentro de los limites del range
                continue
            value = str(row[col]).replace(',', '.')  # Reemplazamos la , por . en los valores

            for condition, transformation in conditions.items():
                if condition in value:
                    number = re.search(rf'(\d+\.?\d*)\s*{condition}', value)
                    if number:
                        row[col] = transformation(number.group(1))
                        break  # Salimos del loop si encontramos una condicion
        return row

    def transform_row(row):
        if row[3] == 'CC':
            for col in columns_to_transform:
                if col in df.columns:
                    try:
                        row[col] = pd.to_numeric(row[col]) * 1.68
                    except (ValueError, TypeError):
                        pass  # Salteamos la celda si hay error
            row[3] = 'H'
        elif row[3] == 'M':
            for col in columns_to_transform:
                if col in df.columns:
                    try:
                        row[col] = pd.to_numeric(row[col]) / 1000
                    except (ValueError, TypeError):
                        pass  # Salteamos la celda si hay error
            row[3] = 'H'
        elif row[3] == 'MC':
            for col in columns_to_transform:
                if col in df.columns:
                    try:
                        row[col] = pd.to_numeric(row[col]) / 10000
                    except (ValueError, TypeError):
                        pass  # Salteamos la celda si hay error
            row[3] = 'H'
        for col in row.index:
            if isinstance(row[col], str):
                row = cell_checker(row)  # Aplicamos el cell_checker
        return row

    df = df.apply(transform_row, axis=1)

    df = df.applymap(lambda x: str(x).replace(',', '.') if isinstance(x, (int, float)) else x)

    # Reemplazamos , por . 
    df[columns_to_transform] = df[columns_to_transform].replace(',', '.', regex=True)

    # Convertimos a floaat y sino le damos coerce
    df[columns_to_transform] = df[columns_to_transform].apply(pd.to_numeric, errors='coerce')

    # Reemplaazamos los Nan por espacios vacíos
    df = df.fillna('')

    df = df.rename(columns={str(index): old_name for index, old_name in enumerate(original_column_names)})

    # Guardamos el datafrme en un excel
    output_file_path = os.path.join(output_directory, file)
    df.to_excel(output_file_path, index=False)

    print(f"Processed and saved {file} to {output_file_path}")