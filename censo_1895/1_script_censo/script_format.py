#### Importamos liberias
import os
import pandas as pd
import numpy as np
import re
from typing import List, Dict, Callable

# Definimos input y output directories
INPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v2/1_script_censo/input'
OUTPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v2/1_script_censo/output'

# Nos aseguramos la existencia del directorio de salida
def setup_directories() -> None:
    """Ensure the output directory exists."""
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

# Obtenemos el listado de archivos Excel en el directorio de entrada
def get_excel_files(directory: str) -> List[str]:
    """Get a list of Excel files in the given directory."""
    return [f for f in os.listdir(directory) if f.endswith('.xlsx')]

# Preprocesamos el dataframe para convertir nombres de columnas a minúsculas y eliminar 'cuartel' si existe
def preprocess_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the dataframe: lowercase columns, drop 'cuartel' if exists."""
    df.columns = df.columns.str.lower()
    if 'cuartel' in df.columns:
        df = df.drop('cuartel', axis=1)
    return df

# Limpiamos filas con caracteres '?'
def clean_question_marks(df: pd.DataFrame, columns_to_check: List[str]) -> pd.DataFrame:
    """Remove rows containing '?' in specified columns."""
    for col in columns_to_check:
        if col in df.columns:
            df = df[df[col] != '?']
    return df

# Extraemos la primera letra de una columna específica y la convertimos en mayúscula
def extract_first_letter(df: pd.DataFrame, column_index: int) -> pd.DataFrame:
    """Extract the first letter of the specified column and uppercase it."""
    df.iloc[:, column_index] = df.iloc[:, column_index].astype(str).str[0].str.upper()
    return df

# Función para convertir valores a numéricos de forma segura
def safe_to_numeric(value: any, col: str, idx: int, error_mask: pd.DataFrame) -> float:
    """Safely convert to numeric, updating error_mask if conversion fails."""
    try:
        return pd.to_numeric(value)
    except ValueError:
        error_mask.loc[idx, col] = True
        return value

# Función para revisar y transformar celdas específicas
def cell_checker(row):
    """Check and transform cells based on specific patterns."""
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
        if col >= len(row):  # Ensure we stay within range
            continue
        value = str(row[col]).replace(',', '.')  # Replace comma with dot

        for condition, transformation in conditions.items():
            if condition in value:
                number = re.search(rf'(\d+\.?\d*)\s*{condition}', value)
                if number:
                    row[col] = transformation(number.group(1))
                    break  # Stop once a condition is applied
    return row

# Transformamos los valores en columnas específicas con condiciones
def transform_values(df: pd.DataFrame, columns_to_transform: List[str]) -> pd.DataFrame:
    """Transform values in specified columns based on conditions."""
    conditions: Dict[str, Callable[[str], float]] = {
        'MC': lambda x: float(x) / 1000,
        'H': float,
        'h': float,
        '1/2': lambda x: 0.5,
        '1 1/2': lambda x: 1.5,
        'CC': lambda x: float(x) * 1.68,
        'cc': lambda x: float(x) * 1.68,
        'METROS': lambda x: float(x) / 1000,
        'M': lambda x: float(x) / 1000,
        'm': lambda x: float(x) / 1000,
        'mt': lambda x: float(x) / 1000
    }

    def transform_row(row):
        for col in columns_to_transform:
            if col in df.columns:
                value = str(row[col]).replace(',', '.')
                for condition, transformation in conditions.items():
                    if condition in value:
                        number = re.search(rf'(\d+\.?\d*)\s*{condition}', value)
                        if number:
                            row[col] = transformation(number.group(1))
                            break
        return row

    return df.apply(transform_row, axis=1)

# Reemplazamos comas por puntos para valores numéricos
def replace_comma_with_dot(x):
    """Replace comma with dot for numeric values."""
    return str(x).replace(',', '.') if isinstance(x, (int, float)) else x

# Nueva función para transformar valores en función de la columna 3
def transform_row(row, columns_to_transform: List[str], df: pd.DataFrame) -> pd.Series:
    """Transform specific values in row 3 based on certain conditions."""
    if row[3] == 'CC':
        for col in columns_to_transform:
            if col in df.columns:
                try:
                    row[col] = pd.to_numeric(row[col]) * 1.68
                except (ValueError, TypeError):
                    pass  # Saltamos si hay error
        row[3] = 'H'
    elif row[3] == 'M':
        for col in columns_to_transform:
            if col in df.columns:
                try:
                    row[col] = pd.to_numeric(row[col]) / 1000
                except (ValueError, TypeError):
                    pass  # Saltamos si hay error
        row[3] = 'H'
    elif row[3] == 'MC':
        for col in columns_to_transform:
            if col in df.columns:
                try:
                    row[col] = pd.to_numeric(row[col]) / 10000
                except (ValueError, TypeError):
                    pass  # Saltamos si hay error
        row[3] = 'H'

    # Aplicamos otras transformaciones si es necesario
    for col in row.index:
        if isinstance(row[col], str):
            row = cell_checker(row)  # Aplicamos cell_checker si es necesario
    return row

# Procesamos un archivo Excel y aplicamos todas las transformaciones
def process_file(file_path: str) -> None:
    """Process a single Excel file."""
    df = pd.read_excel(file_path)
    df = preprocess_dataframe(df)
    original_column_names = df.columns.tolist()
    
    # Clean rows with '?' characters
    columns_to_check = df.columns[2:3].tolist() + df.columns[7:18].tolist()
    df = clean_question_marks(df, columns_to_check)
    
    df = df.rename(columns={old_name: str(index) for index, old_name in enumerate(df.columns)})
    df = extract_first_letter(df, 1)
    
    columns_to_transform = df.columns[2:3].tolist() + df.columns[7:18].tolist()
    error_mask = pd.DataFrame(False, index=df.index, columns=df.columns)
    
    for col in columns_to_transform:
        if col in df.columns:
            df[col] = df.apply(lambda row: safe_to_numeric(row[col], col, row.name, error_mask), axis=1)
    
    # Apply the transform_row function to each row
    df = df.apply(lambda row: transform_row(row, columns_to_transform, df), axis=1)
    
    # Replace commas with dots
    df = df.applymap(replace_comma_with_dot)
    
    df[columns_to_transform] = df[columns_to_transform].replace(',', '.', regex=True)
    df[columns_to_transform] = df[columns_to_transform].apply(pd.to_numeric, errors='coerce')
    df = df.fillna('')
    
    # Restore original column names
    df = df.rename(columns={str(index): old_name for index, old_name in enumerate(original_column_names)})
    
    # Save the processed file
    output_file_path = os.path.join(OUTPUT_DIRECTORY, os.path.basename(file_path))
    df.to_excel(output_file_path, index=False)
    print(f"Processed and saved {os.path.basename(file_path)} to {output_file_path}")

# Función principal para procesar todos los archivos en el directorio de entrada
def main():
    """Main function to process all Excel files in the input directory."""
    setup_directories()
    excel_files = get_excel_files(INPUT_DIRECTORY)
    
    for file in excel_files:
        file_path = os.path.join(INPUT_DIRECTORY, file)
        process_file(file_path)

if __name__ == "__main__":
    main()
