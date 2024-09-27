import os
import pandas as pd
import numpy as np
import re
from typing import List, Dict, Callable

INPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/1_script_censo/input'
OUTPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/1_script_censo/output'

def setup_directories() -> None:
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

def get_excel_files(directory: str) -> List[str]:
    return [f for f in os.listdir(directory) if f.endswith('.xlsx')]

def remove_blank_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are completely blank."""
    return df.dropna(how='all')

def clean_question_marks(df: pd.DataFrame, columns_to_check: List[int]) -> pd.DataFrame:
    """Remove rows containing '?' in specified columns."""
    for col in columns_to_check:
        if col < df.shape[1]:  # Check if the column index is valid
            df = df[df.iloc[:, col] != '?']
    return df

def extract_first_letter(df: pd.DataFrame, column: int) -> pd.DataFrame:
    """Extract the first letter of the specified column and uppercase it."""
    if column < df.shape[1]:
        df.iloc[:, column] = df.iloc[:, column].astype(str).str[0].str.upper()
    return df

def cell_checker(value):
    """Check and transform cell based on specific patterns."""
    conditions = {
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
    
    value_str = str(value).replace(',', '.')
    for condition, transformation in conditions.items():
        if condition in value_str:
            match = re.search(rf'(\d+\.?\d*)\s*{condition}', value_str)
            if match:
                return transformation(match.group(1))
    try:
        return float(value_str)
    except ValueError:
        return value

def transform_values(df: pd.DataFrame, columns_to_transform: List[int]) -> pd.DataFrame:
    def transform_row(row):
        transformation_factor = 1

        # Check column 3 (index 2 after dropping 'cuartel')
        if 3 < len(row) and pd.notna(row.iloc[3]) and pd.notna(row.iloc[2]):
            if row.iloc[3] == 'CC':
                transformation_factor = 1.68
            elif row.iloc[3] == 'M':
                transformation_factor = 1/1000
            elif row.iloc[3] == 'MC':
                transformation_factor = 1/10000
            row.iloc[3] = 'H'

        for col in columns_to_transform:
            if col < len(row):
                value = row.iloc[col]
                try:
                    if isinstance(value, str):
                        value = value.replace(',', '.')
                    row.iloc[col] = float(value) * transformation_factor
                except ValueError:
                    # If conversion fails, use cell_checker as fallback
                    row.iloc[col] = cell_checker(value)

        return row

    return df.apply(transform_row, axis=1)

def process_file(file_path: str) -> None:
    df = pd.read_excel(file_path)
    
    # Remove blank rows
    df = remove_blank_rows(df)
    
    # Drop the 'cuartel' column (assuming it's the first column)
    df = df.iloc[:, 1:]
    
    # Define column indices (adjusted for dropped 'cuartel' column)
    columns_to_check = [i for i in [2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] if i < df.shape[1]]
    columns_to_transform = [i for i in [2, 4, 5, 6, 7, 8] if i < df.shape[1]]
    
    df = clean_question_marks(df, columns_to_check)
    
    # Apply extract_first_letter to the 'explota_propietario_arrendatario_mediero' column (now index 1)
    if df.shape[1] > 1:
        df = extract_first_letter(df, 1)
    
    # Apply transformations
    df = transform_values(df, columns_to_transform)
    
    df = df.fillna('')
    
    # Assign column names
    column_names = [
        'Titular', 'La explota el propietario, arrendatario o mediero',
        'Extensión total de las tierras dedicadas a labranza', 'medida', 'Trigo',
        'Maíz', 'Lino', 'Cebada', 'Alfalfa', 'Arados', 'Maquinas de segar',
        'Rastrillos', 'Trilladoras a vapor', 'Maquinas a vapor', 'Maquinas a agua', 'Bombas'
    ]
    df.columns = column_names[:df.shape[1]]  # Assign only as many names as there are columns
    
    output_file_path = os.path.join(OUTPUT_DIRECTORY, os.path.basename(file_path))
    df.to_excel(output_file_path, index=False)
    print(f"Processed and saved {os.path.basename(file_path)} to {output_file_path}")

def main():
    setup_directories()
    excel_files = get_excel_files(INPUT_DIRECTORY)
    
    for file in excel_files:
        file_path = os.path.join(INPUT_DIRECTORY, file)
        try:
            process_file(file_path)
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")

if __name__ == "__main__":
    main()