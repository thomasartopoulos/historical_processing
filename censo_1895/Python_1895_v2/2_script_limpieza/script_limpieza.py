import pandas as pd
import os
from typing import List, Tuple, Dict

# Constants
INPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v2/1_script_censo/output'
OUTPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v2/2_script_limpieza/output'

def setup_directories() -> None:
    """Ensure the output directory exists."""
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

def get_excel_files(directory: str) -> List[str]:
    """Get a list of Excel files in the given directory."""
    return [f for f in os.listdir(directory) if f.endswith('.xlsx')]

def apply_filters(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Apply filters to the dataframe and return filtered dataframes with descriptions."""
    # Ensure columns 6, 7, 8, 9 are numeric
    for col in [6, 7, 8, 9]:
        df.iloc[:, col] = pd.to_numeric(df.iloc[:, col], errors='coerce')

    filters = {
        '0_tabla_original': df,
        '1_filtro_titular_(nonulo)': df[df.iloc[:, 0].notna()],
        '2_filtro_extension_(nonulo)': df[df.iloc[:, 0].notna() & df.iloc[:, 2].notna()],
        '3_filtro_tipo_(nonulos)': df[df.iloc[:, 0].notna() & df.iloc[:, 2].notna() & df.iloc[:, 1].notna()],
        '4_filtro_tipo_AMP': df[df.iloc[:, 0].notna() & df.iloc[:, 2].notna() & df.iloc[:, 1].notna() &
                                df.iloc[:, 1].astype(str).str.upper().isin(['A', 'M', 'P'])],
        '5_filtro_cultivo_(_1)': df[df.iloc[:, 0].notna() & df.iloc[:, 2].notna() & df.iloc[:, 1].notna() &
                                    df.iloc[:, 1].astype(str).str.upper().isin(['A', 'M', 'P']) &
                                    ((df.iloc[:, 7] >= 1) | (df.iloc[:, 8] >= 1) | 
                                     (df.iloc[:, 9] >= 1) | (df.iloc[:, 6] >= 10))]
    }
    return filters

def process_file(file_path: str) -> None:
    """Process a single Excel file, apply filters, and save results."""
    print(f"Processing file: {os.path.basename(file_path)}")
    
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return

    filtered_dfs = apply_filters(df)

    for description, filtered_df in filtered_dfs.items():
        print(f"{description}: {len(filtered_df)} rows")

    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_file_name = f'{base_name}_tabla_final.xlsx'
    output_path = os.path.join(OUTPUT_DIRECTORY, output_file_name)

    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            for sheet_name, filtered_df in filtered_dfs.items():
                filtered_df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"Excel saved: {output_path}")
    except Exception as e:
        print(f"Error saving Excel file {output_path}: {e}")

def main():
    """Main function to process all Excel files in the input directory."""
    setup_directories()
    excel_files = get_excel_files(INPUT_DIRECTORY)

    for file in excel_files:
        file_path = os.path.join(INPUT_DIRECTORY, file)
        process_file(file_path)

if __name__ == "__main__":
    main()
