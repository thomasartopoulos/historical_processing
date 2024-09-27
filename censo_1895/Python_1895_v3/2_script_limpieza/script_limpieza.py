import pandas as pd
import os
import logging
from typing import List, Dict, Optional
from pathlib import Path

# Constants
INPUT_DIRECTORY = Path('C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/1_script_censo/output')
OUTPUT_DIRECTORY = Path('C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/2_script_limpieza/output')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_directories() -> None:
    """Ensure the output directory exists."""
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

def get_excel_files(directory: Path) -> List[Path]:
    """Get a list of Excel files in the given directory."""
    return list(directory.glob('*.xlsx'))

def apply_filters(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Apply filters to the dataframe and return filtered dataframes with descriptions."""
    # Ensure columns 4, 5, 6, 7, 8 are numeric
    for col in [4, 5, 6, 7, 8]:
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
                                    ((df.iloc[:, 4] >= 1) | (df.iloc[:, 5] >= 1) |
                                     (df.iloc[:, 6] >= 1) | (df.iloc[:, 7] >= 10))]
    }
    return filters

def process_file(file_path: Path) -> Optional[Dict[str, pd.DataFrame]]:
    """Process a single Excel file, apply filters, and return results."""
    logging.info(f"Processing file: {file_path.name}")
   
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

    filtered_dfs = apply_filters(df)
    for description, filtered_df in filtered_dfs.items():
        logging.info(f"{description}: {len(filtered_df)} rows")

    return filtered_dfs

def save_results(filtered_dfs: Dict[str, pd.DataFrame], output_path: Path) -> None:
    """Save filtered dataframes to an Excel file."""
    try:
        with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
            for sheet_name, filtered_df in filtered_dfs.items():
                filtered_df.to_excel(writer, sheet_name=sheet_name, index=False)
        logging.info(f"Excel saved: {output_path}")
    except Exception as e:
        logging.error(f"Error saving Excel file {output_path}: {e}")

def main() -> None:
    """Main function to process all Excel files in the input directory."""
    setup_directories()
    excel_files = get_excel_files(INPUT_DIRECTORY)

    for file_path in excel_files:
        filtered_dfs = process_file(file_path)
        if filtered_dfs:
            output_path = OUTPUT_DIRECTORY / f'{file_path.stem}_tabla_final.xlsx'
            save_results(filtered_dfs, output_path)

if __name__ == "__main__":
    main()