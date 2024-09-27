import pandas as pd
import os
from typing import List, Dict
from pathlib import Path

# Constants
INPUT_DIRECTORY = Path('C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/2_script_limpieza/output')
OUTPUT_DIRECTORY = Path('C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/3_script_calculo/output')

BIN_EDGES = [0, 10, 100, 200, 300, 500, 1000, 1250, 2500, float('inf')]
BIN_LABELS = ['Hasta 10 hectáreas', '11 a 100 hectáreas', '101 a 200 hectáreas',
              '201 a 300 hectáreas', '301 a 500 hectáreas', '501 a 1000 hectáreas',
              '1001 a 1250 hectáreas', '1250 a 2500 hectáreas', 'más de 2500 hectáreas']
CUSTOM_ORDER = BIN_LABELS

SHEET_DESCRIPTIONS = {
    '0_tabla_original': 'Tabla original, con todas las EAPs incluyendo las que no tienen tipo de tenencia y extensión',
    '1_filtro_titular_(nonulo)': 'Tabla con EAPs con titular no nulo',
    '2_filtro_extension_(nonulo)': 'Tabla con extension no nula',
    '3_filtro_tipo_(nonulos)': 'Tabla con EAPs con tipo de tenencia no nulo',
    '4_filtro_tipo_AMP': 'Tabla con EAPs con tenencia A, M o P',
    '5_filtro_cultivo_(_1)': 'Tabla con EAPs con al menos una hectárea de cultivo o pastura'
}

def setup_directories() -> None:
    """Ensure the output directory exists."""
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)

def get_excel_files(directory: Path) -> List[Path]:
    """Get a list of Excel files in the given directory."""
    return list(directory.glob('*.xlsx'))

def create_filter_result_df(file_path: Path) -> pd.DataFrame:
    """Create a DataFrame with filter results for each sheet."""
    xls = pd.ExcelFile(file_path)
    filter_result_df = pd.DataFrame(columns=['Nombre de hoja', 'Descripción', 'Número de filas'])
    
    for sheet_name in xls.sheet_names[:6]:
        df = pd.read_excel(file_path, sheet_name)
        num_rows = df.shape[0]
        description = SHEET_DESCRIPTIONS.get(sheet_name, 'No description available')
        sheet_info_df = pd.DataFrame({'Nombre de hoja': [sheet_name], 'Descripción': [description], 'Número de filas': [num_rows]})
        filter_result_df = pd.concat([filter_result_df, sheet_info_df], ignore_index=True)
    
    return filter_result_df

def process_dataframe(df: pd.DataFrame, file: str) -> pd.DataFrame:
    """Process the DataFrame by creating bins and aggregating data."""
    # Ensure 'Extensión total de las tierras dedicadas a labranza' is numeric
    df['Extensión total de las tierras dedicadas a labranza'] = pd.to_numeric(df['Extensión total de las tierras dedicadas a labranza'], errors='coerce')
    
    df['extension_h_bins'] = pd.cut(df['Extensión total de las tierras dedicadas a labranza'], 
                                    bins=BIN_EDGES, labels=BIN_LABELS, include_lowest=True).astype(str)
    
    df['extension_h_bins'] = pd.Categorical(df['extension_h_bins'], categories=CUSTOM_ORDER, ordered=True)
    
    # Move 'extension_h_bins' to the first column
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index('extension_h_bins')))
    df = df[cols]

    # Aggregate data
    df_processed = df.groupby(['extension_h_bins'], observed=False).agg({
        'La explota el propietario, arrendatario o mediero': 'count',
        'Extensión total de las tierras dedicadas a labranza': 'sum',
        'Trigo': 'sum',
        'Maíz': 'sum',
        'Lino': 'sum',
        'Cebada': 'sum',
        'Alfalfa': 'sum',
        'Arados': 'sum',
        'Maquinas de segar': 'sum',
        'Rastrillos': 'sum',
        'Trilladoras a vapor': 'sum',
        'Maquinas a vapor': 'sum',
        'Maquinas a agua': 'sum',
        'Bombas': 'sum'
    }).reset_index()

    return df_processed

def process_tenencia_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Process the DataFrame to extract tenure information."""
    df['Extensión total de las tierras dedicadas a labranza'] = pd.to_numeric(df['Extensión total de las tierras dedicadas a labranza'], errors='coerce')
    
    df['extension_h_bins'] = pd.cut(df['Extensión total de las tierras dedicadas a labranza'], 
                                    bins=BIN_EDGES, labels=BIN_LABELS, include_lowest=True).astype(str)
    
    df['extension_h_bins'] = pd.Categorical(df['extension_h_bins'], categories=CUSTOM_ORDER, ordered=True)

    forma_tenencia_counts = pd.crosstab(index=df['extension_h_bins'], 
                                        columns=df['La explota el propietario, arrendatario o mediero'],
                                        margins=False).reset_index()

    forma_tenencia_counts.columns.name = None

    columns = ['extension_h_bins','A','M','P'] 
    df_tenencia_final = forma_tenencia_counts[columns].sort_values(by='extension_h_bins')

    df_extension = df.groupby('extension_h_bins', as_index=False)['Extensión total de las tierras dedicadas a labranza'].sum()
    
    df_tenencia_final = pd.merge(df_tenencia_final, df_extension, on='extension_h_bins', how='left')
    
    return df_tenencia_final

def process_file(file_path: Path) -> None:
    """Process a single Excel file."""
    print(f"Processing file: {file_path.name}")
    
    filter_result_df = create_filter_result_df(file_path)
    
    df_filtrado = pd.read_excel(file_path, '5_filtro_cultivo_(_1)', header=0)
    df_sin_filtrar = pd.read_excel(file_path, '4_filtro_tipo_AMP', header=0)
    
    df_titular_tenencia = process_dataframe(df_filtrado, file_path)
    df_tenencia_sinfiltro = process_dataframe(df_sin_filtrar, file_path)
    
    df_filtrado_processed = process_tenencia_dataframe(df_filtrado)
    df_sin_filtrar_processed = process_tenencia_dataframe(df_sin_filtrar)
    
    dfs = [filter_result_df, df_titular_tenencia, df_filtrado_processed, df_tenencia_sinfiltro, df_sin_filtrar_processed]
    sheet_names = ['resultados_tablas', 'titular_filtro_cultivo', 'titular_filtro_tenencia','titular_sinfiltro_cultivo', 'titular_sinfiltro_tenencia']
    
    output_file_name = f'{file_path.stem}_calculos.xlsx'
    output_path = OUTPUT_DIRECTORY / output_file_name
    
    with pd.ExcelWriter(output_path) as writer:
        for df, sheet_name in zip(dfs, sheet_names):
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"File '{output_file_name}' has been written to '{OUTPUT_DIRECTORY}'.")

def main():
    """Main function to process all Excel files in the input directory."""
    setup_directories()
    excel_files = get_excel_files(INPUT_DIRECTORY)
    
    for file_path in excel_files:
        process_file(file_path)

if __name__ == "__main__":
    main()