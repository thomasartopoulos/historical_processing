import os
import pandas as pd
from typing import List, Dict

# Constants
BIN_EDGES = [0, 10, 100, 200, 300, 500, 1000, 1250, 2500, float('inf')]
BIN_LABELS = ['Hasta 10 hectáreas', '11 a 100 hectáreas', '101 a 200 hectáreas',
              '201 a 300 hectáreas', '301 a 500 hectáreas', '501 a 1000 hectáreas',
              '1001 a 1250 hectáreas', '1250 a 2500 hectáreas', 'más de 2500 hectáreas']
CUSTOM_ORDER = BIN_LABELS

INPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/3_script_calculo/output'
OUTPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/4_script_suma/output'
TENENCIA_INPUT_DIRECTORY = 'C:/Users/tomia/OneDrive/Documentos/Python_1895_v3/2_script_limpieza/output'

COLUMN_NAMES = [
    "extension_h_bins", "La explota el propietario, arrendatario o mediero",
    "Extensión total de las tierras dedicadas a labranza", "Trigo", "Maíz",
    "Lino", "Cebada", "Alfalfa", "Arados", "Maquinas de segar", "Rastrillos",
    "Trilladoras a vapor", "Maquinas a vapor", "Maquinas a agua", "Bombas"
]

def ensure_output_directory(directory: str) -> None:
    """Ensure the output directory exists."""
    if not os.path.exists(directory):
        os.makedirs(directory)

def process_file(file_path: str) -> pd.DataFrame:
    """Process a single Excel file and return a grouped DataFrame."""
    df = pd.read_excel(file_path, sheet_name='titular_sinfiltro_cultivo')
    df.columns = COLUMN_NAMES

    numeric_columns = COLUMN_NAMES[1:]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    grouped_df = df.groupby('extension_h_bins').agg({
        'La explota el propietario, arrendatario o mediero': 'sum',
        'Extensión total de las tierras dedicadas a labranza': 'sum',
        'Trigo': 'sum',
        'Maíz': 'sum',
        'Lino': 'sum',
        'Cebada': 'sum',
        'Alfalfa': 'sum'
    }).reset_index()

    total_extensión = grouped_df['Extensión total de las tierras dedicadas a labranza'].sum()
    grouped_df['% extensión total de las tierras dedicadas a labranza'] = (grouped_df['Extensión total de las tierras dedicadas a labranza'] / total_extensión) * 100

    return grouped_df

def aggregate_data(input_directory: str) -> pd.DataFrame:
    """Aggregate data from all Excel files in the input directory."""
    aggregated_df = pd.DataFrame()

    for filename in os.listdir(input_directory):
        if filename.endswith('.xlsx'):
            print(f"Processing file: {filename}")
            file_path = os.path.join(input_directory, filename)
            grouped_df = process_file(file_path)
            aggregated_df = pd.concat([aggregated_df, grouped_df], ignore_index=True)

    aggregated_df['extension_h_bins'] = pd.Categorical(aggregated_df['extension_h_bins'], categories=CUSTOM_ORDER, ordered=True)
    return aggregated_df

def create_final_dataframes(aggregated_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Create final dataframes from the aggregated data."""
    cuadro1 = aggregated_df.groupby('extension_h_bins', observed=False).agg({
        'La explota el propietario, arrendatario o mediero': 'sum',
        'Extensión total de las tierras dedicadas a labranza': 'sum',
        '% extensión total de las tierras dedicadas a labranza': 'mean'
    }).reset_index()

    final_grouped_df = aggregated_df.groupby('extension_h_bins', observed=False).agg({
        'La explota el propietario, arrendatario o mediero': 'sum',
        'Extensión total de las tierras dedicadas a labranza': 'sum',
        'Trigo': 'sum',
        'Maíz': 'sum',
        'Lino': 'sum',
        'Cebada': 'sum',
        'Alfalfa': 'sum',
        '% extensión total de las tierras dedicadas a labranza': 'mean'
    }).reset_index()

    return {'cuadro1': cuadro1, 'final_grouped': final_grouped_df}

def process_tenencia_file(file_path: str) -> pd.DataFrame:
    """Process a single tenencia Excel file and return a pivoted DataFrame."""
    df = pd.read_excel(file_path, sheet_name='4_filtro_tipo_AMP')
    
    # Identify the relevant columns
    extension_col = [col for col in df.columns if 'extensi' in col.lower()][0]
    tenencia_col = [col for col in df.columns if 'explota' in col.lower()][0]
    
    df['extension_h_bins'] = pd.cut(df[extension_col], bins=BIN_EDGES, labels=BIN_LABELS, include_lowest=True).astype(str)
    df['extension_h_bins'] = pd.Categorical(df['extension_h_bins'], categories=CUSTOM_ORDER, ordered=True)

    grouped = df.groupby(['extension_h_bins', tenencia_col], observed=False).agg({
        df.columns[0]: 'count',  # Assuming the first column is 'Titular'
        extension_col: 'sum'
    }).reset_index()

    pivoted = grouped.pivot(index='extension_h_bins', columns=tenencia_col)
    pivoted.fillna(0, inplace=True)
    pivoted.columns = ['_'.join(col).strip() for col in pivoted.columns.values]

    return pivoted

def aggregate_tenencia_data(tenencia_input_directory: str) -> pd.DataFrame:
    """Aggregate tenencia data from all Excel files in the input directory."""
    tenencia_df = pd.DataFrame()

    for filename in os.listdir(tenencia_input_directory):
        if filename.endswith('.xlsx'):
            print(f"Processing tenencia file: {filename}")
            file_path = os.path.join(tenencia_input_directory, filename)
            pivoted = process_tenencia_file(file_path)
            tenencia_df = tenencia_df.add(pivoted, fill_value=0) if not tenencia_df.empty else pivoted

    # Identify the correct column names
    titular_cols = [col for col in tenencia_df.columns if col.startswith(tenencia_df.columns[0].split('_')[0])]
    extension_cols = [col for col in tenencia_df.columns if col.startswith(tenencia_df.columns[1].split('_')[0])]

    desired_order = []
    for tenure in ['A', 'M', 'P']:
        titular_col = [col for col in titular_cols if col.endswith(tenure)]
        extension_col = [col for col in extension_cols if col.endswith(tenure)]
        if titular_col and extension_col:
            desired_order.extend([titular_col[0], extension_col[0]])

    for col in desired_order:
        if col not in tenencia_df.columns:
            tenencia_df[col] = 0

    return tenencia_df[desired_order]

def save_to_excel(dataframes: Dict[str, pd.DataFrame], output_path: str) -> None:
    """Save all dataframes to a single Excel file with multiple sheets."""
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        dataframes['cuadro1'].to_excel(writer, sheet_name='Cuadro inicial', index=False)
        dataframes['final_grouped'].to_excel(writer, sheet_name='Cuadro con cultivos', index=False)
        dataframes['tenencia'].to_excel(writer, sheet_name='Cuadro por tenencia', index=True)

def main():
    ensure_output_directory(OUTPUT_DIRECTORY)
    
    aggregated_df = aggregate_data(INPUT_DIRECTORY)
    final_dataframes = create_final_dataframes(aggregated_df)
    
    tenencia_df = aggregate_tenencia_data(TENENCIA_INPUT_DIRECTORY)
    final_dataframes['tenencia'] = tenencia_df

    final_output_path = os.path.join(OUTPUT_DIRECTORY, 'suma_de_partidos.xlsx')
    save_to_excel(final_dataframes, final_output_path)
    
    print(f"Agregación completa. Resultados guardados en {final_output_path}")

if __name__ == "__main__":
    main()