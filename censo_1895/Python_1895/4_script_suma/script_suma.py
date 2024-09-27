import os
import pandas as pd

# Bins and labels for land tenure processing
bin_edges = [0, 10, 100, 200, 300, 500, 1000, 1250, 2500, float('inf')]
bin_labels = ['Hasta 10 hectáreas', '11 a 100 hectáreas', '101 a 200 hectáreas',
              '201 a 300 hectáreas', '301 a 500 hectáreas', '501 a 1000 hectáreas',
              '1001 a 1250 hectáreas', '1250 a 2500 hectáreas', 'más de 2500 hectáreas']
custom_order = bin_labels

input_directory = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/3_script_calculo/output'
output_directory = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/4_script_suma/output'

# Ensure the output directory exists
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

aggregated_df = pd.DataFrame()

# Process files in the input directory
for filename in os.listdir(input_directory):
    file_path = os.path.join(input_directory, filename)
    if filename.endswith('.xlsx'):
        print(f"Processing file: {filename}")
        df = pd.read_excel(file_path, sheet_name='titular_sinfiltro_cultivo')
        
        # Trim column names
        df.columns = df.columns.str.strip()
        
        # Ensure the first three columns are named correctly
        correct_names = ["extension_h_bins", "titular", "extensión total de las tierras dedicadas a labranza"]
        current_names = df.columns[:3].tolist()
        if current_names != correct_names:
            rename_dict = {current_names[i]: correct_names[i] for i in range(3)}
            df.rename(columns=rename_dict, inplace=True)
        
        # Convert all columns except the first one to numeric
        df.iloc[:, 1:] = df.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')
        
        # Group by "extension_h_bins" and sum the specified columns
        grouped_df = df.groupby('extension_h_bins').agg({
            'titular': 'sum', 
            'extensión total de las tierras dedicadas a labranza': 'sum',
            'trigo': 'sum', 
            'maíz': 'sum', 
            'lino': 'sum', 
            'cebada': 'sum', 
            'alfalfa': 'sum'
        }).reset_index()
        
        # Calculate total and percentage
        total_extensión = grouped_df['extensión total de las tierras dedicadas a labranza'].sum()
        grouped_df['% extensión total de las tierras dedicadas a labranza'] = (grouped_df['extensión total de las tierras dedicadas a labranza'] / total_extensión) * 100
        
        # Append the grouped DataFrame to the aggregated results DataFrame
        aggregated_df = pd.concat([aggregated_df, grouped_df], ignore_index=True)

# Set custom order for the categories
aggregated_df['extension_h_bins'] = pd.Categorical(aggregated_df['extension_h_bins'], categories=custom_order, ordered=True)

# Aggregate final grouped data
cuadro1 = aggregated_df.groupby('extension_h_bins').agg({
    'titular': 'sum', 
    'extensión total de las tierras dedicadas a labranza': 'sum',
    '% extensión total de las tierras dedicadas a labranza': 'mean'
}).reset_index()

final_grouped_df = aggregated_df.groupby('extension_h_bins').agg({
    'titular': 'sum', 
    'extensión total de las tierras dedicadas a labranza': 'sum',
    'trigo': 'sum', 
    'maíz': 'sum', 
    'lino': 'sum', 
    'cebada': 'sum', 
    'alfalfa': 'sum',
    '% extensión total de las tierras dedicadas a labranza': 'mean'
}).reset_index()

tenencia_input_directory = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/2_script_limpieza/output'

tenencia_df = pd.DataFrame()

# Process files in the tenencia input directory
for filename in os.listdir(tenencia_input_directory):
    file_path = os.path.join(tenencia_input_directory, filename)
    if filename.endswith('.xlsx'):
        print(f"Processing file: {filename}")
        df = pd.read_excel(file_path, sheet_name='4_filtro_tipo_AMP')
        
        # Rename columns to have clear and consistent names
        df.rename(columns={df.columns[1]: 'tenencia', df.columns[0]: 'propietario', df.columns[2]: 'extension'}, inplace=True)
        
        # Create bins for 'extension_h_bins'
        df['extension_h_bins'] = pd.cut(df['extension'], bins=bin_edges, labels=bin_labels, include_lowest=True).astype(str)
        df['extension_h_bins'] = pd.Categorical(df['extension_h_bins'], categories=custom_order, ordered=True)

        # Group and aggregate
        grouped = df.groupby(['extension_h_bins', 'tenencia'], observed=False).agg({
            'propietario': 'count',  # Count values from 'propietario'
            'extension': 'sum'     # Sum values from 'extension'
        }).reset_index()

        # Pivot with multi-level columns
        pivoted = grouped.pivot(index='extension_h_bins', columns='tenencia')

        # Fill missing values
        pivoted.fillna(0, inplace=True)

        # Rename multi-level column names for clarity
        pivoted.columns = ['_'.join(col).strip() for col in pivoted.columns.values]

        # Sum the results instead of concatenating
        if tenencia_df.empty:
            tenencia_df = pivoted
        else:
            tenencia_df = tenencia_df.add(pivoted, fill_value=0)

# Desired column order (example, adjust as necessary)
desired_order = [
    'propietario_A', 'extension_A',
    'propietario_M', 'extension_M',
    'propietario_P', 'extension_P'
]

# Ensure all desired columns exist
for col in desired_order:
    if col not in tenencia_df.columns:
        tenencia_df[col] = 0

# Reorder the columns
tenencia_df = tenencia_df[desired_order]

final_output_path = os.path.join(output_directory, 'suma_de_partidos.xlsx')

with pd.ExcelWriter(final_output_path, engine='xlsxwriter') as writer:
    cuadro1.to_excel(writer, sheet_name='Cuadro inicial', index=False)
    final_grouped_df.to_excel(writer, sheet_name='Cuadro con cultivos', index=False)
    tenencia_df.to_excel(writer, sheet_name='Cuadro por tenencia', index=True)

print(f"Agregación completa. Resultados guardados en {final_output_path}")
