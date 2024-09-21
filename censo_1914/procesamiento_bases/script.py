import os
import pandas as pd
import numpy as np

# Define the directories
input_directory = 'C:/Users/tomia/Downloads/Script bases/input'
output_directory = 'C:/Users/tomia/Downloads/Script bases/output'

# Ensure output directory exists
os.makedirs(output_directory, exist_ok=True)

# Get the list of Excel files in the input directory
excel_files = [f for f in os.listdir(input_directory) if f.endswith('.xlsx')]

# Process each Excel file
for file in excel_files:
    file_path = os.path.join(input_directory, file)
    df = pd.read_excel(file_path)
    
    # Check if either set of necessary columns is present
    set1_required_columns = ['foto ', 'N° registro', 'partido', 'Apellido', 'Nombre', 'Sup.', 'prop']
    set2_required_columns = ['foto ', 'N° registro', 'partido', 'Propietario Apellido', 'Nombre', 'Superficie', 'prop']
    
    set1_columns_exist = all(col in df.columns for col in set1_required_columns)
    set2_columns_exist = all(col in df.columns for col in set2_required_columns)
    
    if not (set1_columns_exist or set2_columns_exist):
        print(f"File {file} is missing required columns.")
        continue
    
    # Select specific columns based on which set exists
    if set1_columns_exist:
        df = df[set1_required_columns]
        df = df.rename(columns={'Apellido': 'Propietario Apellido', 'Sup.': 'Superficie'})
    else:
        df = df[set2_required_columns]

    # Convert 'Superficie' column to numeric, handling errors gracefully
    try:
        df['Hectareas'] = pd.to_numeric(df['Superficie'], errors='coerce') / 10000
    except ValueError:
        print(f"File {file}: Error converting 'Superficie' column to numeric. Skipping.")
        continue
    
    # Process the DataFrame
    df['Nombre'] = np.where(df['Nombre'].notna(), df['Nombre'], '-')
    df['Propietario Apellido'] = np.where(df['Propietario Apellido'].notna(), df['Propietario Apellido'], '-')
    df['Propietarios'] = df['Propietario Apellido'] + ' ' + df['Nombre']
    df['Hectareas'] = df['Hectareas'].astype(float).round(2)
    df['foto '] = df['foto '].fillna('-')

    # Filter the DataFrame
    filtered_df = df[(df['Hectareas'] > 1) & (df['prop'] != 'E')]

    # Define bin edges and labels for 'Hectareas'
    second_bin_labels = [
        'Hasta 10 hectáreas', '11 a 100 hectáreas', '101 a 200 hectáreas',
        '201 a 500 hectáreas', '501 a 1000 hectáreas', '1001 a 2500 hectáreas',
        '2501 a 4999 hectáreas', 'más de 5000 hectáreas'
    ]
    second_bin_edges = [0, 10, 100, 200, 500, 1000, 2500, 5000, float('inf')]

    # Apply binning to 'Hectareas'
    second_filtered_df = filtered_df.copy()
    second_filtered_df['extension_h_bins'] = pd.cut(
        filtered_df['Hectareas'], bins=second_bin_edges, labels=second_bin_labels, include_lowest=True
    ).astype(str)

    # Group by 'Propietarios' and sum 'Hectareas'
    second_grouped_df = second_filtered_df.groupby('Propietarios')['Hectareas'].sum().reset_index()

    # Ensure the bins are categorical and ordered
    second_grouped_df['extension_h_bins'] = pd.cut(
        second_grouped_df['Hectareas'], bins=second_bin_edges, labels=second_bin_labels, include_lowest=True
    ).astype(str)

    # Group by 'extension_h_bins' and count 'Propietarios', sum 'Hectareas'
    second_df_bins = second_grouped_df.groupby(['extension_h_bins']).agg({
        'Propietarios': ['count'],
        'Hectareas': 'sum'
    }).reset_index()

    # Rename columns for clarity
    new_columns = []
    for col in second_df_bins.columns.values:
        if col[1] != '':
            new_columns.append(''.join(col).rstrip('_'))
        else:
            new_columns.append(col[0])

    second_df_bins.columns = new_columns

    # Replace 'sum' and 'count' from column names
    second_df_bins.columns = second_df_bins.columns.str.replace(r'(sum|count)', '', regex=True)

    # Ensure the bins are categorical and ordered
    second_df_bins['extension_h_bins'] = pd.Categorical(
        second_df_bins['extension_h_bins'], categories=second_bin_labels, ordered=True
    )

    second_df_bins.sort_values(by = "extension_h_bins",inplace=True)

    # Save the processed DataFrame to a new Excel file
    output_file_path = os.path.join(output_directory, file)
    second_df_bins.to_excel(output_file_path, index=False)

    print(f"Processed and saved {file} to {output_file_path}")