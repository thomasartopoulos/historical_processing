import pandas as pd
import os

directory_path = 'C:/Users/tomia/OneDrive/Documentos/Python_1895/demografia/'
excel_file_path = os.path.join(directory_path, 'poblacion.xlsx')

df = pd.read_excel(excel_file_path)
rural_df = df[df['Condición'] == 'Rural'].copy()
rural_df['Profesión'] = rural_df['Profesión'].str.lower()  # Lowercase once

partidos = rural_df['Partido'].unique()

# Lists to store DataFrames for concatenation
all_menores_info = []
all_profesion_counts = []
all_nacimiento_counts = []
all_agricultores_info = []

for partido in partidos:
    partido_df = rural_df[rural_df['Partido'] == partido].copy()

    # Agricultores profession
    agricultores_profesion = partido_df[partido_df['Profesión'].isin(['chacarero', 'agricultor', 'estanciero'])]
    agricultores_profesion = agricultores_profesion.groupby('Lugar de nacimiento').size().reset_index(name='Count')

    # Profesion counts
    profesion_counts_df = partido_df.groupby('Profesión').size().reset_index(name='Count')
    
    # Nacimiento counts
    nacimiento_counts_df = partido_df.groupby('Lugar de nacimiento').size().reset_index(name='Count')
    
    # Menores info
    menores_df = partido_df[partido_df['Edad'] < 14]
    info_menores_df = pd.DataFrame({
        'Partido': [partido],
        'Proporción Lee y Escribe': [(menores_df['Lee y escribe'] == 'si').mean()],
        'Proporción Va a la Escuela': [(menores_df['Va a la escuela'] == 'si').mean()],
        'Proporción con Profesión': [menores_df['Profesión'].notna().mean()],
        'Total de menores': [len(menores_df)],
        'Total Mujer': [(menores_df['Sexo'] == 'Mujer').sum()],
        'Total Varon': [(menores_df['Sexo'] == 'Varón').sum()]
    })

    # Append to lists
    all_menores_info.append(info_menores_df)
    all_profesion_counts.append(profesion_counts_df)
    all_nacimiento_counts.append(nacimiento_counts_df)
    all_agricultores_info.append(agricultores_profesion)

    # Write to Excel for each 'Partido'
    with pd.ExcelWriter(os.path.join(directory_path, f'{partido}.xlsx')) as writer:
        info_menores_df.to_excel(writer, sheet_name='Menores Info', index=False)
        profesion_counts_df.to_excel(writer, sheet_name='Profesion Counts', index=False)
        nacimiento_counts_df.to_excel(writer, sheet_name='Nacimiento Counts', index=False)
        agricultores_profesion.to_excel(writer, sheet_name='Agricultores Profesion', index=False)

# Concatenate all DataFrames outside the loop
all_menores_info_df = pd.concat(all_menores_info)
all_profesion_counts_df = pd.concat(all_profesion_counts).groupby('Profesión').sum().reset_index()
all_nacimiento_counts_df = pd.concat(all_nacimiento_counts).groupby('Lugar de nacimiento').sum().reset_index()
all_agricultores_info_df = pd.concat(all_agricultores_info).groupby('Lugar de nacimiento').sum().reset_index()

# Write aggregated counts to a new Excel file
with pd.ExcelWriter(os.path.join(directory_path, 'aggregated_counts.xlsx')) as writer:
    all_menores_info_df.to_excel(writer, sheet_name='Aggregated Menores Info', index=False)
    all_profesion_counts_df.to_excel(writer, sheet_name='Aggregated Profesion Counts', index=False)
    all_nacimiento_counts_df.to_excel(writer, sheet_name='Aggregated Nacimiento Counts', index=False)
    all_agricultores_info_df.to_excel(writer, sheet_name='Agricultores Profesion', index=False)