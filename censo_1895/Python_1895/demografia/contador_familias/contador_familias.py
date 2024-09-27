import pandas as pd
import numpy as np

df=pd.read_csv('C:/Users/tomia/OneDrive/Documentos/Python_1895/demografia/contador_familias/df_familias.csv')

df['Nombre_pila'] = df['Nombre'].str.rsplit(' ', n=1).str[0]

df['Apellido_pila'] = df['Nombre'].str.split().str[-1]

# Convert 'Edad' column to numeric, coercing errors to NaN
df['Edad'] = pd.to_numeric(df['Edad'], errors='coerce')

# Continue with the rest of your code as before

# Inicializar la columna para almacenar Apellido_pila de la fila anterior
df['Prev_Apellido_pila'] = ''

# Iterar a través del DataFrame para actualizar la columna
for i in range(1, len(df)):
    if pd.notnull(df.loc[i, 'Hijos']) and df.loc[i, 'Sexo'] == 'Female' and (df.loc[i, 'Estado Civil'] == 'Married' or df.loc[i, 'Estado Civil'] == 'Single'):
        current_apellido_pila = df.loc[i, 'Apellido_pila']
        previous_apellido_pila = df.loc[i - 1, 'Apellido_pila']
        values_written = False
        
        # Iterar sobre las filas siguientes
        for j in range(i + 1, len(df)):
            if df.loc[j, 'Apellido_pila'] == previous_apellido_pila and df.loc[j, 'Edad'] < df.loc[i, 'Edad']:
                df.loc[j, 'Prev_Apellido_pila'] = previous_apellido_pila
                values_written = True
            else:
                break
        
        # Verificar condiciones para la fila i-1
        if (df.loc[i - 1, 'Apellido_pila'] == previous_apellido_pila and 
            df.loc[i - 1, 'Sexo'] == 'Male' and 
            df.loc[i - 1, 'Estado Civil'] == 'Married'):
            df.loc[i - 1, 'Prev_Apellido_pila'] = previous_apellido_pila
        
        # Si se escribieron valores, actualizar las filas i e i-1
        if values_written:
            df.loc[i, 'Prev_Apellido_pila'] = previous_apellido_pila
            df.loc[i - 1, 'Prev_Apellido_pila'] = previous_apellido_pila

bloques = df.groupby('Prev_Apellido_pila').size().reset_index(name='Cantidad')

# Calculate the mean Edad for each Prev_Apellido_pila bloque
mean_edad_and_count = df.groupby('Prev_Apellido_pila')['Edad'].agg(Mean_Edad='mean', Count='count').reset_index()

print(mean_edad_and_count)
# Mostrar el DataFrame actualizado
#df.to_csv('C:/Users/tomia/OneDrive/Documentos/Python_1895/demografia/contador_familias/output.csv', index=False)