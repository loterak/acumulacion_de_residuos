import csv
import pandas as pd

# Datos se enecuentran en un zip, deben descomprimirse antes de poder ejecutar este módulo
df = pd.read_csv('PreProcessedData.csv')

wanted_columns = ['CODCOMP', 'P_TOT', 'GID', 'n', 'distance']
df = df[wanted_columns]
df = df.dropna()

# Peso de contenedores cercanos y lejanos
near_weight = 0.8
far_weight = 0.2

# Distancia limite
dist = 50

# Donde se guardan los resultados
res = dict()

# Lista de zonas por codigo de zona
zonas = df['CODCOMP'].unique()

for zona in zonas:
  # Rows de la zona
  data = df[df['CODCOMP'] == zona]
  # cantidad de contenedores asociados a zona
  cant_contenedores = len(data)
  # cantidad de contenedores cercanos
  near_cont = len(data[(data['distance'] <= dist)])
  # cantidad de contenedores lejanos
  far_cont = cant_contenedores - near_cont
  # cantidad de personas en la zona
  cant_p = data['P_TOT'].iloc[0]
  # cantidad de personas por cada contenedor cercano
  near_cant = 0 if near_cont == 0 else cant_p*near_weight/near_cont
  # cantidad de personas por cada contenedor lejano
  far_cant = 0 if far_cont == 0 else cant_p*far_weight/far_cont

  for _, row in data.iterrows():
    if row['GID'] in res:
      res[row['GID']] += (far_cant if row['distance'] > 50 else near_cant)
    else:
      res[row['GID']] = (far_cant if row['distance'] > 50 else near_cant)


# Se guardan datos básicos en csv
headerList = ['cantidad_personas']
infoContenedores = pd.DataFrame.from_dict(data=res, columns=headerList, orient='index')

lista_dias_acumulacion = []
peso_maximo_por_cont = 200 # kilogramos - obtenido desde division limpieza de la IMM
for idx, row in infoContenedores.iterrows():
  if row["cantidad_personas"] > 0:
    lista_dias_acumulacion.append(round(peso_maximo_por_cont / row["cantidad_personas"], 0))
  else:
    lista_dias_acumulacion.append(0)

# Se modifica el dataset para que GID sea una columna
infoContenedores.reset_index(inplace=True)
infoContenedores = infoContenedores.rename(columns = {'index':'GID'})

infoContenedores["max_dias_acumulacion"] = lista_dias_acumulacion
infoContenedores.to_csv('infoConAcumulacion.csv', header=['GID', 'cantidad_personas', "max_dias_acumulacion"], index_label='ID')