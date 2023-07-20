import pandas as pd
import csv
import json

from datetime import datetime

def procesarRecorrido(filename):
    # cargamos CSV de maxima acumulacion por contenedor
    infoConAcumulacion = pd.read_csv('infoConAcumulacion.csv')

    # cargamos recorrido a procesar
    recorrido = pd.read_csv(f'recorridos/{filename}.csv')

    # procesamos el recorrido
    res = {}
    for idx, row in recorrido.iterrows():
        info = infoConAcumulacion[infoConAcumulacion['GID'] == row['GID']]

        if (info.empty): continue

        fecha = datetime.strptime(row['DIA'], '%d/%m/%Y %H:%M:%S').date()
        
        # Si no existe, inicializa la entrada del contenedor en el diccionario
        if not row['GID'] in res:
            max_dias_acumulacion = int(info['max_dias_acumulacion'].values[0])
            res[row['GID']] = { 'atrasos_por_rango': [], 'max_dias_acumulacion': max_dias_acumulacion, 'ultima_fecha': fecha if row['LEVANTADO'] == 'Si' else None }

        # Agrega rango de dias desde la ultima recoleccion
        if row['LEVANTADO'] == 'Si':
            if res[row['GID']]['ultima_fecha'] is None:
                res[row['GID']]['ultima_fecha'] = fecha

            elif (fecha > res[row['GID']]['ultima_fecha']):
                rango_dias = pd.date_range(start=res[row['GID']]['ultima_fecha'], end=fecha, inclusive="left")

                if (len(rango_dias) > res[row['GID']]['max_dias_acumulacion']):
                    rango_fechas = rango_dias[res[row['GID']]['max_dias_acumulacion']:]
                    res[row['GID']]['atrasos_por_rango'].append(rango_fechas)

            res[row['GID']]['ultima_fecha'] = fecha

    # creamos JSON con los resultados
    with open(f'resultados/{filename}.json', "w") as json_file:
        json.dump(res, json_file, default=str)