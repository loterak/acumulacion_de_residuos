import json
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
import csv
import pandas as pd
from pprint import pprint


recorridos = [
    "A_DU_RM_CL_101", "A_DU_RM_CL_102", "A_DU_RM_CL_103", "A_DU_RM_CL_104", "A_DU_RM_CL_105", "A_DU_RM_CL_106", "A_DU_RM_CL_107", "A_DU_RM_CL_108", 
    "A_DU_RM_CL_109", "A_DU_RM_CL_110", "A_DU_RM_CL_111", "A_DU_RM_CL_112", "A_DU_RM_CL_113", "A_DU_RM_CL_114", "A_DU_RM_CL_115", "A_DU_RM_CL_116",
    "A_DU_RM_CL_117", "A_DU_RM_CL_118", "A_DU_RM_CL_119", "B_DU_RM_CL_101", "B_DU_RM_CL_102", "C_DU_RM_CL_101", "C_DU_RM_CL_102", "C_DU_RM_CL_103",
    "C_DU_RM_CL_104", "C_DU_RM_CL_105", "C_DU_RM_CL_106", "C_DU_RM_CL_107", "C_DU_RM_CL_108", "C_DU_RM_CL_109", "CH_DU_RM_CL_01", "CH_DU_RM_CL_02",
    "CH_DU_RM_CL_03", "CH_DU_RM_CL_04", "CH_DU_RM_CL_05", "CH_DU_RM_CL_06", "CH_DU_RM_CL_07", "CH_DU_RM_CL_08", "CH_DU_RM_CL_09", "CH_DU_RM_CL_10",
    "CH_DU_RM_CL_11", "CH_DU_RM_CL_12", "CH_DU_RM_CL_13", "D_DU_RM_CL_101", "D_DU_RM_CL_102", "D_DU_RM_CL_103", "D_DU_RM_CL_104", "D_DU_RM_CL_105", 
    "D_DU_RM_CL_106", "D_DU_RM_CL_107", "D_DU_RM_CL_108", "D_DU_RM_CL_109", "D_DU_RM_CL_110", "D_DU_RM_CL_111", "D_DU_RM_CL_112", "D_DU_RM_CL_113",
    "D_DU_RM_CL_114", "D_DU_RM_CL_115", "D_DU_RM_CL_116", "D_DU_RM_CL_117", "D_DU_RM_CL_118", "D_DU_RM_CL_119", "D_DU_RM_CL_120", "D_DU_RM_CL_121",
    "E_DU_RM_CL_101", "E_DU_RM_CL_102", "E_DU_RM_CL_103", "E_DU_RM_CL_104", "E_DU_RM_CL_105", "E_DU_RM_CL_106", "E_DU_RM_CL_107", "E_DU_RM_CL_108",
    "E_DU_RM_CL_109", "E_DU_RM_CL_110", "E_DU_RM_CL_111", "E_DU_RM_CL_112", "E_DU_RM_CL_113", "E_DU_RM_CL_114", "E_DU_RM_CL_115", "E_DU_RM_CL_116",
    "E_DU_RM_CL_136", "F_DU_RM_CL_101", "F_DU_RM_CL_102", "F_DU_RM_CL_103", "F_DU_RM_CL_104", "F_DU_RM_CL_105", "F_DU_RM_CL_106", "F_DU_RM_CL_107",
    "F_DU_RM_CL_108", "F_DU_RM_CL_109", "F_DU_RM_CL_110", "F_DU_RM_CL_111", "F_DU_RM_CL_112", "F_DU_RM_CL_113", "G_DU_RM_CL_101", "G_DU_RM_CL_102",
    "G_DU_RM_CL_103", "G_DU_RM_CL_104", "G_DU_RM_CL_105", "G_DU_RM_CL_106", "G_DU_RM_CL_107", "G_DU_RM_CL_108", "G_DU_RM_CL_109", "G_DU_RM_CL_110",
    "G_DU_RM_CL_111", "G_DU_RM_CL_112", "G_DU_RM_CL_113", "G_DU_RM_CL_114", "G_DU_RM_CL_115", "G_DU_RM_CL_116"
]

results = {}

for filename in recorridos:

    with open(f'resultados/{filename}.json', "r") as json_file:
        parsed_dict = json.loads(json_file.read())

    for container in parsed_dict.keys():
        delay_list = list(map(len, parsed_dict[container]['atrasos_por_rango']))
        total_days = 0
        max_delay = 0
        total_delays = 0
        if (delay_list != []):
            total_days = sum(delay_list)
            max_delay = max(delay_list)
            total_delays = len(delay_list)
        # dias totales de atrasos, atraso maximo, cantidad de atrasos
        results[container] = [total_days, max_delay, total_delays]

# Transfer data to csv
pd.DataFrame.from_dict(results, orient='index').to_csv('resultados_contenedores.csv', header=['total_days_delayed', 'max_delay', 'total_delays'], index_label='GID')
