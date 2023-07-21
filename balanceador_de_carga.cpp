#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <cstdlib>
#include <cstring>
#include <python3.11/Python.h>

#define CANT_RECORRIDOS 110

int main (int argc, char **argv) {
    int pid, npr, ndat, tag;

    MPI_Status info;
    
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    MPI_Comm_size(MPI_COMM_WORLD, &npr);

    if (pid == 0) { // proceso maestro - se encarga del balance de carga
        
        std::string recorridos[CANT_RECORRIDOS] = {
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
        };

        // Recorridos existentes pero que no estan en los datos:
        // "C_DU_RM_CL_110", "C_DU_RM_CL_111", "C_DU_RM_CL_112", "C_DU_RM_CL_113", "C_DU_RM_CL_114", "C_DU_RM_CL_115", "C_DU_RM_CL_116", "F_DU_RM_CL_114", "F_DU_RM_CL_115"
        
        auto recorridos_por_proceso = std::div(CANT_RECORRIDOS, npr - 1);
        int resto = recorridos_por_proceso.rem;

        int proximo = 0;
        int resto_a_sumar = 0;
        int total_recorridos_por_proceso = 0;
        for (int destino = 1; destino < npr; destino++) {
            if (resto > 0) {
                resto_a_sumar = 1;
                resto = resto - 1;
            } else {
                resto_a_sumar = 0;
            }

            total_recorridos_por_proceso = recorridos_por_proceso.quot + resto_a_sumar;
            for (int i = proximo; i < total_recorridos_por_proceso + proximo; i++) {
                
                // mandamos total_recorridos_por_proceso como tag para que sepa cuantos recv tiene que hacer
                MPI_Send(recorridos[i].c_str(), recorridos[i].size() + 1, MPI_CHAR, destino, total_recorridos_por_proceso, MPI_COMM_WORLD);
            }

            proximo = total_recorridos_por_proceso + proximo;
        }

    } else {
        char file_name[15];

        MPI_Recv(file_name, 15, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &info);
        MPI_Get_count(&info, MPI_CHAR, &ndat);
        printf("P%d recibe de P%d: tag %d, ndat %d \n", pid, info.MPI_SOURCE, info.MPI_TAG, ndat);

        Py_Initialize();

        FILE* scriptFile = fopen("procesador_de_recorrido.py", "r");
        PyRun_SimpleFile(scriptFile, "procesador_de_recorrido.py");
        fclose(scriptFile);

        PyObject* pModule = PyImport_AddModule("__main__");
        PyObject* pFunc = PyObject_GetAttrString(pModule, "procesarRecorrido");

        for (int j = 0; j < info.MPI_TAG; j++) {
            if (j != 0) {
                MPI_Recv(file_name, 15, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            
            PyObject* pyArg = PyUnicode_FromString(file_name);
            PyObject* pyRes = PyObject_CallFunctionObjArgs(pFunc, pyArg, NULL);

            Py_DECREF(pyArg);
            Py_DECREF(pyRes);
            
            printf("Proceso: %d - Recorrido: %s - Tiempo: %f \n\n", pid, file_name, MPI_Wtime());
        }
        
        Py_DECREF(pFunc);
        Py_DECREF(pModule);
        Py_Finalize();
    }
    
    MPI_Finalize();
    return 0;
}