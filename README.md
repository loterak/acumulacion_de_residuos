# Pasos para ejecucion del proyecto
1 - Clonar el repositorio

2 - Dentro de la carpeta en la que fue clonado el proyecto, generamos el ejecutable de C corriendo el siguiente comando:
mpic++ -lpython3.11 balanceador_de_carga.cpp -o Balanceador

3 - Una vez compilado, utilizamos el comando `mpirun` para empezar la ejecucion. Se pueden utilizar las flags `-np` para establecer la cantidad de procesos a utilizar y `-hosts` para determinar en que computadores ejecutar, separandolos con una coma. Ejemplo:
mpirun -np 8 -hosts pcunix106,pcunix132 ./Balanceador

4 - Los resultados se generan como archivos JSON en el directorio `resultados`.
