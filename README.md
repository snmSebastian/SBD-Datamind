
# Datamind

El siguiente repositorio contiene los codigos que se crearon para el procesamiento de la informacion cruda proveniente de datamind mercado libre-Amazon e Inventario. 

## Información base

El proyecto requiero de una serie de archivos base para su funcionamiento:

 | **Datamind**                          | ****                              | ****                                    | ****                                                                                                                                                                             | ****                                                                                                                        |
|---------------------------------------|-----------------------------------|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| **Fuentes**                           | Tipo almacenamiento               | Tipo archivo                            | Ubicación                                                                                                                                                                        | Nota                                                                                                                        |
| **Data Historica datamind 2018-2023** | Carpeta                           | csv                                     | Data Historica                                                                                                                                                                   | Contiene todos los archivos  descargados de Datamind tanto semanal como mensual para todos los paises, desde 2018-2023.     |
| **Data Actualizacion datamind 2024**  | Carpeta                           | csv                                     | Actualizacion                                                                                                                                                                    | Contiene todos los archivos  descargados de Datamind tanto semanal como mensual para todos los paises,  2024                |
| **Copppel**                           | Archivo                           | csv                                     | Precios historico coppel                                                                                                                                                         | Contiene el precio de los productos vendidos en coppel.                                                                     |
| **Code**                              | Carpeta                           | .py                                     | Code                                                                                                                                                                             | Contiene los archivos .py que procesan la informacion de datamind                                                           |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Mercado Libre - Amazon**            |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Fuentes**                           | Tipo almacenamiento               | Tipo archivo                            | Ubicación                                                                                                                                                                        | Nota                                                                                                                        |
| **Amazon Brasil**                     | Carpeta                           | csv                                     | Amazon Brasil                                                                                                                                                                    | Contiene todos los archivos de venta semanal desde 2023 - actualidad, de amazon para brasil, compartidos por William        |
| **Amazon Mexico**                     | Carpeta                           | csv                                     | Amazon Mexico                                                                                                                                                                    | Contiene todos los archivos de venta semanal desde 2023 - actualidad, de amazon para Mexico, compartidos por William        |
| **Code**                              | Carpeta                           | .py                                     | Code                                                                                                                                                                             | Contiene los archivos .py que procesan la informacion de mercado libre- amazon                                              |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Inventario**                        |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Fuentes**                           | Tipo almacenamiento               | Tipo archivo                            | Ubicación                                                                                                                                                                        | Nota                                                                                                                        |
| **Inventario**                        | Carpeta                           | csv                                     | Inventario                                                                                                                                                                       | Contiene los archivos de descarga semanal de inventario de Datamind, uno por cada pais                                      |
| **Code**                              | Carpeta                           | .py                                     | Code                                                                                                                                                                             | Contiene los archivos .py que procesan la información de inventario                                                         |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Shares Information for Projects**   |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Fuentes**                           | Tipo almacenamiento               | Tipo archivo                            | Ubicación                                                                                                                                                                        | Nota                                                                                                                        |
| **Calendar**                          | Archivo                           | csv                                     | Calendar                                                                                                                                                                         | Calendario personalizado para usar en los diferentes reportes PB                                                            |
| **Calendar code**                     | Archivo                           | .py                                     | Code                                                                                                                                                                             |                                                                                                                             |
| **Fx rate**                           | Archivo                           | xlsx                                    | Fx rates                                                                                                                                                                         | Contiene el historico mensual  de fx rates por pais.                                                                        |
| **Gto Net**                           | Archivo                           | xlsx                                    | Gto Net                                                                                                                                                                          | Contiene el historico mensual  de Gto net por pais.                                                                         |
| **Notacion**                          | Archivo                           | xlsx                                    | Notacion                                                                                                                                                                         | Contiene la homegeneizacion de notacion  de marca, sbu, pais                                                                |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Actualizacion - Correo**            |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Fuentes**                           | Tipo almacenamiento               | Tipo archivo                            | Ubicación                                                                                                                                                                        | Nota                                                                                                                        |
| **Correo update datamind meli_amz**   | Archivo                           | .py                                     | Correo Update Datamind Meli_Amz                                                                                                                                                  | Codigo que ejecuta codigos que actualizan informacion de datamind y meli_amz y envia correo de notificacion                 |
| **Datamind_Meli_Amz_Update**          | Archivo                           | .bat                                    | Datamind_Meli_Amz_Update                                                                                                                                                         | Codigo que ejecuta de manera programada el codigo correo update datamind meli_amz.py                                        |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Codes**                             |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Codes**                             | Fuentes                           | Tipo archivo                            | Ubicación                                                                                                                                                                        | Nota                                                                                                                        |
| **ProcesoETL_Datamind**               | Data Historica datamind 2018-2023 | .py                                     | ProcesoETL_Datamind                                                                                                                                                              | Codigo que procesa la informacion historica que proviene de datamind tanto semanal como mensual de todos los paises         |
| ****                                  | Copppel                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | Fx rate                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Proceso_Update_Datamind**           | Data Actualizacion datamind 2024  | .py                                     | Proceso_Update_Datamind                                                                                                                                                          | Codigo que procesa la informacion de actualizacion  que proviene de datamind tanto semanal como mensual de todos los paises |
| ****                                  | Copppel                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | Fx rate                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **ProcesoETL_Meli_Amz_historico**     | Amazon Brasil                     | .py                                     | ProcesoETL_Meli_Amz_Historico                                                                                                                                                    | Codigo que procesa la informacion historica hasta 2023 de mercado libre y amazon                                            |
| ****                                  | Amazon Mexico                     |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | Gto Net                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **ProcesoETL_Meli_Amz_update**        | Amazon Brasil                     | .py                                     | ProcesoETL_Meli_Amz_Historico                                                                                                                                                    | Codigo que procesa la informacionde 2024  de mercado libre y amazon                                                         |
| ****                                  | Amazon Mexico                     |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | Gto Net                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Dataflow**                          |                                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Fuentes**                           | Tipo almacenamiento               | Ubicación                               | Nota                                                                                                                                                                             |                                                                                                                             |
| **Datamind**                          | df_datamind_historico             | Dataflow Datamind                       | Contiene los archivos con la informacion procesada de, datamind hitorico, datamind update, meli_amz_historico, meli_amz_update, inventario. Estos archivos alimentan el dataflow |                                                                                                                             |
| ****                                  | Datamind_actualizacion_historico  |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | meli_amz_historico                |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | meli_amz_update                   |                                         |                                                                                                                                                                                  |                                                                                                                             |
| **Shared Information for projects**   | Calendar                          | Datafow shared information for projects | Contiene los archivos que pueden ser usados en diferentes projectos                                                                                                              |                                                                                                                             |
| ****                                  | Fx rate                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | Gto Net                           |                                         |                                                                                                                                                                                  |                                                                                                                             |
| ****                                  | Notacion                          |                                         |                                                                                                                                                                                  |                                                                                                                             |
