#________________
#--- LIBRERIA
#________________

import pandas as pd
#Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico de nombre
#de archivo en un directorio o en una jerarquía de directorios.
import glob
import numpy as np
import os
from datetime import datetime, timedelta
#_________________________
#--- EXTRACCION DATOS
#_________________________


paht_data_arg= r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Inventario 28 dias\Data\Inventario_Argentina.xlsx'
df_arg=pd.read_excel(paht_data_arg)

paht_data_chi= r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Inventario 28 dias\Data\Inventario_Chile.xlsx'
df_chi=pd.read_excel(paht_data_chi)

paht_data_mex= r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Inventario 28 dias\Data\Inventario_Mexico.xlsx'
df_mex=pd.read_excel(paht_data_mex)

paht_data_per= r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Inventario 28 dias\Data\Inventario_Peru.xlsx'
df_per=pd.read_excel(paht_data_per)


#se crea columna asignando pais
df_chi['Country'] = 'Chile'
df_per['Country'] = 'Peru'
df_mex['Country'] = 'Mexico'
df_arg['Country'] = df_arg['(L) Retailer'].apply(lambda x: 'Uruguay' if 'Uruguay' in x else 'Argentina')

#__________________________
#---  COLUMNAS
#__________________________

#____ ALINEACION

#Funcion que inserta columna vacia 
def crear_columnas_vacias(df, columnas, posiciones):
    for col, pos in zip(columnas, posiciones):
        df.insert(loc=pos, column=col, value='')
#------------
# Peru
columnas_vacias = ["vacia3"]
posiciones = [3]
crear_columnas_vacias(df_per, columnas_vacias, posiciones)

#------------
# Argentina
columnas_vacias = ["vacia3"]
posiciones = [3]
crear_columnas_vacias(df_arg, columnas_vacias, posiciones)

#-----RENAME COLUMN
col_per=df_per.columns.to_list()
col_arg=df_arg.columns.to_list()
col_mex=df_mex.columns.to_list()
col_chi = df_chi.columns.to_list()

dict_renombres_per = {nombre_per: nombre_chi for nombre_per, nombre_chi in zip(col_per, col_chi)}
dict_renombres_arg = {nombre_arg: nombre_chi for nombre_arg, nombre_chi in zip(col_arg, col_chi)}
dict_renombres_mex = {nombre_mex: nombre_chi for nombre_mex, nombre_chi in zip(col_mex, col_chi)}



df_per.rename(columns=dict_renombres_per, inplace=True)  # renombrar las columnas utilizando el diccionario
df_mex.rename(columns=dict_renombres_mex, inplace=True)  # renombrar las columnas utilizando el diccionario
df_arg.rename(columns=dict_renombres_arg, inplace=True)  # renombrar las columnas utilizando el diccionario

#_______________
#---CONCAT
#______________
df_inventario =pd.concat([df_chi,df_mex,df_arg,df_per],axis=0,ignore_index=True)


#_______________
#--Tratamineto Nulos
#_______________

df_inventario['(E) Marca']=df_inventario['(E) Marca'].fillna('vacio').replace('','vacio')
df_inventario['(I) MARCA']=df_inventario['(I) MARCA'].fillna('vacio').replace('','vacio')

df_inventario['(E) Marca']=df_inventario['(E) Marca'].str.lower()
df_inventario['(I) MARCA']=df_inventario['(I) MARCA'].str.lower()


df_inventario.rename(columns={'(L) TIENDA FISICA /  ECOMMERCE':'canal_venta'},inplace=True)
df_inventario['canal_venta']=df_inventario['canal_venta'].fillna('vacio').replace('','vacio')
df_inventario['canal_venta']=df_inventario['canal_venta'].str.lower()


df_inventario['(I) SBU']=df_inventario['(I) SBU'].fillna('oth').replace(['',' ','no definido','vacio','nan'],'oth')

df_inventario['(L) Local']=df_inventario['(L) Local'].str.lower()
df_inventario['(L) Retailer']=df_inventario['(L) Retailer'].str.lower()

#___________
#--- MARCA
#__________

lst_marca= [  
 'facom', 'iar expert', 'powers', 'troy-bilt', 'yard machine', 'no usar' ,
 'stanley', 'dewalt', 'black+decker', 'irwin', 'proto','bostitch', 'fatmax', 'porter cable', 
'lenox', 'craftsman',   'gridest' 
]

df_inventario['(I) MARCA']=np.where((df_inventario['(I) MARCA']=='vacio')&(df_inventario['(E) Marca']!='vacio'),
                                    df_inventario['(I) MARCA']==df_inventario['(E) Marca'],
                                    df_inventario['(I) MARCA'])
correspondencias = {
    'black+decker': ['b/d','black & de', 'black and decker', 'black&decker', 'black & decker', 'black+decker', 'black+deck', 'black + decker', 'b&d', 'b+d', 'black decker', 'black-d', 'black&deck'],
    'dewalt': ['dewalt', 'de walt'],
    'fatmax': ['stanley fatmax', 'fat max', 'fatmax'],
    'bostitch': ['bosch', 'bostitch', 'bostitch office'],
    'craftsman':['craftsman','craftman'],
    'no usar': ['einhell','sierra','geo','samoa','smart','no usar']
}


df_inventario['(I) MARCA']=df_inventario['(I) MARCA'].apply(lambda x: next((clave for clave, valor in correspondencias.items() if x in valor),x))
df_inventario['(I) MARCA']=df_inventario['(I) MARCA'].apply(lambda x: 'other' if x not in lst_marca else x)


#____________
#--- CANAL
#____________

lst_canal = 'internet|online|distancia|digital|virtual|ecommerce|e-com'
lst_retailer = 'mercado libre|e-comm|ecommerce|mercadolibre|amazon'

mask_retailer = df_inventario['(L) Retailer'].str.contains(lst_retailer)
mask_local=df_inventario['(L) Local'].str.contains(lst_retailer)
mask_ecommerce=(mask_retailer | mask_local)

df_inventario.loc[mask_ecommerce,'canal_venta']='ecommerce'

df_inventario['canal_venta']=df_inventario['canal_venta'].replace(['vacio','moderno','tradicional','0','tienda'],'store')
df_inventario['canal_venta']=df_inventario['canal_venta'].replace(['ecommerce','e-commerce'],'e-commerce')









#_______________
#--- GUARDAR
#_______________


# guardar archivo en el pc local
ruta_local_archivo_csv = r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Data Flow\df_datamind_inventario.csv'
df_inventario.to_csv(ruta_local_archivo_csv, index=False)


# Guardar archivo en el drive
ruta_drive_archivo_csv = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\Data Flow\df_datamind_inventario.csv'
df_inventario.to_csv(ruta_drive_archivo_csv, index=False)
print("se ha ejecutado correctamente")