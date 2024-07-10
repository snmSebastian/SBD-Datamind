

print("""
.-------------------------------------------------.
|                                                 |
|  _   _ ____  ____    _  _____ _____             |
| | | | |  _ \|  _ \  / \|_   _| ____|            |
| | | | | |_) | | | |/ _ \ | | |  _|              |
| | |_| |  __/| |_| / ___ \| | | |___             |
|  \___/|_|_  |____/_/   \_\_|_|_____|   _ ____   |
| |  _ \  / \|_   _|/ \  |  \/  |_ _| \ | |  _ \  |
| | | | |/ _ \ | | / _ \ | |\/| || ||  \| | | | | |
| | |_| / ___ \| |/ ___ \| |  | || || |\  | |_| | |
| |____/_/   \_\_/_/   \_\_|  |_|___|_| \_|____/  |
|                                                 |
'-------------------------------------------------'
""")
#_____________________
# -----LIBRERIAS -----
#_____________________


#___________________________________________________

import pandas as pd

#Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico de nombre
#de archivo en un directorio o en una jerarquía de directorios.
import glob

import numpy as np
import os
#import dask.dataframe as dd
from datetime import datetime, timedelta
print("librerias")
#___________________________
# --- EXTRACCION DATOS -----
#___________________________


ruta_chile_men= r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Chile Mensual.xlsx'
ruta_chile_sem =r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Chile Semanal.xlsx'

ruta_arg_men =r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Argentina Mensual.xlsx'
ruta_arg_sem =r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Argentina Semanal.xlsx'

ruta_per_men = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Peru Mensual.xlsx'
ruta_per_sem = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Peru Semanal.xlsx'

ruta_mex_men = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Mexico  Mensual.xlsx'
ruta_mex_sem = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Actualizacion\Actualizacion Mexico Semanal.xlsx'


df_chi_men = pd.read_excel(ruta_chile_men, header=0,dtype=str)
df_arg_men = pd.read_excel(ruta_arg_men, header=0,dtype=str)
df_per_men = pd.read_excel(ruta_per_men, header=0,dtype=str)
df_mex_men = pd.read_excel(ruta_mex_men, header=0,dtype=str)

df_chi_sem = pd.read_excel(ruta_chile_sem, header=0,dtype=str)
df_arg_sem = pd.read_excel(ruta_arg_sem, header=0,dtype=str)
df_per_sem = pd.read_excel(ruta_per_sem, header=0,dtype=str)
df_mex_sem= pd.read_excel(ruta_mex_sem, header=0,dtype=str)


print("cargo archivos")


#_______________________
#---- TIPO DE DATOS  ---
#_______________________


df_chi_men['Tipo de dato'] ='men'
df_mex_men['Tipo de dato'] ='men'
df_arg_men['Tipo de dato'] ='men'
df_per_men['Tipo de dato'] ='men'


df_chi_sem['Tipo de dato'] ='sem'
df_mex_sem['Tipo de dato'] ='sem'
df_arg_sem['Tipo de dato'] ='sem'
df_per_sem['Tipo de dato'] ='sem'

#___________________________
#---- RENAME COLUMNA -------
#___________________________
 
df_chi_men.rename(columns={'Mes':'Date'},inplace=True)
df_chi_sem.rename(columns={'Semana':'Date'},inplace=True)

df_arg_men.rename(columns={'Mes': 'Date'},inplace=True)
df_arg_sem.rename(columns={'Semana':'Date'},inplace=True)

df_per_men.rename(columns={'Mes':'Date'},inplace=True)
df_per_sem.rename(columns={'Semana':'Date'},inplace=True)

df_mex_men.rename(columns={'Mes':'Date'},inplace=True)
df_mex_sem.rename(columns={'Semana':'Date'},inplace=True)


#---Concatenacion por pais

df_chi =pd.concat([df_chi_men,df_chi_sem],axis=0,ignore_index=True)
df_mex=pd.concat([df_mex_men,df_mex_sem],axis=0,ignore_index=True)
df_per=pd.concat([df_per_men,df_per_sem],axis=0,ignore_index=True)
df_arg=pd.concat([df_arg_men,df_arg_sem],axis=0,ignore_index=True)

#---se crea columna asignando pais
df_chi['Country'] = 'Chile'
df_per['Country'] = 'Peru'
df_mex['Country'] = 'Mexico'
df_arg['Country'] = df_arg['(L) Retailer'].apply(lambda x: 'Uruguay' if 'Uruguay' in x else 'Argentina')


#---Funcion que inserta columna vacia 
def crear_columnas_vacias(df, columnas, posiciones):
    for col, pos in zip(columnas, posiciones):
        df.insert(loc=pos, column=col, value='')

#------------
# --Mexico        
columnas_vacias = ["vacia1", "vacia2", "vacia3", "vacia4"]
posiciones = [10, 12, 13, 14]
crear_columnas_vacias(df_mex, columnas_vacias, posiciones)


#------------
#--- Peru
columnas_vacias = ["vacia1", "vacia2", "vacia3", "vacia4", "vacia5", "vacia6", "vacia7", "vacia8"]
posiciones = [4, 8, 9, 10, 12, 13, 14, 17]
crear_columnas_vacias(df_per, columnas_vacias, posiciones)


#------------
#--- Argentina
columnas_vacias = ["vacia1", "vacia2", "vacia3", "vacia4", "vacia5", "vacia6", "vacia7", "vacia8", "vacia9"]
posiciones = [4, 8, 9, 10, 12, 13, 14, 17, 18]
crear_columnas_vacias(df_arg, columnas_vacias, posiciones)


# --------- RENAME COLUMNAS -------------------------------
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

print("alineacion columnas")

#____________________________
#---- df_actualizacion ------
#____________________________
 
df_actualizacion =pd.concat([df_chi,df_mex,df_arg,df_per],axis=0,ignore_index=True)

#solo tomo info del año presente
df_actualizacion = df_actualizacion[df_actualizacion['Date'].str.startswith('2024') & df_actualizacion['Date'].notna()]

print("concateno")

#______________________________
#---- TRATAMIENTO COLUMNAS ----
#______________________________


# Eliminar filas con valores NaN en la columna 'Date'
df_actualizacion = df_actualizacion.dropna(subset=['Date']) 
# Convertir a int y luego a str
df_actualizacion['Date'] = df_actualizacion['Date'].astype(str) 


df_actualizacion['(L) Retailer'] =df_actualizacion['(L) Retailer'].str.lower()
df_actualizacion['(L) Local'] = df_actualizacion['(L) Local'].str.lower()
df_actualizacion['(I) SBU'] =df_actualizacion['(I) SBU'].str.lower()


df_actualizacion.rename(columns={'(L) TIENDA FISICA /  ECOMMERCE':'canal_venta'},inplace=True)
df_actualizacion['canal_venta'] = df_actualizacion['canal_venta'].str.lower().fillna('vacio').replace('', 'vacio')

df_actualizacion['(I) MARCA'] = df_actualizacion['(I) MARCA'].astype(str).str.lower().fillna('vacio').replace('nan','vacio')
df_actualizacion['(E) Marca'] = df_actualizacion['(E) Marca'].astype(str).str.lower().fillna('vacio').replace('nan','vacio')

df_actualizacion['(I) Código Producto Interno'] = df_actualizacion['(I) Código Producto Interno'].astype(str).str.lower()
df_actualizacion['Tipo de dato'] = df_actualizacion['Tipo de dato'].astype(str).str.lower()
print("tratamiento columna")

#-- Formato numerico
df_actualizacion['Venta bruta'] = df_actualizacion['Venta bruta'].str.replace(',', '').astype(float)
df_actualizacion['Venta neta']=df_actualizacion['Venta neta'].str.replace(',', '').astype(float)
df_actualizacion['Venta costo']=df_actualizacion['Venta costo'].str.replace(',', '').astype(float)
df_actualizacion['Unidades vendidas']=df_actualizacion['Unidades vendidas'].str.replace(',', '').astype(float)
df_actualizacion['Volumen vendido (Capacidad 1)']=df_actualizacion['Volumen vendido (Capacidad 1)'].str.replace(',', '').astype(float)
df_actualizacion['Precio Publico Estimado']=df_actualizacion['Precio Publico Estimado'].str.replace(',', '').astype(float)

#--- TRATAMIENTO NULOS
def vacios_str(columna):
  return columna.fillna('vacio').replace('', 'vacio')

columnas_object = list(df_actualizacion.select_dtypes(include=['object']).columns)

for columna in columnas_object:
  df_actualizacion[columna] = vacios_str(df_actualizacion[columna])

def vacios_float(columna):
  return columna.fillna(0)
columnas_float = list(df_actualizacion.select_dtypes(include=['float']).columns)
for columna in columnas_float:
  df_actualizacion[columna] = vacios_float(df_actualizacion[columna])

print("nulos")

#___________________________
#----- COLUMNA NUM_FILA  ---
#___________________________ 

df_actualizacion.reset_index(inplace=True)
df_actualizacion.rename(columns={'index': 'num_fila'}, inplace=True)


#___________________
#---- MARCAS  ------
#___________________
 
lst_marca= [  
 'facom', 'iar expert', 'powers', 'troy-bilt', 'yard machine', 'no usar' ,
 'stanley', 'dewalt', 'black+decker', 'irwin', 'proto','bostitch', 'fatmax', 'porter cable', 
'lenox', 'craftsman',   'gridest' 
]

#---------------- ASIGNACION  ---------------------------------------------------

df_actualizacion['(I) MARCA'] = np.where((df_actualizacion['(I) MARCA'] =='vacio') & (df_actualizacion['(E) Marca'] != 'vacio'),
                               df_actualizacion['(E) Marca'],
                               df_actualizacion['(I) MARCA'])

df_actualizacion['(I) MARCA'] = df_actualizacion['(I) MARCA'].replace('vacio', 'other')

correspondencias = {
    'black+decker': ['b/d','black & de', 'black and decker', 'black&decker', 'black & decker', 'black+decker', 'black+deck', 'black + decker', 'b&d', 'b+d', 'black decker', 'black-d', 'black&deck'],
    'dewalt': ['dewalt', 'de walt'],
    'fatmax': ['stanley fatmax', 'fat max', 'fatmax'],
    'bostitch': ['bosch', 'bostitch', 'bostitch office'],
    'craftsman':['craftsman','craftman'],
    'no usar': ['einhell','sierra','geo','samoa','smart','no usar']
}

df_actualizacion['(I) MARCA'] = df_actualizacion['(I) MARCA'].apply(lambda x: next((clave for clave, valor in correspondencias.items() if x in valor), x))
df_actualizacion['(I) MARCA'] =df_actualizacion['(I) MARCA'].apply(lambda x:'other' if x not in lst_marca else x)
df_actualizacion['(I) MARCA'] =df_actualizacion['(I) MARCA'].str.upper()

print("marcas")


#_______________
#----- SBU  ----
#_______________
 
df_actualizacion['(I) SBU'] = df_actualizacion['(I) SBU'].fillna('oth').replace(['',' ','no definido','vacio','nan'], 'oth')
print("sbu")

#__________________________
#---- ECOMMERCE TIENDA  ---
#__________________________
 
lst_canal = 'internet|online|distancia|digital|virtual|ecommerce|e-com'
lst_retailer = 'mercado libre|e-comm|ecommerce|mercadolibre|amazon'

mask_retailer = df_actualizacion['(L) Retailer'].str.contains(lst_retailer)
mask_canal = df_actualizacion['(L) Local'].str.contains(lst_canal)
mask_ecom= (mask_retailer|mask_canal)

df_actualizacion.loc[mask_ecom, 'canal_venta'] = 'ecommerce'

df_actualizacion['canal_venta'] = df_actualizacion['canal_venta'].replace(['vacio','moderno','tradicional','tienda'], 'store')
df_actualizacion['canal_venta'] = df_actualizacion['canal_venta'].replace(['ecommerce','e-commerce'], 'e-commerce')

print("canal venta")

#__________________________
#------ MEXICO THD --------
#__________________________
 

df_thd = df_actualizacion.loc[(df_actualizacion['Country']=='Mexico')
                    & ((df_actualizacion['(L) Retailer'] == 'the home depot') | (df_actualizacion['(L) Retailer'] == 'the home depot e-comm') )
                    ]
df_thd= df_thd.reset_index(drop=True)


df_thd['concat_thd'] = df_thd['Tipo de dato'] + df_thd['Date'] +df_thd['(L) Local']+ df_thd['(I) Código Producto Interno']+ df_thd['canal_venta']
df_thd['concat_thd_ecom'] = df_thd['Tipo de dato'] + df_thd['Date'] +df_thd['(L) Local']+ df_thd['(I) Código Producto Interno']+ 'e-commerce'
#df_thd['countif'] = df_thd.groupby('concat_thd_ecom')['concat_thd'].transform('count')
counts = df_thd['concat_thd'].value_counts()
df_thd['countif'] = df_thd['concat_thd_ecom'].map(counts)
df_thd['concat_update'] = df_thd['Tipo de dato']+df_thd['Country'] + df_thd['Date'] +df_thd['(L) Retailer']
df_thd['countif'] = df_thd['countif'].replace(np.nan, 0).astype(int)


#contador= 0
lst_concat= df_thd['concat_thd'].to_list()
lst_concat_ecom= df_thd['concat_thd_ecom'].to_list()

def actualizar_venta_neta(row, lst_concat):
    if ((row['canal_venta'] == 'store') and (row['countif'] >= 1)):
        row_index = lst_concat.index(row['concat_thd_ecom'])
        row['Venta neta'] = row['Venta neta'] - df_thd.at[row_index, 'Venta neta']
    else:
        row['Venta neta'] = row['Venta neta']
    return row

df_thd = df_thd.apply(lambda row: actualizar_venta_neta(row, lst_concat) , axis=1)


# --------- ACTUALIZACION TDH
df_thd.drop(['concat_thd','concat_thd_ecom', 'countif'],axis=1, inplace=True)

df_merged = pd.merge(df_actualizacion[['num_fila']], df_thd[['num_fila', 'Venta neta']], on='num_fila', how='left')
df_actualizacion['Venta neta'] = df_merged['Venta neta'].fillna(df_actualizacion['Venta neta'])

print("thd")

#_______________________
#---- MEXICO COPPEL ----
#_______________________

ruta_coppel = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Coppel\precios_historico_coppel.csv'    
    
df_coppel_precios = pd.read_csv(ruta_coppel)
df_coppel_precios['concat'] = df_coppel_precios['MODELO']+df_coppel_precios['P PUBLICO']
df_coppel_precios.drop_duplicates(subset=['concat'],inplace=True)

df_coppel = df_actualizacion.loc[(df_actualizacion['Country']=='Mexico')
                    & ((df_actualizacion['(L) Retailer'] == 'coppel b')  )
                    ]
df_coppel= df_coppel.reset_index(drop=True)

#-------------------------------- COPPEL --------------------------------------------
# Convertir columnas a string y minúsculas

df_coppel_precios['MODELO'] = df_coppel_precios['MODELO'].astype(str).str.lower()
df_coppel_precios['año'] = df_coppel_precios['año'].astype(str)

# Crear nuevas columnas
df_coppel['year_modelo'] = df_coppel['Date'].str[:4] + df_coppel['(I) Código Producto Interno']
df_coppel_precios['year_modelo'] = df_coppel_precios['año'] + df_coppel_precios['MODELO']
df_coppel_precios = df_coppel_precios.drop_duplicates(subset='year_modelo')

# Realizar la búsqueda
df_coppel = df_coppel.merge(df_coppel_precios[['year_modelo', 'P PUBLICO']], on='year_modelo', how='left')

# Convertir precios a float64
df_coppel['P PUBLICO'] = df_coppel['P PUBLICO'].str.replace(',', '').astype(np.float64)

# Calcular la venta neta
df_coppel['Venta neta'] = df_coppel['Unidades vendidas'] * df_coppel['P PUBLICO']

#elimino columna para poder concatener al df_historico
df_coppel.drop(['year_modelo', 'P PUBLICO'],axis=1,inplace=True)

#-------------- ACTUALIZACION DF_ACTUALZICION
df_merged = pd.merge(df_actualizacion[['num_fila']], df_coppel[['num_fila', 'Venta neta']], on='num_fila', how='left')
df_actualizacion['Venta neta'] = df_merged['Venta neta'].fillna(df_actualizacion['Venta neta'])

print("coppel")
#_________________________________________________________
#---- COLUMNA UPDATE PARA ACTUALIZAR DF_ACTUALIZACION ----
#_________________________________________________________
 
df_actualizacion['concat_update'] = df_actualizacion['Country']+df_actualizacion['Tipo de dato']+df_actualizacion['Date']
df_actualizacion['concat_update'] = df_actualizacion['concat_update'].str.lower().str.strip()

df_actualizacion['concat_update_meli_amz'] = df_actualizacion['Country']+df_actualizacion['(L) Retailer']+df_actualizacion['Tipo de dato']+df_actualizacion['Date']
df_actualizacion['concat_update_meli_amz'] = df_actualizacion['concat_update'].str.lower().str.strip()

#___________________
#---- CALENDAR -----
#___________________

from datetime import datetime as dt
from datetime import datetime, timedelta
from isoweek import Week

from datetime import datetime as dt
def get_date_from_year_week(year_week):
    year = int(year_week[:4])
    week = int(year_week[4:])
    # Obtener la fecha del primer día de la semana
    first_day_of_week = Week(year, week).monday()
    # Agregar 1 día para obtener la fecha del lunes de esa semana
    #date = first_day_of_week + timedelta(days=1)
    # Formatear la fecha en el formato deseado
    return first_day_of_week.strftime('%m-%d-%Y')

def get_date_from_year_month(year_month):
    year = int(year_month[:4])
    month = int(year_month[4:])
    # Obtener la fecha del primer día del mes
    date = datetime(year, month, 1)
    # Formatear la fecha en el formato deseado
    return date.strftime('%m-%d-%Y')

# Aplicar las funciones lambda al dataframe
df_actualizacion['Fecha']=''
df_actualizacion = df_actualizacion.fillna('')
df_actualizacion.loc[(df_actualizacion['Tipo de dato'] == 'sem') & (df_actualizacion['Fecha'] == ''), 'Fecha'] = df_actualizacion.loc[df_actualizacion['Tipo de dato'] == 'sem', 'Date'].apply(get_date_from_year_week)
df_actualizacion.loc[(df_actualizacion['Tipo de dato'] == 'men') & (df_actualizacion['Fecha'] == ''), 'Fecha'] = df_actualizacion.loc[df_actualizacion['Tipo de dato'] == 'men', 'Date'].apply(get_date_from_year_month)
print("calendario")

#_________________
#-----HOLDER -----
#_________________
df_actualizacion['Holder']=''
notacion_holder = {
    'Mercado Libre': ['mercadolibre', 'mercado libre multivende', 'mercado libre spiral', 'mercado libre spiral ar'],
    'Sodimac': ['sodimac', 'sodimac mexico', 'sodimac argentina', 'sodimac uruguay', 'sodimac peru'],
    'Amazon': ['amazon mx'],
    'Walmart': ['walmart', 'walmart mexico', 'walmart argentina'],
    'Coppel': ['coppel b'],
    'The Home Depot': ['the home depot e-comm', 'the home depot'],
    'Easy': ['easy argentina']
}
notacion_pais = {
    'Mexico': 'MX',
    'Chile': 'CH',
    'Argentina': 'AR',
    'Uruguay': 'URU',
    'Peru': 'PE'
}
notacion_holder_invertido = {value: key for key, values in notacion_holder.items() for value in values}

lst = ['mercado libre', 'sodimac', 'walmart', 'the home depot', 'easy', 'amazon']


def holder(row):
    pais = notacion_pais.get(row['Country'], row['Country'])
    retailer = notacion_holder_invertido.get(row['(L) Retailer'].lower(), row['(L) Retailer'])
    
    local = row['(L) Local'].lower()
    retailer = retailer.lower()

    if pais == 'CH' and retailer == 'mercado libre' and local in ['fcom fcom', 'fcom']:
        retailer = 'falabella'
    elif pais == 'CH' and retailer == 'mercado libre' and local in[ 'paris paris','paris']:
        retailer = 'paris'

    if retailer in lst:
        return retailer + ' ' + pais
    else:
        return retailer
df_actualizacion['Holder'] = df_actualizacion.apply(holder, axis=1)
print("holder")
#_________________
#--- VENTA USD ---
#_________________
ruta_fx_rate = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Shares Information for Projects\FX Rate\FX_Rate.xlsx'

df_actualizacion['fecha_my'] = pd.to_datetime(df_actualizacion['Fecha']).dt.strftime('%m%Y')

df_fx_rate = pd.read_excel(ruta_fx_rate)
df_fx_rate.drop_duplicates(subset=['Fecha', 'Country'], inplace=True) # Agregar inplace=True para modificar el dataframe original
df_fx_rate['Country']= df_fx_rate['Country'].apply(lambda x: 'Uruguay' if x=='PUB' else x)

df_fx_rate['Fecha'] = pd.to_datetime(df_fx_rate['Fecha'])
df_fx_rate['fecha_my'] = df_fx_rate['Fecha'].dt.strftime('%m%Y')
df_fx_rate = df_fx_rate[df_fx_rate['Country'].isin(df_actualizacion['Country']) 
                          & df_fx_rate['fecha_my'].isin(df_actualizacion['fecha_my'])]

df_actualizacion = pd.merge(df_actualizacion, df_fx_rate[['fecha_my', 'Country', 'Adjusted Rate']], on=["fecha_my", "Country"], how='left')
try:
    df_actualizacion['Venta_neta_usd'] = df_actualizacion['Venta neta'] / df_actualizacion['Adjusted Rate']
except:
    df_actualizacion['Venta_neta_usd'] = df_actualizacion['Venta neta']

df_actualizacion['Venta_neta_usd']=df_actualizacion['Venta_neta_usd'].fillna(0)

df_actualizacion.drop(['fecha_my', 'Adjusted Rate'],axis=1,inplace=True)

print("venta usd")

#_________________________
#--- UPDATE FILE  --------
#_________________________

# Se toma la version local, para optimizar el procesamiento

#---df_historico
#path_df_historico = r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Data Flow\df_datamind_historico.csv'
#df_historico = pd.read_csv(path_df_historico,dtype=str)


#df_actualizacion_historico
path_df_actualizacion_historico=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\Data Flow\datamind_actualizacion_historico.csv'
df_actualizacion_historico=pd.read_csv(path_df_actualizacion_historico,dtype=str,header=0)
print("carga df_actualizacion_historico local")


#--- ACTUALIZAR DF_ACTUALIZACION_HISTORICO
#Con el fin de eliminar del historico la informacion que se desea actualizar

#Obtener los índices de las filas que deben ser eliminadas de df_actualizacion_historico
indices_a_eliminar_actualizacion = df_actualizacion_historico[df_actualizacion_historico['concat_update'].isin(df_actualizacion['concat_update'])].index

#Eliminar los registros del df_actualizacion
df_actualizacion_historico = df_actualizacion_historico.drop(indices_a_eliminar_actualizacion, axis=0)

#Agrego la informacion nueva al actualizacion_historico
df_actualizacion_historico = pd.concat([df_actualizacion_historico, df_actualizacion], ignore_index=True)

print("Agrego la informacion nueva al actualizacion_historico")
#Obtener los índices de las filas que deben ser eliminadas
#Con el fin de que no se agrege info que ya esta en el df_historico, pues este no se actualiza

#indices_a_eliminar = df_actualizacion_historico[df_actualizacion_historico['concat_update'].isin(df_historico['concat_update'])].index
#Eliminar los registros del df_actualizacion
#df_actualizacion_historico = df_actualizacion_historico.drop(indices_a_eliminar, axis=0)

#print("verifica que no se agrege info que ya esta en el df_historico, pues este no se actualiza")

#______________________________
#--- GUARDAR
#______________________________


#--- DRIVE

#Guardar el df_historico como archivo CSV en la ruta deseada
ruta_drive_archivo_actualizacion_csv = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\Data Flow\datamind_actualizacion_historico.csv'
df_actualizacion_historico.to_csv(ruta_drive_archivo_actualizacion_csv, index=False)

print("guarda archivo local")
#--- LOCAL
#Guardar el df_historico como archivo CSV en la ruta deseada
ruta_local_archivo_actualizacion_csv = r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Data Flow\datamind_actualizacion_historico.csv'
df_actualizacion_historico.to_csv(ruta_local_archivo_actualizacion_csv, index=False)

print("guarda archivo drive")
print("se ha ejecutado correctamente")

