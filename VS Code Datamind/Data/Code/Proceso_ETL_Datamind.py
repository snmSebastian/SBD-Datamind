#--------------------------------------------------
#---------------- LIBRERIAS -----------------------
#--------------------------------------------------
#Liberia
import pandas as pd

#Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico de nombre
#de archivo en un directorio o en una jerarquía de directorios.
import glob

import numpy as np
import os
from datetime import datetime, timedelta

print("cargo librerias")

#_________________________________________________
#--------- EXTRACCION DATOS -----------------------
#_________________________________________________

# Obtener la lista de archivos que por pais y tipo de dato(sem - men):


# utilizamos la función glob para crear una lista de rutas de archivo que coinciden con el patrón *Chile Mensual*.csv
# en el directorio path_chi_men. Esto nos da una lista de todas las rutas de archivo que cumplen con el patrón en el
# directorio.

#paht_data_historica= r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\data Historico'

paht_data_historica= r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\VS Code Datamind\Data\data Historico'

all_files_chi_men = glob.glob(paht_data_historica + "/*Chile Mensual*.csv")

all_files_arg_men = glob.glob(paht_data_historica + "/*Argentina Mensual*.csv")

all_files_per_men = glob.glob(paht_data_historica + "/*Peru Mensual*.csv")

all_files_mex_men = glob.glob(paht_data_historica + "/*Mexico Mensual*.csv")

#__________________________________________________

#Data Semanal Historica
all_files_chi_sem = glob.glob(paht_data_historica + "/*Chile Semanal*.csv")

all_files_arg_sem = glob.glob(paht_data_historica + "/*Argentina Semanal*.csv")

all_files_per_sem = glob.glob(paht_data_historica + "/*Peru Semanal*.csv")

all_files_mex_sem = glob.glob(paht_data_historica + "/*Mexico Semanal*.csv")

print("cargo archvivos")

## ------------------APPEND LISTAS -----------------------
#Leer cada archivo y agregarlos a una lista:
ls_chi_men = []
ls_mex_men = []
ls_per_men = []
ls_arg_men = []

ls_chi_sem = []
ls_mex_sem = []
ls_per_sem = []
ls_arg_sem = []

# Se anexan los archivos en una sola lista

for filename in all_files_chi_men:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_chi_men.append(df)

for filename in all_files_arg_men:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_arg_men.append(df)

for filename in all_files_per_men:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_per_men.append(df)

for filename in all_files_mex_men:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_mex_men.append(df)



for filename in all_files_chi_sem:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_chi_sem.append(df)

for filename in all_files_arg_sem:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_arg_sem.append(df)

for filename in all_files_per_sem:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_per_sem.append(df)

for filename in all_files_mex_sem:
    df = pd.read_csv(filename, index_col=None, header=0,dtype=str)
    ls_mex_sem.append(df)

## ---------------- LISTA A DATAFRAME --------------------

df_chi_men = pd.concat(ls_chi_men, axis=0, ignore_index=True)
df_arg_men = pd.concat(ls_arg_men, axis=0, ignore_index=True)
df_per_men = pd.concat(ls_per_men, axis=0, ignore_index=True)
df_mex_men = pd.concat(ls_mex_men, axis=0, ignore_index=True)

df_chi_sem = pd.concat(ls_chi_sem, axis=0, ignore_index=True)
df_arg_sem = pd.concat(ls_arg_sem, axis=0, ignore_index=True)
df_per_sem = pd.concat(ls_per_sem, axis=0, ignore_index=True)
df_mex_sem= pd.concat(ls_mex_sem, axis=0, ignore_index=True)

# ---- Se agrega columna "tipo de datos" ----------------


df_chi_men['Tipo de dato'] ='men'
df_mex_men['Tipo de dato'] ='men'
df_arg_men['Tipo de dato'] ='men'
df_per_men['Tipo de dato'] ='men'


df_chi_sem['Tipo de dato'] ='sem'
df_mex_sem['Tipo de dato'] ='sem'
df_arg_sem['Tipo de dato'] ='sem'
df_per_sem['Tipo de dato'] ='sem'

## ----- RENAME col(mes -semana) -> Date ------------------
#Esto con el fin de poder concatener los dataframes y poder diferenciar el origen de la informacion
 
df_chi_men.rename(columns={'Mes':'Date'},inplace=True)
df_chi_sem.rename(columns={'Semana':'Date'},inplace=True)

df_arg_men.rename(columns={'Mes': 'Date'},inplace=True)
df_arg_sem.rename(columns={'Semana':'Date'},inplace=True)

df_per_men.rename(columns={'Mes':'Date'},inplace=True)
df_per_sem.rename(columns={'Semana':'Date'},inplace=True)

df_mex_men.rename(columns={'Mes':'Date'},inplace=True)
df_mex_sem.rename(columns={'Semana':'Date'},inplace=True)

#Concatenacion por pais

df_chi =pd.concat([df_chi_men,df_chi_sem],axis=0,ignore_index=True)
df_mex=pd.concat([df_mex_men,df_mex_sem],axis=0,ignore_index=True)
df_per=pd.concat([df_per_men,df_per_sem],axis=0,ignore_index=True)
df_arg=pd.concat([df_arg_men,df_arg_sem],axis=0,ignore_index=True)

#se crea columna asignando pais
df_chi['Country'] = 'Chile'
df_per['Country'] = 'Peru'
df_mex['Country'] = 'Mexico'
df_arg['Country'] = df_arg['(L) Retailer'].apply(lambda x: 'Uruguay' if 'Uruguay' in x else 'Argentina')

#..................................................................................

#Hasta este punto se ha unificado todos los archivos de cada pais semanal y mensual en un solo dataframe.
#Se puede procecer a realizar el proceso de transformación correspondiente para cada país


#_________________________________________________
# ----------------- TRANSFORMACION ----------------------------
#_________________________________________________


## Alineacion de columnas

# Esto para poder concatenar todos los df en uno solo

#Funcion que inserta columna vacia 
def crear_columnas_vacias(df, columnas, posiciones):
    for col, pos in zip(columnas, posiciones):
        df.insert(loc=pos, column=col, value='')

#------------
# Mexico        
columnas_vacias = ["vacia1", "vacia2", "vacia3", "vacia4"]
posiciones = [10, 12, 13, 14]
crear_columnas_vacias(df_mex, columnas_vacias, posiciones)


#------------
# Peru
columnas_vacias = ["vacia1", "vacia2", "vacia3", "vacia4", "vacia5", "vacia6", "vacia7", "vacia8"]
posiciones = [4, 8, 9, 10, 12, 13, 14, 17]
crear_columnas_vacias(df_per, columnas_vacias, posiciones)


#------------
# Argentina
columnas_vacias = ["vacia1", "vacia2", "vacia3", "vacia4", "vacia5", "vacia6", "vacia7", "vacia8", "vacia9"]
posiciones = [4, 8, 9, 10, 12, 13, 14, 17, 18]
crear_columnas_vacias(df_arg, columnas_vacias, posiciones)

# Renombro nombres de las columnas

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


## ----------------- df_historico ------------------------------------
#se concatenan los df_historico: df_mex df_arg df_per df_chi
#este contiene la data historica de todos los paises semanal y mensual

df_historico =pd.concat([df_chi,df_mex,df_arg,df_per],axis=0,ignore_index=True)


print("concateno")
#_________________________________________________
# -------------- TRATAMIENTO COLUMNAS -------------
#_________________________________________________

#Se hace tratamiento de las columnas que se usaran para el posterior proceso de tratamiento para mexico

df_historico['Date'] = df_historico['Date'].astype(str)

df_historico['(L) Retailer'] =df_historico['(L) Retailer'].str.lower()
df_historico['(L) Local'] = df_historico['(L) Local'].str.lower()
df_historico['(I) SBU'] =df_historico['(I) SBU'].str.lower()


df_historico.rename(columns={'(L) TIENDA FISICA /  ECOMMERCE':'canal_venta'},inplace=True)
df_historico['canal_venta'] = df_historico['canal_venta'].str.lower().fillna('vacio').replace('', 'vacio')


df_historico['(I) MARCA'] = df_historico['(I) MARCA'].astype(str).str.lower().fillna('vacio').replace('nan','vacio')
df_historico['(E) Marca'] = df_historico['(E) Marca'].astype(str).str.lower().fillna('vacio').replace('nan','vacio')


df_historico['(I) Código Producto Interno'] = df_historico['(I) Código Producto Interno'].astype(str).str.lower()
df_historico['Tipo de dato'] = df_historico['Tipo de dato'].astype(str).str.lower()

df_historico['Venta neta']=df_historico['Venta neta'].str.replace(',', '').astype(float)
df_historico['Venta bruta']=df_historico['Venta bruta'].str.replace(',', '').astype(float)
df_historico['Venta costo']=df_historico['Venta costo'].str.replace(',', '').astype(float)
df_historico['Unidades vendidas']=df_historico['Unidades vendidas'].str.replace(',', '').astype(float)
df_historico['Volumen vendido (Capacidad 1)']=df_historico['Volumen vendido (Capacidad 1)'].str.replace(',', '').astype(float)
df_historico['Precio Publico Estimado']=df_historico['Precio Publico Estimado'].str.replace(',', '').astype(float)


#--- TRATAMIENTO NULOS

#---STR
def vacios_str(columna):
  return columna.fillna('vacio').replace('', 'vacio')

columnas_object = list(df_historico.select_dtypes(include=['object']).columns)

for columna in columnas_object:
  df_historico[columna] = vacios_str(df_historico[columna])

#---FLOAT
def vacios_float(columna):
  return columna.fillna(0)
columnas_float = list(df_historico.select_dtypes(include=['float']).columns)
for columna in columnas_float:
  df_historico[columna] = vacios_float(df_historico[columna])

print("nulos")


#Se agrega una columna indice para el proceso de TDH Y Coppel
df_historico.reset_index(inplace=True)
df_historico.rename(columns={'index': 'num_fila'}, inplace=True)


## ---------------------- Marcas ------------------------------------------

lst_marca= [  
 'facom', 'iar expert', 'powers', 'troy-bilt', 'yard machine', 'no usar' ,
 'stanley', 'dewalt', 'black+decker', 'irwin', 'proto','bostitch', 'fatmax', 'porter cable', 
'lenox', 'craftsman',   'gridest' 
]

### -----------------Asignacion

df_historico['(I) MARCA'] = np.where((df_historico['(I) MARCA'] =='vacio') & (df_historico['(E) Marca'] != 'vacio'),
                               df_historico['(E) Marca'],
                               df_historico['(I) MARCA'])

df_historico['(I) MARCA'] = df_historico['(I) MARCA'].replace('vacio', 'other')

#-------------------------------------------------------------------------

correspondencias = {
    'black+decker': ['b/d','black & de', 'black and decker', 'black&decker', 'black & decker', 'black+decker', 'black+deck', 'black + decker', 'b&d', 'b+d', 'black decker', 'black-d', 'black&deck'],
    'dewalt': ['dewalt', 'de walt'],
    'fatmax': ['stanley fatmax', 'fat max', 'fatmax'],
    'bostitch': ['bosch', 'bostitch', 'bostitch office'],
    'craftsman':['craftsman','craftman'],
    'no usar': ['einhell','sierra','geo','samoa','smart','no usar']
}

df_historico['(I) MARCA'] = df_historico['(I) MARCA'].apply(lambda x: next((clave for clave, valor in correspondencias.items() if x in valor), x))
df_historico['(I) MARCA'] =df_historico['(I) MARCA'].apply(lambda x:'other' if x not in lst_marca else x)
df_historico['(I) MARCA'] =df_historico['(I) MARCA'].str.upper()

print("marcas")
## ------------------- SBU ------------------------

df_historico['(I) SBU'] = df_historico['(I) SBU'].fillna('oth').replace(['',' ','no definido','vacio','nan'], 'oth')

## ---------------- Canal Ecom-Tienda --------------------

lst_canal = 'internet|online|distancia|digital|virtual|ecommerce|e-com'
lst_retailer = 'mercado libre|e-comm|ecommerce|mercadolibre|amazon'

mask_retailer = df_historico['(L) Retailer'].str.contains(lst_retailer)
mask_canal = df_historico['(L) Local'].str.contains(lst_canal)
mask_ecom= (mask_retailer|mask_canal)

df_historico.loc[mask_ecom, 'canal_venta'] = 'ecommerce'

#mask_vacio = (df_historico['canal_venta'] == 'vacio')
#df_historico.loc[mask_vacio, 'canal_venta'] = 'tienda'

df_historico['canal_venta'] = df_historico['canal_venta'].replace(['vacio','moderno','tradicional','0','tienda'], 'store')
df_historico['canal_venta'] = df_historico['canal_venta'].replace(['ecommerce','e-commerce'], 'e-commerce')

print("sbu-canal venta")
#_________________________________________________
## --------------- Fin transformacion global -----
#_________________________________________________
#Hasta este punto se ha conseguido unificar toda la informacion segun:
#    País
#    Año
#    tipo ( semanal - mensual)
#Con sus respectivos ajustes de:
#    Marca
#    SBU
#    Canal (tienda - ecommerce)


#_________________________________________________
# ---------- TRANSFORMACION MEXICO -------
#_________________________________________________


### -------------- THD
#Para el caso de thd sucede que datamind reporte thd store y thd ecommerce, pero en tdh store
# esta reportando la venta total( store + ecommerce), por lo cuál se debe restar la venta 
#ecommercer a tdh store


#Se crea un df filtrado con TDH

df_thd = df_historico.loc[(df_historico['Country']=='Mexico')
                    & ((df_historico['(L) Retailer'] == 'the home depot') | (df_historico['(L) Retailer'] == 'the home depot e-comm') )
                    ]
df_thd= df_thd.reset_index(drop=True)

#Se crea una columna concat 

df_thd['concat_thd'] = df_thd['Tipo de dato'] + df_thd['Date'] +df_thd['(L) Local']+ df_thd['(I) Código Producto Interno']+ df_thd['canal_venta']
df_thd['concat_thd_ecom'] = df_thd['Tipo de dato'] + df_thd['Date'] +df_thd['(L) Local']+ df_thd['(I) Código Producto Interno']+ 'e-commerce'
#df_thd['countif'] = df_thd.groupby('concat_thd_ecom')['concat_thd'].transform('count')
counts = df_thd['concat_thd'].value_counts()
df_thd['countif'] = df_thd['concat_thd_ecom'].map(counts)
df_thd['concat_update'] = df_thd['Tipo de dato']+df_thd['Country'] + df_thd['Date'] +df_thd['(L) Retailer']
df_thd['countif'] = df_thd['countif'].replace(np.nan, 0).astype(int)

# -------------- ACTUALIZAR VENTA THD
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

#### Actualizacion historico TDH

df_thd.drop(['concat_thd','concat_thd_ecom', 'countif'],axis=1, inplace=True)

df_merged = pd.merge(df_historico[['num_fila']], df_thd[['num_fila', 'Venta neta']], on='num_fila', how='left')
df_historico['Venta neta'] = df_merged['Venta neta'].fillna(df_historico['Venta neta'])

print("the home depot")

# -------------------- COPEL 
#Para el caso de coppel sucede que se tiene reporte de unidades vendiadas pero no del monto de venta neeta
#que representan las unidades vendidas. Pero tomando en cuenta la base compartida por william se sabe el precio de las
#unidades, asi podemos obtener el valor de venta neta.

# Extraccion precios
ruta_coppel = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\VS Code Datamind\Data\Coppel\precios_historico_coppel.csv'    
df_coppel_precios = pd.read_csv(ruta_coppel)
df_coppel_precios['concat'] = df_coppel_precios['MODELO']+df_coppel_precios['P PUBLICO']
df_coppel_precios.drop_duplicates(subset=['concat'],inplace=True)

#Se crea un df filtrado con coppel

df_coppel = df_historico.loc[(df_historico['Country']=='Mexico')
                    & ((df_historico['(L) Retailer'] == 'coppel b')  )
                    ]
df_coppel= df_coppel.reset_index(drop=True)

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

# ACTUALIZA DATOS HISTORICOS CON INF DE COPPEL
df_merged = pd.merge(df_historico[['num_fila']], df_coppel[['num_fila', 'Venta neta']], on='num_fila', how='left')
df_historico['Venta neta'] = df_merged['Venta neta'].fillna(df_historico['Venta neta'])

print("coppel")

#_________________________________________________
#---------- COLUMNA UPDATE ---------------------------
#_________________________________________________

#------------ DATAMIND
df_historico['concat_update'] = df_historico['Country']+df_historico['Tipo de dato']+df_historico['Date']
df_historico['concat_update'] = df_historico['concat_update'].str.lower().str.strip()

#------------ MELI-AMZ
df_historico['concat_update_meli_amz'] = df_historico['Country']+df_historico['(L) Retailer']+df_historico['Tipo de dato']+df_historico['Date']
df_historico['concat_update_meli_amz'] = df_historico['concat_update'].str.lower().str.strip()

#_________________________________________________
#---------- CALENDAR ---------------------------
#_________________________________________________


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
df_historico['Fecha']=''
df_historico = df_historico.fillna('')
df_historico.loc[(df_historico['Tipo de dato'] == 'sem') & (df_historico['Fecha'] == ''), 'Fecha'] = df_historico.loc[df_historico['Tipo de dato'] == 'sem', 'Date'].apply(get_date_from_year_week)
df_historico.loc[(df_historico['Tipo de dato'] == 'men') & (df_historico['Fecha'] == ''), 'Fecha'] = df_historico.loc[df_historico['Tipo de dato'] == 'men', 'Date'].apply(get_date_from_year_month)
print("calendar")

#_________________________________________________
#---------- HOLDER ---------------------------
#_________________________________________________
df_historico['Holder']=''
notacion_holder = {
    'Mercado Libre': ['mercadolibre','mercado libre', 'mercado libre multivende', 'mercado libre spiral', 'mercado libre spiral ar'],
    'Sodimac': ['sodimac', 'sodimac mexico', 'sodimac argentina', 'sodimac uruguay', 'sodimac peru'],
    'Amazon': ['amazon mx'],
    'Walmart': ['walmart', 'walmart mexico', 'walmart argentina'],
    'Coppel': ['coppel b'],
    'The Home Depot': ['the home depot e-comm', 'the home depot'],
    'Easy': ['easy argentina'],
    'Carrefour':['carrefour argentina']
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
df_historico['Holder'] = df_historico.apply(holder, axis=1)
print("holder")

#___________________________
#--- VENTA USD 
#___________________________
ruta_fx_rate = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Shares Information for Projects\FX Rate\FX_Rate.xlsx'

df_historico['fecha_my'] = pd.to_datetime(df_historico['Fecha']).dt.strftime('%m%Y')

df_fx_rate = pd.read_excel(ruta_fx_rate)
df_fx_rate.drop_duplicates(subset=['Fecha', 'Country'], inplace=True) # Agregar inplace=True para modificar el dataframe original
df_fx_rate['Country']= df_fx_rate['Country'].apply(lambda x: 'Uruguay' if x=='PUB' else x)

df_fx_rate['Fecha'] = pd.to_datetime(df_fx_rate['Fecha'])
df_fx_rate['fecha_my'] = df_fx_rate['Fecha'].dt.strftime('%m%Y')
df_fx_rate = df_fx_rate[df_fx_rate['Country'].isin(df_historico['Country']) 
                          & df_fx_rate['fecha_my'].isin(df_historico['fecha_my'])]

df_historico = pd.merge(df_historico, df_fx_rate[['fecha_my', 'Country', 'Adjusted Rate']], on=["fecha_my", "Country"], how='left')
try:
    df_historico['Venta_neta_usd'] = df_historico['Venta neta'] / df_historico['Adjusted Rate']
except:
    df_historico['Venta_neta_usd'] = df_historico['Venta neta']

df_historico['Venta_neta_usd']=df_historico['Venta_neta_usd'].fillna(0)
df_historico.drop(['fecha_my', 'Adjusted Rate'],axis=1,inplace=True)
print("venta usd")

# ...............................................................
#Hasta este punto se tiene procesado en su totalidad el df_historico,


#___________________________________________________
# Guardar y actualizar
#___________________________________________________


# guardar archivo en el pc local
ruta_local_archivo_csv = r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Data Flow\df_datamind_historico.csv'
df_historico.to_csv(ruta_local_archivo_csv, index=False)

print("guarda archivo local")
# Guardar archivo en el drive
#ruta_drive_archivo_csv = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\Data Flow\df_datamind_historico.csv'
#df_historico.to_csv(ruta_drive_archivo_csv, index=False)
#print("guarda archivo drive")

print("se ha ejecutado correctamente")