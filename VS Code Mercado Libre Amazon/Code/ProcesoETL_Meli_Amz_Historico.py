print("""
.--------------------------------------------------------.
|                                                        |
|  __  __ _____ _     ___              _    __  __ _____ |
| |  \/  | ____| |   |_ _|            / \  |  \/  |__  / |
| | |\/| |  _| | |    | |   _____    / _ \ | |\/| | / /  |
| | |  | | |___| |___ | |  |_____|  / ___ \| |  | |/ /_  |
| |_|  |_|_____|_____|___|         /_/   \_\_|  |_/____| |
|                                                        |
'--------------------------------------------------------'
""")
#________________
#--- LIBRERIAS --
#________________
import pandas as pd

#Permite buscar y recuperar una lista de nombres de archivos que coinciden con un patrón específico de nombre
#de archivo en un directorio o en una jerarquía de directorios.
import glob

import numpy as np
import os
from datetime import datetime, timedelta
print("librerias")
#__________________________
#--- EXTRACCION DATOS -----
#__________________________



#. utilizamos la función glob para crear una lista de rutas de archivo que coinciden con el patrón *Chile Mensual*.csv
#. en el directorio path_chi_men. Esto nos da una lista de todas las rutas de archivo que cumplen con el patrón en el
#. directorio.


#--- AMAZON 



#-------BRASIL ------------
paht_data_historica_amz_bra= r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Data Mercado Libre - Amazon\Amazon_Vendor_Data\new_sales_input\Brazil\Sales_Manufacturing'
all_files_amz_bra = glob.glob(paht_data_historica_amz_bra + "/*Sales_Manufacturing_Retail_Brazil*.csv")

#-------MEXICO ------------
paht_data_historica_amz_mex= r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Data Mercado Libre - Amazon\Amazon_Vendor_Data\new_sales_input\Mexico\Sales_Manufacturing'
all_files_amz_mex = glob.glob(paht_data_historica_amz_mex + "/*Sales_Manufacturing_Retail_Mexico*.csv")


#---MERCADO LIBRE

paht_data_historica_meli= r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Data Mercado Libre - Amazon\meli_historic_data\historic_data_meli2.csv'


#__________________
#--- APPENED LIST -
#__________________


#--- AMAZON 

ls_amz_bra = []
ls_amz_mex = []

# Se anexan los archivos en una sola lista

for filename in all_files_amz_bra:
    df = pd.read_csv(filename, index_col=None, header=1,dtype=str)
     # Obtener el nombre del archivo sin la extensión
    file_name = os.path.splitext(os.path.basename(filename))[0]
    # Agregar una nueva columna con el nombre del archivo
    df['Archivo'] = file_name
    ls_amz_bra.append(df)


for filename in all_files_amz_mex:
    df = pd.read_csv(filename, index_col=None, header=1,dtype=str)
    # Obtener el nombre del archivo sin la extensión
    file_name = os.path.splitext(os.path.basename(filename))[0]
    # Agregar una nueva columna con el nombre del archivo
    df['Archivo'] = file_name
    ls_amz_mex.append(df)



#--- AMAZON 

df_amz_bra = pd.concat(ls_amz_bra, axis=0, ignore_index=True)
df_amz_mex = pd.concat(ls_amz_mex, axis=0, ignore_index=True)


#--- MERCADO LIBRE
df_meli = pd.read_csv(paht_data_historica_meli, dtype=str, encoding='UTF-8',sep=",", quotechar='"',decimal='.')

print("unifico info")
#_____________
#---CONCAT ---
#_____________

#_______________________
#--- AMAZON 
#_______________________
df_amz_bra['Date'] = df_amz_bra['Archivo'].apply(lambda x:x[-6:] if len(x) >= 5 else None)
df_amz_bra['Tipo de dato'] ='sem'

df_amz_mex['Date'] = df_amz_mex['Archivo'].apply(lambda x:x[-6:] if len(x) >= 5 else None)
df_amz_mex['Tipo de dato'] ='sem'

df_amz_mex=df_amz_mex[~df_amz_mex['Date'].str.startswith('2024')]
df_amz_bra=df_amz_bra[~df_amz_bra['Date'].str.startswith('2024')]

df_amz_bra['Country'] ='Brasil'
df_amz_mex['Country'] = 'Mexico'

#se concatena la informacion de brasil y mexico
df_amz=pd.concat([df_amz_bra,df_amz_mex])


#_______________________
#--- MERCADO LIBRE
#_______________________
df_meli['YEAR'] = df_meli['YEAR'].astype(str)
df_meli['WEEK'] = df_meli['WEEK'].astype(str)
df_meli['Date'] = df_meli['YEAR']+df_meli['WEEK']
df_meli['Tipo de dato']='sem'

df_meli=df_meli[df_meli['Date'].str.startswith('2023')]


df_meli['COUNTRY'] = df_meli['COUNTRY'].str.strip().str.capitalize()
df_meli.rename(columns={'COUNTRY': 'Country'}, inplace=True)

conditions = [
    df_meli['Country'] == 'Brazil',
    df_meli['Country'] == 'México',    
    df_meli['Country'] == 'Mexico',
    df_meli['Country'] == 'MÃ©xico',
    df_meli['Country'] == 'Peru',
    df_meli['Country'] == 'Perú',
    df_meli['Country'] == 'PerÃº',
    
]
choices = [
    'Brasil',
    'Mexico',
    'Mexico',
    'MÃ©xico',
    'Peru',
    'Peru',
    'PerÃº'
]
df_meli['Country'] = np.select(conditions, choices, default=df_meli['Country'])
print("date-pais")

#_____________________
#--- TRANSFORMACION --
#_____________________


#_______________________
#--- AMAZON 
#_______________________
rename_col_amz={'ASIN':'(I) Código Producto Interno',
            'Product Title':'(I) Producto Interno',
            'Brand':'(I) MARCA',
            'Shipped Units':'Unidades vendidas',
            'Shipped Revenue':'Venta bruta'}
df_amz.rename(columns=rename_col_amz,inplace=True)
#df_amz.columns

#_______________________
#--- MERCADO LIBRE 
#_______________________
rename_col_meli={'marca':'(I) MARCA',
                 'PRODUCT_NAME':'(I) Producto Interno',
                 'MODEL':'(I) Código Producto Interno',
                 'TSI_FCST':'Unidades vendidas',
                 'TGMV_FCST':'Venta bruta'}
df_meli.rename(columns=rename_col_meli,inplace=True)

#_____________________________
# --- EXTRACCION COL A USAR --
#______________________________
# se escojen las columnas comunes a la informacion que suministra datamind

#_______________________
#--- AMAZON 
#_______________________
cols=['(I) Código Producto Interno', '(I) Producto Interno', 'Venta bruta', 
       'Unidades vendidas', '(I) MARCA','Country', 'Date','Tipo de dato']

df_amz =df_amz[cols]

#Se ordenan segun el orden de datamind
order_col=['Date','(I) MARCA','(I) Producto Interno','(I) Código Producto Interno','Venta bruta','Unidades vendidas','Tipo de dato','Country']
df_amz = df_amz.reindex(columns=order_col)

#_______________________
#--- MERCADO LIBRE 
#_______________________

df_meli =df_meli[cols]
df_meli = df_meli.reindex(columns=order_col)


col_chi=(['num_fila',
       'Date',
       '(L) Retailer',
       '(L) Local',
       '(L) Cadena',
       'canal_venta',
       '(I) SBU', 
       '(I) MARCA',
       '(E) Marca',
       '(I) NPI',
       '(I) GPP Division',
       '(I) GPP Division Cod.',
       '(I) GPP Category',
       '(I) GPP Category Cod.',
       '(I) GPP Portfolio',
       '(I) GPP Portfolio Cod.',
       '(I) Producto Interno', 
       '(I) Código Producto Interno',
       '(I) OGSM Strategy',
       '(I) CORD / CORDLESS / COMB / NEUM',
       'Venta neta',
       'Venta bruta',
       'Venta costo',
       'Unidades vendidas',
       'Volumen vendido (Capacidad 1)',
       'Precio Publico Estimado',
       'Tipo de dato', 
       'Country',
       'concat_update',
       'concat_update_meli_amz'])

print("col a usar")
#______________________
#--- COLUMNAS VACIAS --
#______________________


#_______________________
#--- AMAZON 
#_______________________

#Funcion que inserta columna vacia 
def crear_columnas_vacias(df, columnas, posiciones):
    for col, pos in zip(columnas, posiciones):
        df.insert(loc=pos, column=col, value='')
columnas_vacias = ["vacia0",
                    "vacia2", "vacia3", "vacia4", "vacia5", "vacia6",
                    "vacia8","vacia9","vacia10","vacia11","vacia12","vacia13","vacia14","vacia15",
                    "vacia18","vacia19","vacia20"
                    "vacia22",
                    "vacia24","vacia25",
                    "vacia28","vacia29"]
posiciones = [0, 2, 3,4,5,6,8,9,10,11,12,13,14,15,18,19,20,22,24,25,28,29]

crear_columnas_vacias(df_amz, columnas_vacias, posiciones)


#_______________________
#---  MERCADO LIBRE 
#_______________________
#Funcion que inserta columna vacia 
def crear_columnas_vacias(df, columnas, posiciones):
    for col, pos in zip(columnas, posiciones):
        df.insert(loc=pos, column=col, value='')


crear_columnas_vacias(df_meli, columnas_vacias, posiciones)

print("col vacias")
#___________________
#--- RENAME COL ----
#___________________

#_______________________
#--- AMAZON 
#_______________________
col_amz=df_amz.columns.to_list()

dict_renombres_amz = {nombre_amz: nombre_chi for nombre_amz, nombre_chi in zip(col_amz, col_chi)}
df_amz.rename(columns=dict_renombres_amz, inplace=True)  # renombrar las columnas utilizando el diccionario

#_______________________
#--- MERCADO LIBRE 
#_______________________
col_meli=df_meli.columns.to_list()

dict_renombres_meli = {nombre_meli: nombre_chi for nombre_meli, nombre_chi in zip(col_meli, col_chi)}
df_meli.rename(columns=dict_renombres_meli, inplace=True)  # renombrar las columnas utilizando el diccionario

print("organizo col")
#____________________
#--- COMPLETAR COL --
#____________________


#_______________________
#--- AMAZON 
#_______________________
df_amz['(L) Retailer'] = 'Amazon'
df_amz['canal_venta']='E-commerce'

#--------- VENTA NETA ---------------

#Tratamiento de quitar letras al valor

df_amz['Venta bruta'] = df_amz['Venta bruta'].astype(str)

df_amz['Venta bruta'] = df_amz['Venta bruta'].str.replace(r'^MX\$', '', regex=True)
df_amz['Venta bruta'] = df_amz['Venta bruta'].str.replace(r'^R\$', '', regex=True)
df_amz['Venta bruta'] = df_amz['Venta bruta'].str.replace(',', '').fillna('0').astype(float)
df_amz['Venta neta'] = df_amz['Venta neta'].str.replace('','0').fillna('0').astype(float)
#_______________________
#--- MERCADO LIBRE 
#_______________________
df_meli['(L) Retailer'] = 'Mercado Libre'
df_meli['canal_venta'] ='E-commerce'

#--------- VENTA NETA ---------------
df_meli['Venta bruta'] = df_meli['Venta bruta'].str.replace(',', '.').fillna('0').astype(float)
df_meli['Venta neta'] =df_meli['Venta neta'].str.replace('','0').fillna('0').astype(float)
print("tratamiento valores num")
#_________________________
#---- DF_MELI_AMZ --------
#_________________________
df_meli_amz= pd.concat([df_meli,df_amz])


#__________________
# --- TRATAR COL --
#__________________


df_meli_amz['Date'] = df_meli_amz['Date'].astype(str)

df_meli_amz['(I) MARCA'] = df_meli_amz['(I) MARCA'].astype(str).str.lower().fillna('vacio').replace('nan','vacio')

df_meli_amz['(I) Código Producto Interno'] = df_meli_amz['(I) Código Producto Interno'].astype(str).str.lower()

df_meli_amz['Date'] = df_meli_amz['Date'].apply(lambda x: x[:4] + '0' + x[4:] if len(x) == 5 else x)

#--- TRATAMIENTO NULOS
def vacios_str(columna):
  return columna.fillna('0').replace('', '0')

columnas_object = list(df_meli_amz.select_dtypes(include=['object']).columns)

for columna in columnas_object:
  df_meli_amz[columna] = vacios_str(df_meli_amz[columna])

def vacios_float(columna):
  return columna.fillna(0).replace('', 0)
columnas_float = list(df_meli_amz.select_dtypes(include=['float']).columns)
for columna in columnas_float:
  df_meli_amz[columna] = vacios_float(df_meli_amz[columna])

col_0=['Venta costo', 'Volumen vendido (Capacidad 1)', 'Precio Publico Estimado']
df_meli_amz.loc[:, col_0] = 0


#______________
# --- MARCAS --
#______________
lst_marca= [  
 'facom', 'iar expert', 'powers', 'troy-bilt', 'yard machine', 'no usar' ,
 'stanley', 'dewalt', 'black+decker', 'irwin', 'proto','bostitch', 'fatmax', 'porter cable', 
'lenox', 'craftsman',   'gridest' 
]


#_______________________
#--- Asignacion
#_______________________

correspondencias = {
    'black+decker': ['b/d','black & de', 'black and decker', 'black&decker', 'black & decker', 'black+decker', 'black+deck', 'black + decker', 'b&d', 'b+d', 'black decker', 'black-d', 'black&deck'],
    'dewalt': ['dewalt', 'de walt'],
    'fatmax': ['stanley fatmax', 'fat max', 'fatmax'],
    'bostitch': ['bosch', 'bostitch', 'bostitch office'],
    'craftsman':['craftsman','craftman'],
    'no usar': ['einhell','sierra','geo','samoa','smart','no usar']
}

df_meli_amz['(I) MARCA'] = df_meli_amz['(I) MARCA'].apply(lambda x: next((clave for clave, valor in correspondencias.items() if x in valor), x))
df_meli_amz['(I) MARCA'] =df_meli_amz['(I) MARCA'].apply(lambda x:'other' if x not in lst_marca else x)
#df_meli_amz['(I) MARCA'] =df_meli_amz['(I) MARCA'].str.upper()




#______________________________
#-- TRATAMIENTO MARCAS OTHERS
#______________________________
df_meli_amz['(I) Producto Interno']=df_meli_amz['(I) Producto Interno'].str.lower().str.strip()
df_meli_amz['(I) MARCA']=df_meli_amz['(I) MARCA'].str.lower().str.strip()

#reasigno marca a aquellos que estas como other basado en la descripcion del producto
def assign_brand(description, current_brand):    
    if current_brand == 'other':  # Apply logic only if current brand is 'other'
        for brand, keywords in correspondencias.items():
            if any(keyword in description.lower() for keyword in keywords):
                return brand
    return current_brand  # Return the existing brand if not 'other'

# Apply the function to 'descripcion' column using 'marca' as the argument
df_meli_amz['(I) MARCA'] = df_meli_amz[['(I) Producto Interno', '(I) MARCA']].apply(lambda row: assign_brand(row['(I) Producto Interno'], row['(I) MARCA']), axis=1)

print("marca")
#_______________
#----- SBU  ----
#_______________
 
df_meli_amz['(I) SBU'] = df_meli_amz['(I) SBU'].fillna('oth').replace(['',' ','0','no definido','vacio','nan'], 'oth')
print("sbu")

print("sbu")
#_______________
#--- COL UPD ---
#_______________

df_meli_amz['concat_update_meli_amz'] = df_meli_amz['Country']+df_meli_amz['(L) Retailer']+df_meli_amz['Tipo de dato']+df_meli_amz['Date']
df_meli_amz['concat_update'] = df_meli_amz['concat_update'].str.lower().str.strip()
print("col upd")
#_________________
#--- CALENDAR *---
#_________________
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
df_meli_amz['Fecha']=''
#df_meli_amz = df_meli_amz.fillna('')
df_meli_amz.loc[(df_meli_amz['Tipo de dato'] == 'sem') & (df_meli_amz['Fecha'] == ''), 'Fecha'] = df_meli_amz.loc[df_meli_amz['Tipo de dato'] == 'sem', 'Date'].apply(get_date_from_year_week)
df_meli_amz.loc[(df_meli_amz['Tipo de dato'] == 'men') & (df_meli_amz['Fecha'] == ''), 'Fecha'] = df_meli_amz.loc[df_meli_amz['Tipo de dato'] == 'men', 'Date'].apply(get_date_from_year_month)

print("calendar")
#_________________________________________________
#---------- HOLDER ---------------------------
#_________________________________________________
df_meli_amz['(L) Retailer']=df_meli_amz['(L) Retailer'].str.lower()
df_meli_amz['Holder']=''
notacion_holder = {
    'mercado libre': ['mercadolibre', 'mercado libre multivende', 'mercado libre spiral', 'mercado libre spiral ar'],
    'amazon': ['amazon mx']}
  
notacion_pais = {
    'Mexico': 'MX',
    'México':'MX',
    'Chile': 'CH',
    'Argentina': 'AR',
    'Uruguay': 'UR',
    'Peru': 'PE',
    'Perú': 'PE',
    'Brazil': 'BR',
    'Brasil': 'BR',
     'Ecuador': 'EC',
     'Colombia':'CO'
    
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
df_meli_amz['Holder'] = df_meli_amz.apply(holder, axis=1)
print("holder")


#___________________________
#VENTA USD 
#___________________________

df_meli_amz['fecha_my'] = pd.to_datetime(df_meli_amz['Fecha']).dt.strftime('%m%Y')

ruta_gto_net = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Shares Information for Projects\GsTo_Net\GtoNet.xlsx'

df_gto_net = pd.read_excel(ruta_gto_net)
df_gto_net.drop_duplicates(subset=['Fecha', 'Country'], inplace=True) # Agregar inplace=True para modificar el dataframe original
df_gto_net['Fecha'] = pd.to_datetime(df_gto_net['Fecha'])
df_gto_net['fecha_my'] = df_gto_net['Fecha'].dt.strftime('%m%Y')
df_gto_net = df_gto_net[df_gto_net['Country'].isin(df_meli_amz['Country']) 
                          & df_gto_net['fecha_my'].isin(df_meli_amz['fecha_my'])]

df_meli_amz = pd.merge(df_meli_amz, df_gto_net[['fecha_my', 'Country', 'GTN%']], on=["fecha_my", "Country"], how='left')
df_meli_amz['GTN%'] = df_meli_amz['GTN%'].fillna(0)


meli_amz_mask = (df_meli_amz['(L) Retailer'] != '0') & (df_meli_amz['Venta bruta'] != 0)&(df_meli_amz['GTN%'] != 0)
gtn = df_meli_amz.loc[meli_amz_mask, 'GTN%']
venta_bruta = df_meli_amz.loc[meli_amz_mask, 'Venta bruta']
df_meli_amz.loc[meli_amz_mask, 'Venta neta'] = venta_bruta * (1 - gtn)


meli_amz_mask2 = (df_meli_amz['(L) Retailer'] != '0') & (df_meli_amz['Venta bruta'] != 0)&(df_meli_amz['GTN%'] == 0)
gtn = np.full(len(meli_amz_mask2), 0.74)
venta_bruta = df_meli_amz.loc[meli_amz_mask2, 'Venta bruta']
df_meli_amz.loc[meli_amz_mask2, 'Venta neta'] = venta_bruta * (1 - 0.0714)


ruta_fx_rate = r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Shares Information for Projects\FX Rate\FX_Rate.xlsx'
print("ok")
df_fx_rate = pd.read_excel(ruta_fx_rate)
df_fx_rate.drop_duplicates(subset=['Fecha', 'Country'], inplace=True) # Agregar inplace=True para modificar el dataframe original
df_fx_rate['Country']= df_fx_rate['Country'].apply(lambda x: 'Uruguay' if x=='PUB' else x)

df_fx_rate['Fecha'] = pd.to_datetime(df_fx_rate['Fecha'])
df_fx_rate['fecha_my'] = df_fx_rate['Fecha'].dt.strftime('%m%Y')
df_fx_rate = df_fx_rate[df_fx_rate['Country'].isin(df_meli_amz['Country']) 
                          & df_fx_rate['fecha_my'].isin(df_meli_amz['fecha_my'])]

df_meli_amz = pd.merge(df_meli_amz, df_fx_rate[['fecha_my', 'Country', 'Adjusted Rate']], on=["fecha_my", "Country"], how='left')
try:
    df_meli_amz['Venta_neta_usd'] = df_meli_amz['Venta neta'] / df_meli_amz['Adjusted Rate']
except:
    df_meli_amz['Venta_neta_usd'] = df_meli_amz['Venta neta']
col=['num_fila', 'Date', '(L) Retailer', '(L) Local', '(L) Cadena',
       'canal_venta', '(I) SBU', '(I) MARCA', '(E) Marca', '(I) NPI',
       '(I) GPP Division', '(I) GPP Division Cod.', '(I) GPP Category',
       '(I) GPP Category Cod.', '(I) GPP Portfolio', '(I) GPP Portfolio Cod.',
       '(I) Producto Interno', '(I) Código Producto Interno',
       '(I) OGSM Strategy', '(I) CORD / CORDLESS / COMB / NEUM', 'Venta neta',
       'Venta bruta', 'Venta costo', 'Unidades vendidas',
       'Volumen vendido (Capacidad 1)', 'Precio Publico Estimado',
       'Tipo de dato', 'Country', 'concat_update', 'concat_update_meli_amz',
       'Fecha', 'Holder', 'Venta_neta_usd']
df_meli_amz = df_meli_amz[col]
print("gtonet - fx rates")
print(df_meli_amz.columns)
#Guardar

#Local
ruta_meli_amz_historico_local=r'C:\Users\SSN0609\Documents\Dashboards LAG PC LOCAL\Data Flow\Datamind\Data Flow\meli_amz_historico.csv'
df_meli_amz.to_csv(ruta_meli_amz_historico_local)
print("local")
#Drive
ruta_meli_amz_historico_drive=r'C:\Users\SSN0609\OneDrive - Stanley Black & Decker\Dashboards LAG\Data Flow\Datamind\Data Flow\meli_amz_historico.csv'
df_meli_amz.to_csv(ruta_meli_amz_historico_drive)
print("drive")
