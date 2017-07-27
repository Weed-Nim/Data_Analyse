#------------- Inician Imports. -------------#
import re
import sys
import time
import random
import datetime
from datetime import timedelta
import calendar
from calendar import monthrange
import os
import pymssql
import _mssql
from decimal import *
import statistics as stats
#------------- Finalizan Imports. -------------#
#---
#------------- Inicia Declaración de Variables Globales. -------------#
#------------- Inicia Configuración de BD. -------------#
#--- Variables de conexión a la base de datos.
connection = pymssql.connect(server='66.232.22.196',
                            user='FOXCLEA_TAREAS',
                            password='JACINTO2014',
                            database='FOXCLEA_TAREAS'
                            #charset='utf8mb4',
                            #cursorclass=pymssql.cursors.DictCursor
                           )
#---
#------------- Finaliza Configuración de BD. -------------#


OPTIONS = ['AIRBNB','BOOKING','EXPEDIA','APARSOL','PERSONALMENTE','MIRAI','HOMEAWAY','TELEFONO TFNO','EMAIL','CORREO','BUDGETPLACES','DIRECTO']
today = (datetime.datetime.now()).date()
RESULTADO = []
A_LAST_RES = []
B_LAST_RES = []
E_LAST_RES = []
BOOKING = []
AIRBNB = []
EXPEDIA = []

#--- Suma Meses a una Fecha
def add_months(sourcedate,months):
    #---
    month = sourcedate.month - 1 + months
    year = int(sourcedate.year + month / 12 )
    month = month % 12 + 1
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    #---
    return datetime.date(year,month,day)
#--- Resta de meses
def monthdelta(d1, d2):
    delta = 0
    while True:
        mdays = monthrange(d1.year, d1.month)[1]
        d1 += timedelta(days=mdays)
        if d1 <= d2:
            delta += 1
        else:
            break
    return delta

#--- Se obtiene la fecha del primer registro
def get_init_date(PORTAL):
    f_date = PORTAL[0][1].date()
    ini_date = []
    ini_date.append(f_date.month)
    ini_date.append(f_date.year)
    #---
    return ini_date
#----  Se saca el mes y el año de un registro
def get_data_month(PORTAL, month, year):
    #---
    datos = []   
    #---    
    #---   
    for d_portal in PORTAL:
        #--- 
        begin = d_portal[1].date()            
        #---                
        if ((begin.month == month) and (begin.year == year)):
            #---
            datos.append(d_portal)
    #---
    return datos
#--- se saca el precio por noche
def get_prom_price(p_data):
    temp_resul = []
    for t_precio in p_data:
        days = (t_precio[2].date() - t_precio[1].date())
        days = days.days
        if (days == 0):
            days = 1
        temp_resul.append((t_precio[5])/days)
    #---
    return temp_resul
#--- Se calcula el numero de días entres dos fechas
def get_date_diff(d_data, end, init):
    temp_resul = []
    for t_date in d_data:
        days = (t_date[end].date() - t_date[init].date())
        days = days.days
        if (days == 0):
            days = 1
        temp_resul.append(days)
    #---
    return temp_resul
#--- Se genera una lista con los datos de un campo especifico
def get_one_field_data(data, index):
    temp_resul = []
    for t_data in data:
        temp_resul.append(t_data[index])
    #---
    return temp_resul
#---
#---  Calculara Mediana      
def get_mediana(data):
    if (len(data) == 4):
        data.append(1)

    dOrder = sorted(data)
    mediana = None
    n = len(dOrder)
    if (n <= 3):
        s_data = sum(data)
        media = Decimal(s_data) * Decimal(1) / Decimal(n)
        media = round(media,2)
        #---
        return media        
    else:        
        middle = n/2
        # codigo para calcular la mediana
        if (n%2 == 0):
            mediana = (dOrder[int(middle) + 1] + dOrder[int(middle) + 2]) / 2
        else:
            mediana = dOrder[int(middle) + 1] * 1  

        return mediana
#---
#--- Calcular Media
def get_media(data):
    # codigo para calcular la media aritmetica
    dOrder = sorted(data)
    n = len(dOrder)
    s_data = sum(data)
    media = Decimal(s_data) * Decimal(1) / Decimal(n)
    media = round(media,2)
    #---
    return media
#---
#--- Calcular Moda
def get_moda(data):
    # codigo para calcular la moda
    repetir = 0                                                                         
    for i in data:                                                                              
        aparece = data.count(i)                                                             
        if (aparece > repetir):                                                       
            repetir = aparece                                                       
                                                                                            
    moda = []                                                                               
    for i in data:                                                                              
        aparece = data.count(i)                                                             
        if aparece == repetir and i not in moda:                                   
            moda.append(i)
    #---
    return moda
#---
#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
try:
    #---
    with connection.cursor() as cursor:
        #--- Consulta especifica
        sql = ("SELECT r.FECHA_RESERVA, r.FECHA_ENTRADA, r.FECHA_SALIDA, r.NO_PERSONAS, r.NO_NIÑOS, r.PRECIO, r.PROCEDENCIA, s.nombre, r.ESTADO FROM AV_RESERVAS r INNER JOIN bmsubcon s ON r.AGENCIA = s.numero WHERE r.ESTADO = 'FINALIZADA' OR r.ESTADO = 'EN CURSO' OR r.ESTADO = 'ACTIVA' ORDER BY r.FECHA_ENTRADA ASC")        
        cursor.execute(sql)
        RESULTADO = cursor.fetchall()
        #---
        #print(PORTAL)
        print('Correcto #1 -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error #1 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---
for r_data in RESULTADO:
    if ((str.lower(OPTIONS[0]) in str.lower(r_data[6])) or (str.lower(OPTIONS[0]) in str.lower(r_data[7]))):
        if (r_data[5] != None): #<--- Si no hay un precio definido, no se toma en concideracion la consulta
            AIRBNB.append(r_data)        
    elif ((str.lower(OPTIONS[1]) in str.lower(r_data[6])) or (str.lower(OPTIONS[1]) in str.lower(r_data[7]))):
        if (r_data[5] != None): #<--- Si no hay un precio definido, no se toma en concideracion la consulta
            BOOKING.append(r_data)
    elif ((str.lower(OPTIONS[2]) in str.lower(r_data[6])) or (str.lower(OPTIONS[2]) in str.lower(r_data[7]))):
        if (r_data[5] != None): #<--- Si no hay un precio definido, no se toma en concideracion la consulta
            EXPEDIA.append(r_data)

#----
print('airbnbn',len(AIRBNB))
print('booking',len(BOOKING))
print('expedia',len(EXPEDIA))

init_airbnb = (AIRBNB[0][1].date())
init_booking = (BOOKING[0][1].date())
init_expedia = (EXPEDIA[0][1].date())

print('Aibnb ',init_airbnb)
print('booking ',init_booking)
print('expedia ',init_expedia)

month = BOOKING[0][1].date()
month = month.month
year = init_booking.year

#----

#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
def get_last_regis(id_port):
    try:
        #---
        last_result = []
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "SELECT TOP 1 * FROM SCR_PORTALES_DETALLE WHERE ID_PORTAL = %s ORDER BY [ID] DESC "
            cursor.execute(sql, id_port)
            last_result = cursor.fetchone()
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
    print(last_result)
    return last_result

    #---
#---
def get_portal_data(ID,PORTAL,nombre):
    #---
    i = 0
    #--- 
    #---- Mes y Año de Inicio
    dates = None
    #---
    actual_month = (int(today.month) - 1)
    #---
    for i in range(len(PORTAL)):
        #---
        l_regis = get_last_regis(ID)
        NUM_RESERVA = None 
        PRECIO_MEDIA = None
        RESERVA_MEDIA = None
        RESERVA_MODA = []
        RESERVA_MEDIANA = None 
        RESERVA_DESV = None
        ANT_RESERVA_MEDIA = None
        ANT_RESERVA_MODA = []
        ANT_RESERVA_MEDIANA = None
        ANT_RESERVA_DES = None
        ADULTOS_MEDIA = None
        ADULTOS_MODA = []
        NIÑOS_MEDIA = None
        NIÑOS_MODA = []
        #---
        if (l_regis == None):
            #---
            dates = get_init_date(PORTAL)
            print('No hay registros anteriores')
        #---
        elif (l_regis[2] == actual_month and l_regis[3] == today.year):
            break
        #---
        elif (l_regis[3] < today.year):
            if (l_regis[2] == 12):
                dates[0] = 1
                dates[1] = l_regis[3] + 1
            else:                
                dates[0] = l_regis[2] + 1
                dates[1] = l_regis[3]
        #---
        elif (l_regis[2] < actual_month and l_regis[3] == today.year):
            if (l_regis[2] == 12):
                dates[0] = 1
                dates[1] = l_regis[3] + 1
            else:                
                dates[0] = l_regis[2] + 1
                dates[1] = l_regis[3]
            
            
        #---     
        data = get_data_month(PORTAL, dates[0], dates[1])       
        #------------------- PRECIO
        #--- Media.
        print('-------------------------------------------------')
        print(nombre, ' ---- Mes: ',dates[0],' - Año: ', dates[1])
        #---
        print('-------------NUM RESERVAS----------------')
        #---
        NUM_RESERVA = len(data)
        print('N Reservas ',nombre,' = ', NUM_RESERVA)
        #---
        print('-------------PRECIO----------------')
        #---
        if (len(data) != 0):
            precio = get_prom_price(data)
            PRECIO_MEDIA = round(stats.mean(precio),2)
            print(nombre,' Precio Medio = ', PRECIO_MEDIA)
        #---    
        else:
            print(nombre,' Precio Medio = ',0)
        #---
        print('----')
        #---                    
        #------------------- RESERVA    
        #---
        print('-------------RESERVA----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            reserv_data = get_date_diff(data, 2, 1)
            RESERVA_MEDIA = round(stats.mean(reserv_data), 2)
            print(nombre, ' Reserva Medio = ', RESERVA_MEDIA)
            #--- Moda.
            RESERVA_MODA = get_moda(reserv_data)
            print(nombre, ' Reserva Moda = ', RESERVA_MODA)
            if (len(RESERVA_MODA) == 1):
                RESERVA_MODA.append(None)                
            #--- Mediana.
            RESERVA_MEDIANA = round(stats.median(reserv_data), 2)
            print(nombre, ' Reserva Mediana = ', RESERVA_MEDIANA)
            #--- Desviacion Standar.
            RESERVA_DESV = round(stats.pstdev(reserv_data), 2)
            print(nombre, ' Reserva Desviación Standard = ', RESERVA_DESV)
        #---
        else:
            #--- Media.
            print(nombre, ' Reserva Medio = ', 0)
            #--- Moda.
            print(nombre, ' Reserva Moda = ', 0)
            RESERVA_MODA.append(None)
            RESERVA_MODA.append(None)
            #--- Mediana.
            print(nombre, ' Reserva Mediana = ', 0)
            #--- Desviacion Standar..
            print(nombre, ' Reserva Desviación Standard = ', 0)
        #---
        print('----')
        #---
       

        #------------------- ANTICIPACION RESERVA
        #---
        print('-------------ANTICIPACION RESERVA----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            ant_reserv_data = get_date_diff(data, 1, 0)
            ANT_RESERVA_MEDIA = round(stats.mean(ant_reserv_data), 2)
            print(nombre, ' Antelación Reserva Medio = ', ANT_RESERVA_MEDIA)
            #--- Moda.
            ANT_RESERVA_MODA = get_moda(ant_reserv_data)
            print(nombre, ' Antelación Reserva Moda = ', ANT_RESERVA_MODA)
            if (len(ANT_RESERVA_MODA) == 1):
                ANT_RESERVA_MODA.append(None)   
            #--- Mediana.
            ANT_RESERVA_MEDIANA = round(stats.median(ant_reserv_data), 2)
            print(nombre, ' Antelación Reserva Mediana = ', ANT_RESERVA_MEDIANA)
            #--- Desviacion Standar.
            ANT_RESERVA_DES = round(stats.pstdev(ant_reserv_data), 2)
            print(nombre, ' Reserva Desviación Standard = ', ANT_RESERVA_DES)
        #---
        else:
            #--- Media.
            print(nombre, ' Antelación Reserva Medio = ', 0)
            #--- Moda.
            print(nombre, ' Antelación Reserva Moda = ', 0)
            ANT_RESERVA_MODA.append(None)
            ANT_RESERVA_MODA.append(None)
            #--- Mediana.
            print(nombre, ' Antelación Reserva Mediana = ', 0)
            #--- Desviacion Standar..
            print(nombre, ' Reserva Desviación Standard = ', 0)
        #---
        print('----')
       
        #---
        #------------------- ADULTOS 
        #---
        print('-------------ADULTOS----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            adult_data = get_one_field_data(data, 3)
            ADULTOS_MEDIA = round(stats.mean(adult_data), 2)
            print(nombre, ' Adultos Medio = ',ADULTOS_MEDIA)
            #--- Moda.
            ADULTOS_MODA = get_moda(adult_data)
            print(nombre, ' Adultos Moda = ',ADULTOS_MODA)
            if (len(ADULTOS_MODA) == 1):
                ADULTOS_MODA.append(None)  
        #---
        else:
            #--- Media.
            print(nombre, ' Adultos Medio = ',0)
            #--- Moda.
            print(nombre, ' Adultos Moda = ',0)
            ADULTOS_MODA.append(None)
            ADULTOS_MODA.append(None)
        #---
        print('----')
     
        #---
        #------------------- NIÑOS 
        #---
        print('-------------NIÑOS----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            child_data = get_one_field_data(data, 4)
            NIÑOS_MEDIA = round(stats.mean(child_data), 2)
            print(nombre, ' Niños Medio = ', NIÑOS_MEDIA)
            #--- Moda.
            NIÑOS_MODA = get_moda(child_data)
            print(nombre, ' Niños Moda = ',NIÑOS_MODA)
            if (len(NIÑOS_MODA) == 1):
                NIÑOS_MODA.append(None)
        #---
        else:
            #--- Media.
            print(nombre, ' Niños Medio = ',0)
            #--- Moda.
            print(nombre, ' Niños Moda = ',0)
            NIÑOS_MODA.append(None)
            NIÑOS_MODA.append(None)
        #---
        print('----')
       
        #---
        #--- Inicia la conección
        try:
            #---
            with connection.cursor() as cursor:
                #--- Consulta especifica
                sql = "INSERT INTO SCR_PORTALES_DETALLE (ID_PORTAL,MES,AÑO,NUM_RESERVA,PRECIO_MEDIA,RESERVA_MEDIA,RESERVA_MODA_1,RESERVA_MODA_2,RESERVA_MEDIANA,RESERVA_DESV,ANT_RESERVA_MEDIA,ANT_RESERVA_MODA_1,ANT_RESERVA_MODA_2,ANT_RESERVA_MEDIANA,ANT_RESERVA_DES,ADULTOS_MEDIA,ADULTOS_MODA_1,ADULTOS_MODA_2,NIÑOS_MEDIA,NIÑOS_MODA) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" 
                cursor.execute(sql, (ID,int(dates[0]),int(dates[1]),NUM_RESERVA,PRECIO_MEDIA,RESERVA_MEDIA,RESERVA_MODA[0],RESERVA_MODA[1],RESERVA_MEDIANA,RESERVA_DESV,ANT_RESERVA_MEDIA,ANT_RESERVA_MODA[0],ANT_RESERVA_MODA[1],ANT_RESERVA_MEDIANA,ANT_RESERVA_DES,ADULTOS_MEDIA,ADULTOS_MODA[0],ADULTOS_MODA[0],NIÑOS_MEDIA,NIÑOS_MODA[0]))                
                #---                            
                print('Correcto -> Registro Correcto del Log.')
                connection.commit()
        #---
        except _mssql.MssqlDatabaseException as e:
            print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity) 
       
        

    return True

get_data_airbnb = get_portal_data(1,AIRBNB,'AirBnB')
if (get_data_airbnb == True):
    print('Datos de Airbnb')
    time.sleep(10)
get_data_booking = get_portal_data(2,BOOKING,'Booking')
if (get_data_booking == True):
    print('Datos de Booking')
    time.sleep(10)
get_data_expedia = get_portal_data(3,EXPEDIA,'Expedia')
if (get_data_expedia == True):
    print('Datos de Expedia')
    time.sleep(10)