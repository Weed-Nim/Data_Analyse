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
#---  Calculara Mediana      
def get_mediana(data):
    if (len(data) == 4):
        data.append(1)

    dOrder = sorted(data)
    mediana = None
    n = len(dOrder)
    if (n <= 3):
        return data[0]        
    else:        
        middle = n/2
        # codigo para calcular la mediana
        if (n%2 == 0):
            mediana = (dOrder[int(middle) + 1] + dOrder[int(middle) + 2]) / 2
        else:
            mediana = dOrder[int(middle) + 1] * 1  

        return mediana
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
#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
try:
    #---
    with connection.cursor() as cursor:
        #--- Consulta especifica
        sql = ("SELECT r.FECHA_RESERVA, r.FECHA_ENTRADA, r.FECHA_SALIDA, r.NO_PERSONAS, r.NO_NIÑOS, r.PRECIO, r.PROCEDENCIA, s.nombre, r.ESTADO FROM AV_RESERVAS r INNER JOIN bmsubcon s ON r.AGENCIA = s.numero WHERE r.ESTADO = 'FINALIZADA' OR r.ESTADO = 'EN CURSO' OR r.ESTADO = 'ACTIVA'")        
        cursor.execute(sql)
        RESULTADO = cursor.fetchall()
        #---
        #print(PORTAL)
        print('Correcto #1 -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error #1 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)

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


end_date = add_months(AIRBNB[0][1].date(),1)
month = BOOKING[0][1].date()
month = month.month
year = end_date.year

#---
def get_init_date(PORTAL):
    f_date = PORTAL[0][1].date()
    ini_date = []
    ini_date.append(f_date.month)
    ini_date.append(f_date.year)
    #---
    return ini_date
#----    
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
#---
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
#---
def get_date_diff(d_data):
    temp_resul = []
    for t_date in d_data:
        days = (t_date[2].date() - t_date[1].date())
        days = days.days
        if (days == 0):
            days = 1
        temp_resul.append(days)
    #---
    return temp_resul
#---
print('Mes: ',month,' - Año: ', year)
iteraciones = monthdelta(end_date,today)
print('Resta Meses: ', iteraciones)
#----
i = 0
#---- Mes y Año de Inicio
a_dates = get_init_date(AIRBNB)
b_dates = get_init_date(BOOKING)
e_dates = get_init_date(EXPEDIA)   
while (i < iteraciones):
    
    a_data = get_data_month(AIRBNB, a_dates[0], a_dates[1])   
    b_data = get_data_month(BOOKING, b_dates[0], b_dates[1]) 
    e_data = get_data_month(EXPEDIA, e_dates[0], e_dates[1])       
    #------------------- PRECIO
    #--- Media.
    print('AirBnB ---- Mes: ',a_dates[0],' - Año: ', a_dates[1])
    print('Booking ---- Mes: ',b_dates[0],' - Año: ', b_dates[1])
    print('Expedia ---- Mes: ',e_dates[0],' - Año: ', e_dates[1])
    #---
    if (len(a_data) != 0):
        a_precio = get_prom_price(a_data)
        a_media = get_media(a_precio)
        print('AirBnB Precio Medio = ',a_media)
    #---
    else:
        print('AirBnB Precio Medio = ',0)
    #---
    if (len(b_data) != 0):
        b_precio = get_prom_price(b_data)
        b_media = get_media(b_precio)
        print('Booking Precio Medio = ',b_media)
    #---
    else:
        print('Booking Precio Medio = ',0)
    #---
    if (len(e_data) != 0):
        e_precio = get_prom_price(e_data)
        e_media = get_media(e_precio)
        print('Expedia Precio Medio = ',e_media)
    #---
    else:
         print('Expedia Precio Medio = ',0)
    #---                    
    #------------------- RESERVA    
    #---
    if (len(a_data) != 0):
        #--- Media.               
        a_reserv_data = get_date_diff(a_data)
        print(a_reserv_data)
        a_media_reserva = get_media(a_reserv_data)
        print('AirBnB Reserva Medio = ',a_media_reserva)
        #--- Moda.
        a_moda_reserva = get_moda(a_reserv_data)
        print('AirBnB Reserva Moda = ',a_moda_reserva)
        #--- Mediana.
        a_mediana_reserva = get_mediana(a_reserv_data)
        print('AirBnB Reserva Mediana = ',a_mediana_reserva)
    #---
    else:
        #--- Media.
        print('AirBnB Reserva Medio = ',0)
        #--- Moda.
        print('AirBnB Reserva Moda = ',0)
        #--- Mediana.
        print('AirBnB Reserva Mediana = ',0)
    #---
    if (len(b_data) != 0):
        #--- Media.
        b_reserv_data = get_date_diff(b_data)
        print(b_reserv_data)
        b_media_reserva = get_media(b_reserv_data)
        print('Booking Reserva Medio = ',b_media_reserva)
        #--- Moda.
        b_moda_reserva = get_moda(b_reserv_data)
        print('Booking Reserva Moda = ',b_moda_reserva)
        #--- Mediana.
        b_mediana_reserva = get_mediana(b_reserv_data)
        print('Booking Reserva Mediana = ',b_mediana_reserva)
    #---
    else:
        #--- Media.
        print('Booking Reserva Medio = ',0)
        #--- Moda.
        print('Booking Reserva Moda = ',0)
        #--- Mediana.
        print('Booking Reserva Mediana = ',0)
    #---
    if (len(e_data) != 0):
        #--- Media.
        e_reserv_data = get_date_diff(e_data)
        print(e_reserv_data)
        e_media_reserva = get_media(e_reserv_data)
        print('Expedia Reserva Medio = ',e_media_reserva)
        #--- Moda.
        e_moda_reserva = get_moda(e_reserv_data)
        print('Expedia Reserva Moda = ',e_moda_reserva)
        #--- Mediana.
        e_mediana_reserva = get_mediana(e_reserv_data)
        print('Expedia Reserva Mediana = ',e_mediana_reserva)
    #---
    else:
        #--- Media.
        print('Expedia Reserva Medio = ',0)
        #--- Moda.
        print('Expedia Reserva Moda = ',0)
        #--- Mediana.
        print('Expedia Reserva Mediana = ',0)
    #---    

    #-------------------
    #--- Media.

    #--- Moda.

    #--- Mediana.
    if (a_dates[0] == 12):
        a_dates[0] = 1
        a_dates[1] = a_dates[1] + 1
    else:
        a_dates[0] = a_dates[0] + 1
    #---
    if (b_dates[0] == 12):
        b_dates[0] = 1
        b_dates[1] = b_dates[1] + 1
    else:
        b_dates[0] = b_dates[0] + 1
    #---
    if (e_dates[0] == 12):
        e_dates[0] = 1
        e_dates[1] = e_dates[1] + 1
    else:
        e_dates[0] = e_dates[0] + 1
    #---
    i += 1
