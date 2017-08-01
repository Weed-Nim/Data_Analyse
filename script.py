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


OPTIONS = []
today = (datetime.datetime.now()).date()
RESULTADO = []
RESULT_MODIF = []
TEMP_MODIF = []
A_LAST_RES = []
B_LAST_RES = []
E_LAST_RES = []
MONTH_DATA_UPD = []

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
    f_date = PORTAL[0][2].date()
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
        days = (t_precio[3].date() - t_precio[2].date())
        days = days.days
        if (days == 0):
            days = 1
        temp_resul.append((t_precio[6])/days)
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
        if (t_data[index] == None):
            temp_resul.append(0)
        else:
            temp_resul.append(t_data[index])
    #---
    return temp_resul
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
        #---
        sql = "SELECT ID_PORTAL, NOMBRE, URL, DIAS_VERIFICACION FROM SCR_PORTALES WHERE ESTADIST_CALC = 1"
        cursor.execute(sql)
        SETTING = cursor.fetchall()
        #--- Consulta especifica
        sql_1 = ("SELECT r.ID_RESERVA, r.FECHA_RESERVA, r.FECHA_ENTRADA, r.FECHA_SALIDA, r.NO_PERSONAS, r.NO_NIÑOS, r.PRECIO, r.PROCEDENCIA, s.nombre, r.ESTADO, r.PRECIOextra, r.COMISION, r.GASTOENERGIABAS, r.GASTOLIMREAL FROM AV_RESERVAS r INNER JOIN bmsubcon s ON r.AGENCIA = s.numero WHERE r.ESTADO = 'FINALIZADA' OR r.ESTADO = 'EN CURSO' OR r.ESTADO = 'ACTIVA' ORDER BY r.FECHA_ENTRADA ASC")                
        cursor.execute(sql_1)
        RESULTADO = cursor.fetchall()
        #---
        sql_2 = "SELECT r.ID_RESERVA, r.FECHA_RESERVA, r.FECHA_ENTRADA, r.FECHA_SALIDA,r.ESTADO, d.fecha as fecha_modificación,  d.nota, d.usuario, d.pc FROM foxclea_tareas.AV_RESERVAS r JOIN foxclea_tareas.bmdiarioa d ON r.ID_RESERVA = d.codigo WHERE cast(d.nota as nvarchar) is not null and cast(d.nota as nvarchar) <>'' AND d.fichero = 'RS' AND d.categoria = 'MD' AND r.ESTADO <> 'CANCELADA' AND r.ESTADO <> '' AND r.ESTADO <> 'NOSHOW' AND r.ESTADO <> '' AND r.ESTADO <> 'PENDIENTE' ORDER BY r.FECHA_ENTRADA ASC"
        cursor.execute(sql_2)
        TEMP_MODIF = cursor.fetchall()
        #---
        #print(PORTAL)
        print('Correcto #1 -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error #1 -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---

for s_nombre in SETTING:
    OPTIONS.append(s_nombre[1])
#----
def get_especific_data_site(name_s):
    DATA = []
    for r_data in RESULTADO:
        #---
        if ((str.lower(name_s) in str.lower(r_data[7])) or (str.lower(name_s) in str.lower(r_data[8]))):
            if (r_data[5] != None): #<--- Si no hay un precio definido, no se toma en concideracion la consulta
                DATA.append(r_data)
        
        #---
    return DATA
#----
for m_data in TEMP_MODIF:
    if (('entrada' in str.lower(m_data[6])) or ('salida' in str.lower(m_data[6])) or ('precio' in str.lower(m_data[6])) or ('no_personas' in str.lower(m_data[6])) or ('comision' in str.lower(m_data[6])) or ('limpieza' in str.lower(m_data[6])) or ('energia' in str.lower(m_data[6])) ):
        RESULT_MODIF.append(m_data)
#---
print("Datos Modificados")
print("Tamaño = ",len(RESULT_MODIF))
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
#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
def get_last_regis_mod(id_port):
    try:
        #---
        last_result = []
        with connection.cursor() as cursor:
            #--- Consulta especifica
            sql = "SELECT TOP * FROM SCR_PORTALES_MODF WHERE ID_PORTAL = %s ORDER BY [FECHA_MODIFICACION] ASC "
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
def get_portal_data(ID,PORTAL,nombre,days_verif):
    #---
    i = 0
    #--- 
    #---- Mes y Año de Inicio
    dates = []
    #---
    actual_month = (int(today.month) - 1)
    #---
    for i in range(len(PORTAL)):
        #---
        l_regis = get_last_regis(ID)
        NUM_RESERVA = None 
        PRECIO_MEDIA = None
        PRECIO_MEDIANA = None
        PRECIO_DESV = None
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
        PRECIOEXTRA_MEDIA = None
        PRECIOEXTRA_MODA = []
        PRECIOEXTRA_MEDIANA = None
        PRECIOEXTRA_DESV = None
        COMISION_MEDIA = None
        COMISION_MODA = []
        COMISION_MEDIANA = None
        COMISION_DESV = None
        GASTOENERGIABAS_MEDIA = None
        GASTOENERGIABAS_MODA = []
        GASTOENERGIABAS_MEDIANA = None
        GASTOENERGIABAS_DESV = None
        GASTOLIMREAL_MEDIA = None
        GASTOLIMREAL_MODA = []
        GASTOLIMREAL_MEDIANA = None
        GASTOLIMREAL_DESV = None
        #---
        allow = False
        end_ite = False
        UPDATE_VALUE = False
        #---
        if (l_regis == None):
            #---
            dates = get_init_date(PORTAL)
            end_ite = False 
            print('No hay registros anteriores')
        #---
        elif (l_regis[2] == actual_month and l_regis[3] == today.year):
            td_month = actual_month + 1            
            date_init_verif =  datetime.datetime.strptime(('01' + str(td_month) + str(today.year)), "%d%m%Y").date()
            date_end_verif =  date_init_verif + datetime.timedelta(days=days_verif)
            print('Fecha Fin: ',date_end_verif)
            print('Fecha Inicio: ',today)
            #---
            if (today < date_end_verif):
                allow = True
                end_ite = False                                 
                dates.append(l_regis[2])
                dates.append(l_regis[3])
                #l_regis[44] Fecha de modificacion del registro
            else:
                allow = False
                end_ite = True   
        #---
        elif (l_regis[3] < today.year):
            end_ite = False 
            if (l_regis[2] == 12):
                dates.append(1)
                dates.append(l_regis[3] + 1)
            else:                
                dates.append( l_regis[2] + 1)
                dates.append(l_regis[3])
        #---
        elif (l_regis[2] < actual_month and l_regis[3] == today.year):
            end_ite = False 
            if (l_regis[2] == 12):
                dates.append(1)
                dates.append(l_regis[3] + 1)
            else:                
                dates.append(l_regis[2] + 1)
                dates.append(l_regis[3])
                        
        #--
        if (end_ite == True):
            break     
        data = get_data_month(PORTAL, dates[0], dates[1])
        MONTH_DATA_UPD = []
        #---
        for data_check in data:
            for data_modf in RESULT_MODIF:
                if (data_check[0] == data_modf[0]):
                    MONTH_DATA_UPD = data_modf
        #---
        if (len(MONTH_DATA_UPD) == 0):
            i = 0
            while i <= 9:
                MONTH_DATA_UPD.append(None)
                i += 1
        elif (allow == True):
            print('FECHA DE REGISTRO = ',l_regis[44].date(),'--- FECHA DE NUEVO REGISTRO = ',MONTH_DATA_UPD[5].date())
            if (l_regis[44].date() < MONTH_DATA_UPD[5].date()):
                UPDATE_VALUE = True
                allow = True
            else:
                allow = False
                UPDATE_VALUE = False
                end_ite = True
                
        if (allow == False and end_ite == True):
            break
        #---
        print('-------------MODIFICACIÓN----------------')
        print(MONTH_DATA_UPD)
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
            #--- Media. 
            precio = get_prom_price(data)
            PRECIO_MEDIA = round(stats.mean(precio),2)
            print(nombre,' Precio Medio = ', PRECIO_MEDIA)
            #--- Mediana.
            PRECIO_MEDIANA = round(stats.median(precio), 2)
            print(nombre, ' Precio Mediana = ', PRECIO_MEDIANA)
            #--- Desviacion Standar.
            PRECIO_DESV = round(stats.pstdev(precio), 2)
            print(nombre, ' Precio Desviación Standard = ', PRECIO_DESV)
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
            reserv_data = get_date_diff(data, 3, 2)
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
            ant_reserv_data = get_date_diff(data, 2, 1)
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
            print(nombre, ' Antelación Reserva Desviación Standard = ', ANT_RESERVA_DES)
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
            print(nombre, ' Antelación Reserva Desviación Standard = ', 0)
        #---
        print('----') 
        #---
        #------------------- ADULTOS 
        #---
        print('-------------ADULTOS----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            adult_data = get_one_field_data(data, 4)
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
            child_data = get_one_field_data(data, 5)
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
        #------------------- PRECIO EXTRA
        #---
        print('-------------PRECIO EXTRA----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            precioextra_data = get_one_field_data(data, 10)
            PRECIOEXTRA_MEDIA = round(stats.mean(precioextra_data), 2)
            print(nombre, ' Precio Extra Medio = ', PRECIOEXTRA_MEDIA)
            #--- Moda.
            PRECIOEXTRA_MODA = get_moda(precioextra_data)
            print(nombre, ' Precio Extra Moda = ', PRECIOEXTRA_MODA)
            if (len(PRECIOEXTRA_MODA) == 1):
                PRECIOEXTRA_MODA.append(None)   
            #--- Mediana.
            PRECIOEXTRA_MEDIANA = round(stats.median(precioextra_data), 2)
            print(nombre, ' Precio Extra Mediana = ', PRECIOEXTRA_MEDIANA)
            #--- Desviacion Standar.
            PRECIOEXTRA_DESV = round(stats.pstdev(precioextra_data), 2)
            print(nombre, ' Precio Extra Standard = ', PRECIOEXTRA_DESV)
        #---
        else:
            #--- Media.
            print(nombre, ' Precio Extra Medio = ', 0)
            #--- Moda.
            print(nombre, ' Precio Extra Moda = ', 0)
            PRECIOEXTRA_MODA.append(None)
            PRECIOEXTRA_MODA.append(None)
            #--- Mediana.
            print(nombre, ' Precio Extra Mediana = ', 0)
            #--- Desviacion Standar..
            print(nombre, ' Precio Extra Standard = ', 0)
        #---
        print('----')       
        #---
        #------------------- COMISIÓN
        #---
        print('-------------COMISIÓN----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            comision_data = get_one_field_data(data, 11)
            COMISION_MEDIA = round(stats.mean(comision_data), 2)
            print(nombre, 'Comisión Medio = ', COMISION_MEDIA)
            #--- Moda.
            COMISION_MODA = get_moda(comision_data)
            print(nombre, 'Comisión Moda = ', COMISION_MODA)
            if (len(COMISION_MODA) == 1):
                COMISION_MODA.append(None)   
            #--- Mediana.
            COMISION_MEDIANA = round(stats.median(comision_data), 2)
            print(nombre, 'Comisión Mediana = ', COMISION_MEDIANA)
            #--- Desviacion Standar.
            COMISION_DESV = round(stats.pstdev(comision_data), 2)
            print(nombre, 'Comisión Standard = ', COMISION_DESV)
        #---
        else:
            #--- Media.
            print(nombre, 'Comisión Medio = ', 0)
            #--- Moda.
            print(nombre, 'Comisión Moda = ', 0)
            COMISION_MODA.append(None)
            COMISION_MODA.append(None)
            #--- Mediana.
            print(nombre, 'Comisión Mediana = ', 0)
            #--- Desviacion Standar..
            print(nombre, 'Comisión Standard = ', 0)
        #---
        print('----')
        #------------------- GASTO ENERGÍA
        #---
        print('-------------GASTO ENERGÍA----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            energia_data = get_one_field_data(data, 12)
            GASTOENERGIABAS_MEDIA = round(stats.mean(energia_data), 2)
            print(nombre, 'Gasto Energía Medio = ', GASTOENERGIABAS_MEDIA)
            #--- Moda.
            GASTOENERGIABAS_MODA = get_moda(energia_data)
            print(nombre, 'Gasto Energía Moda = ', GASTOENERGIABAS_MODA)
            if (len(GASTOENERGIABAS_MODA) == 1):
                GASTOENERGIABAS_MODA.append(None)   
            #--- Mediana.
            GASTOENERGIABAS_MEDIANA = round(stats.median(energia_data), 2)
            print(nombre, 'Gasto Energía Mediana = ', GASTOENERGIABAS_MEDIANA)
            #--- Desviacion Standar.
            GASTOENERGIABAS_DESV = round(stats.pstdev(energia_data), 2)
            print(nombre, 'Gasto Energía Standard = ', GASTOENERGIABAS_DESV)
        #---
        else:
            #--- Media.
            print(nombre, 'Gasto Energía Medio = ', 0)
            #--- Moda.
            print(nombre, 'Gasto Energía Moda = ', 0)
            GASTOENERGIABAS_MODA.append(None)
            GASTOENERGIABAS_MODA.append(None)
            #--- Mediana.
            print(nombre, 'Gasto Energía Mediana = ', 0)
            #--- Desviacion Standar..
            print(nombre, 'Gasto Energía Standard = ', 0)
        #---
        print('----')
        #------------------- GASTO LIMPIEZA
        #---
        print('-------------GASTO LIMPIEZA----------------')
        #---
        if (len(data) != 0):
            #--- Media.               
            limpieza_data = get_one_field_data(data, 13)
            GASTOLIMREAL_MEDIA = round(stats.mean(limpieza_data), 2)
            print(nombre, 'Gasto Limpieza Medio = ', GASTOLIMREAL_MEDIA)
            #--- Moda.
            GASTOLIMREAL_MODA = get_moda(limpieza_data)
            print(nombre, 'Gasto Limpieza Moda = ', GASTOLIMREAL_MODA)
            if (len(GASTOLIMREAL_MODA) == 1):
                GASTOLIMREAL_MODA.append(None)   
            #--- Mediana.
            GASTOLIMREAL_MEDIANA = round(stats.median(limpieza_data), 2)
            print(nombre, 'Gasto Limpieza Mediana = ', GASTOLIMREAL_MEDIANA)
            #--- Desviacion Standar.
            GASTOLIMREAL_DESV = round(stats.pstdev(limpieza_data), 2)
            print(nombre, 'Gasto Limpieza Standard = ', GASTOLIMREAL_DESV)
        #---
        else:
            #--- Media.
            print(nombre, 'Gasto Limpieza Medio = ', 0)
            #--- Moda.
            print(nombre, 'Gasto Limpieza Moda = ', 0)
            GASTOLIMREAL_MODA.append(None)
            GASTOLIMREAL_MODA.append(None)
            #--- Mediana.
            print(nombre, 'Gasto Limpieza Mediana = ', 0)
            #--- Desviacion Standar..
            print(nombre, 'Gasto Limpieza Standard = ', 0)
        #---
        print('----')
        #---
        #---
        #---
        if (UPDATE_VALUE == True):
            #--- Inicia la conección
            try:
                #---
                with connection.cursor() as cursor:
                    #--- Consulta especifica
                    part_one = "NUM_RESERVA = %s, PRECIO_MEDIA = %s, PRECIO_MEDIANA = %s, PRECIO_DESV = %s, RESERVA_MEDIA = %s, RESERVA_MODA_1 = %s, RESERVA_MODA_2 = %s, RESERVA_MEDIANA = %s, RESERVA_DESV = %s, ANT_RESERVA_MEDIA = %s, ANT_RESERVA_MODA_1 = %s, ANT_RESERVA_MODA_2 = %s, ANT_RESERVA_MEDIANA = %s, ANT_RESERVA_DES = %s, "
                    part_two = "ADULTOS_MEDIA = %s,ADULTOS_MODA_1 = %s, ADULTOS_MODA_2 = %s, NIÑOS_MEDIA = %s, NIÑOS_MODA = %s, PRECIOEXTRA_MEDIA = %s, PRECIOEXTRA_MODA_1 = %s, PRECIOEXTRA_MODA_2 = %s, PRECIOEXTRA_MEDIANA = %s, PRECIOEXTRA_DESV = %s, COMISION_MEDIA = %s, COMISION_MODA_1 = %s ,COMISION_MODA_2 = %s, COMISION_MEDIANA = %s, COMISION_DESV = %s, "
                    part_three = "GASTOENERGIABAS_MEDIA = %s, GASTOENERGIABAS_MODA_1 = %s, GASTOENERGIABAS_MODA_2 = %s, GASTOENERGIABAS_MEDIANA = %s, GASTOENERGIABAS_DESV = %s, GASTOLIMREAL_MEDIA = %s, GASTOLIMREAL_MODA_1 = %s, GASTOLIMREAL_MODA_2 = %s, GASTOLIMREAL_MEDIANA = %s, GASTOLIMREAL_DESV = %s, "
                    part_four = "ID_RS_ULT_MODF = %s, FECHA_MODIFICACION = %s, USUARIO = %s, DETALLE = %s, PC = %s, ACTUALIZADO = ([FOXCLEA_TAREAS].[ITIME](getdate())) "                    
                    sql = "UPDATE SCR_PORTALES_DETALLE  SET " + part_one + part_two + part_three + part_four + " WHERE ID = %s" 
                    print(sql)
                    cursor.execute(sql, (NUM_RESERVA,PRECIO_MEDIA,PRECIO_MEDIANA,PRECIO_DESV,RESERVA_MEDIA,RESERVA_MODA[0],RESERVA_MODA[1],RESERVA_MEDIANA,RESERVA_DESV,ANT_RESERVA_MEDIA,ANT_RESERVA_MODA[0],ANT_RESERVA_MODA[1],ANT_RESERVA_MEDIANA,ANT_RESERVA_DES,ADULTOS_MEDIA,ADULTOS_MODA[0],ADULTOS_MODA[1],NIÑOS_MEDIA,NIÑOS_MODA[0],PRECIOEXTRA_MEDIA,PRECIOEXTRA_MODA[0],PRECIOEXTRA_MODA[1],PRECIOEXTRA_MEDIANA,PRECIOEXTRA_DESV,COMISION_MEDIA,COMISION_MODA[0],COMISION_MODA[1],COMISION_MEDIANA,COMISION_DESV,GASTOENERGIABAS_MEDIA,GASTOENERGIABAS_MODA[0],GASTOENERGIABAS_MODA[1],GASTOENERGIABAS_MEDIANA,GASTOENERGIABAS_DESV,GASTOLIMREAL_MEDIA,GASTOLIMREAL_MODA[0],GASTOLIMREAL_MODA[1],GASTOLIMREAL_MEDIANA,GASTOLIMREAL_DESV,MONTH_DATA_UPD[0],MONTH_DATA_UPD[5],MONTH_DATA_UPD[7],MONTH_DATA_UPD[6],MONTH_DATA_UPD[8],l_regis[0]))                
                    #--- 
                    print('Correcto -> Registro Correcto del Log.')
                    connection.commit()
            except _mssql.MssqlDatabaseException as e:
                print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
        else:
            #--- Inicia la conección
            try:
                #---
                with connection.cursor() as cursor:
                    #--- Consulta especifica
                    part_one = "ID_PORTAL,MES,AÑO,NUM_RESERVA,PRECIO_MEDIA,PRECIO_MEDIANA,PRECIO_DESV,RESERVA_MEDIA,RESERVA_MODA_1,RESERVA_MODA_2,RESERVA_MEDIANA,RESERVA_DESV,ANT_RESERVA_MEDIA,ANT_RESERVA_MODA_1,ANT_RESERVA_MODA_2,ANT_RESERVA_MEDIANA,ANT_RESERVA_DES,"
                    part_two = "ADULTOS_MEDIA,ADULTOS_MODA_1,ADULTOS_MODA_2,NIÑOS_MEDIA,NIÑOS_MODA,PRECIOEXTRA_MEDIA,PRECIOEXTRA_MODA_1,PRECIOEXTRA_MODA_2,PRECIOEXTRA_MEDIANA,PRECIOEXTRA_DESV,COMISION_MEDIA,COMISION_MODA_1,COMISION_MODA_2,COMISION_MEDIANA,COMISION_DESV,"
                    part_three = "GASTOENERGIABAS_MEDIA,GASTOENERGIABAS_MODA_1,GASTOENERGIABAS_MODA_2,GASTOENERGIABAS_MEDIANA,GASTOENERGIABAS_DESV,GASTOLIMREAL_MEDIA,GASTOLIMREAL_MODA_1,GASTOLIMREAL_MODA_2,GASTOLIMREAL_MEDIANA,GASTOLIMREAL_DESV,"
                    part_four = "ID_RS_ULT_MODF,FECHA_MODIFICACION,USUARIO,DETALLE,PC"
                    part_five = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
                    sql = "INSERT INTO SCR_PORTALES_DETALLE (" + part_one + part_two + part_three + part_four + ") VALUES (" + part_five + ")" 
                    cursor.execute(sql, (ID,int(dates[0]),int(dates[1]),NUM_RESERVA,PRECIO_MEDIA,PRECIO_MEDIANA,PRECIO_DESV,RESERVA_MEDIA,RESERVA_MODA[0],RESERVA_MODA[1],RESERVA_MEDIANA,RESERVA_DESV,ANT_RESERVA_MEDIA,ANT_RESERVA_MODA[0],ANT_RESERVA_MODA[1],ANT_RESERVA_MEDIANA,ANT_RESERVA_DES,ADULTOS_MEDIA,ADULTOS_MODA[0],ADULTOS_MODA[1],NIÑOS_MEDIA,NIÑOS_MODA[0],PRECIOEXTRA_MEDIA,PRECIOEXTRA_MODA[0],PRECIOEXTRA_MODA[1],PRECIOEXTRA_MEDIANA,PRECIOEXTRA_DESV,COMISION_MEDIA,COMISION_MODA[0],COMISION_MODA[1],COMISION_MEDIANA,COMISION_DESV,GASTOENERGIABAS_MEDIA,GASTOENERGIABAS_MODA[0],GASTOENERGIABAS_MODA[1],GASTOENERGIABAS_MEDIANA,GASTOENERGIABAS_DESV,GASTOLIMREAL_MEDIA,GASTOLIMREAL_MODA[0],GASTOLIMREAL_MODA[1],GASTOLIMREAL_MEDIANA,GASTOLIMREAL_DESV,MONTH_DATA_UPD[0],MONTH_DATA_UPD[5],MONTH_DATA_UPD[7],MONTH_DATA_UPD[6],MONTH_DATA_UPD[8]))                
                    #--- 
                    print('Correcto -> Registro Correcto del Log.')
                    connection.commit()
            except _mssql.MssqlDatabaseException as e:
                print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
    return True

for s_item in SETTING:
    #---
    S_DATA = get_especific_data_site(s_item[1])
    #---
    get_data_site = get_portal_data(s_item[0],S_DATA,s_item[1],s_item[3])
    #---
    if (get_data_site == True):
        print('Datos de ', s_item[1],' Ingresados')
    #---
    else:
        print('No Hay Datos de ', s_item[1],)
    time.sleep(5)
#---

