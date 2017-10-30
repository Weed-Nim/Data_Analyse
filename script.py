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
#--- Variables 
OPTIONS = [] #<--- Se almacenan los sitios que se analizaran, Ex: Airbnb, Booking, Expedia,etc.
today = (datetime.datetime.now()).date() #<--- Fecha de hoy.
RESULTADO = [] #<--- Lista donde se almacenan los datos de la tabla AV_Reservas
TEMP_MODIF = []  #<--- Lista donde se almacenan el historial de modificaciones hechas a las reservas.
RESULT_MODIF = [] #<--- Lista donde se filtraran las modificaciones de interes.
MONTH_DATA_UPD = [] #<--- Lista donde se almacena los datos de la ultima modificación del mes.
#----
#------------- Inicio de Funciones. -------------#


def change_State(state):
    #---
    td = (datetime.datetime.now())  # <--- Fecha del dia de hoy.
    #---
    try:
        #---
        with connection.cursor() as cursor:
            #---
            sql = "UPDATE SCR_ESTADO SET ESTADO = %s, FECHA = %s WHERE ID_PORTAL = %s"
            cursor.execute(sql, (state, today, 4))
        connection.commit()
        print("------ Se ha realizado el cambio de estado de la consulta.")
    #---
    except _mssql.MssqlDatabaseException as e:
        #---
        print('Error -> Número de error: ', e.number,
              ' - ', 'Severidad: ', e.severity)
    #---
    if (state == True):
        return "El Script se ejecuto correctamente. Se Ha modificado el estado de ejecución del script."
    else:
        return "El Script no se termino de ejecutar. Se Ha modificado el estado de ejecución del script."
#---
print(change_State(False))  # --- Se cambia el estado de la pagina a Falso.
#--- Suma Meses a una Fecha
def add_months(sourcedate,months):
    #---
    month = sourcedate.month - 1 + months
    year = int(sourcedate.year + month / 12 )
    month = month % 12 + 1
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    #---
    return datetime.date(year,month,day) #<--- Devuelve una fecha.
#---
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
    return delta  #<--- Devuelve la la diferencia entre dos meses.
#---
#--- Se obtiene la fecha del primer registro, de un portal en especifico.
def get_init_date(PORTAL):
    f_date = PORTAL[0][2].date()
    ini_date = []
    ini_date.append(f_date.month)
    ini_date.append(f_date.year)
    #---
    return ini_date #<--- Devuelve anio y mes del primer registro de un portal.
#---
#----  Se saca el mes y el año de un registro.
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
    return datos #<--- Devuelve una lista con el anio y mes.
#---
#--- se saca el precio por noche.
def get_prom_price(p_data):
    temp_resul = []
    for t_precio in p_data:
        days = (t_precio[3].date() - t_precio[2].date())
        days = days.days
        if (days == 0):
            days = 1
        temp_resul.append((t_precio[6])/days)
    #---
    return temp_resul #<--- Devuelve una lista con el precio por noche de la reserva.
#---
#--- Se calcula el numero de días entres dos fechas.
def get_date_diff(d_data, end, init):
    temp_resul = []
    for t_date in d_data:
        days = (t_date[end].date() - t_date[init].date())
        days = days.days
        if (days == 0):
            days = 1
        temp_resul.append(days)
    #---
    return temp_resul #<--- Devuelve una lista con los días de diferencia entre las fechas especificadas.
#---
#--- Se genera una lista con los datos de un campo especifico.
def get_one_field_data(data, index):
    temp_resul = []
    for t_data in data:
        if (t_data[index] == None):
            temp_resul.append(0)
        else:
            temp_resul.append(t_data[index])
    #---
    return temp_resul #<--- Devuelve una lista con todos los datos de un campo en especifico.
#---
#--- Calcular Moda.
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
    return moda #<--- Devuelve una lista con los datos mas repetidos.
#----
#------------- Fin de  Funciones. -------------#
#---
#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
try:
    #---
    with connection.cursor() as cursor:
        #--- Extraccion de los datos de los portales a analizar de SCR_PORTALES
        sql = "SELECT ID_PORTAL, NOMBRE, URL, DIAS_VERIFICACION FROM SCR_PORTALES WHERE ESTADIST_CALC = 1"
        cursor.execute(sql)
        SETTING = cursor.fetchall() #<--- Lista con los portales activos.
        #---
        #--- Extracción de los datos de las reservas a analizar.
        sql_1 = ("SELECT r.ID_RESERVA, r.FECHA_RESERVA, r.FECHA_ENTRADA, r.FECHA_SALIDA, r.NO_PERSONAS, r.NO_NIÑOS, r.PRECIO, r.PROCEDENCIA, s.nombre, r.ESTADO, r.PRECIOextra, r.COMISION, r.GASTOENERGIABAS, r.GASTOLIMREAL FROM AV_RESERVAS r INNER JOIN bmsubcon s ON r.AGENCIA = s.numero WHERE r.ESTADO = 'FINALIZADA' OR r.ESTADO = 'EN CURSO' OR r.ESTADO = 'ACTIVA' ORDER BY r.FECHA_ENTRADA ASC")                
        cursor.execute(sql_1)
        RESULTADO = cursor.fetchall() #<--- Lista con los datos de las reservas.
        #---
        #--- Extracción de los datos de las modificaciones hechas a reservas.
        sql_2 = "SELECT r.ID_RESERVA, r.FECHA_RESERVA, r.FECHA_ENTRADA, r.FECHA_SALIDA,r.ESTADO, d.fecha as fecha_modificación,  d.nota, d.usuario, d.pc FROM foxclea_tareas.AV_RESERVAS r JOIN foxclea_tareas.bmdiarioa d ON r.ID_RESERVA = d.codigo WHERE cast(d.nota as nvarchar) is not null and cast(d.nota as nvarchar) <>'' AND d.fichero = 'RS' AND d.categoria = 'MD' AND r.ESTADO <> 'CANCELADA' AND r.ESTADO <> '' AND r.ESTADO <> 'NOSHOW' AND r.ESTADO <> '' AND r.ESTADO <> 'PENDIENTE' ORDER BY r.FECHA_ENTRADA ASC"
        cursor.execute(sql_2)
        TEMP_MODIF = cursor.fetchall() #<--- Lista con los datos respecto a las modificaciones efectuadas en las reservas.
        #---
        #print(PORTAL)
        print('Correcto -> Extracción de los datos del "portal" a usar.')
#---
except _mssql.MssqlDatabaseException as e:
    print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
#---

#--- Ciclo para extraer los nombres de los portales a analizar.
for s_nombre in SETTING:
    OPTIONS.append(s_nombre[1]) #<--- OPTION toma los nombres de los portales.
#---

#--- Función para filtrar los resultados por sitio.
def get_especific_data_site(name_s):
    #---
    DATA = [] #<--- Lista temporal que almacenara los datos de interes.
    #---
    for r_data in RESULTADO:
        #--- Filtra solo los datos del portal espeficicado.
        if ((str.lower(name_s) in str.lower(r_data[7])) or (str.lower(name_s) in str.lower(r_data[8]))):
            if (r_data[5] != None): #<--- Si no hay un precio definido, no se toma en concideracion la consulta
                DATA.append(r_data)
        #---
    return DATA #<--- Devuelve una lista con los datos del portal espeficicado.
#---

#--- Se filtra los datos modificados de interes, Ex: Fecha de entrada, salida, precio, no_personas, comisión, etc, datos que afecte el precio como tal.
for m_data in TEMP_MODIF:
    if (('entrada' in str.lower(m_data[6])) or ('salida' in str.lower(m_data[6])) or ('precio' in str.lower(m_data[6])) or ('no_personas' in str.lower(m_data[6])) or ('comision' in str.lower(m_data[6])) or ('limpieza' in str.lower(m_data[6])) or ('energia' in str.lower(m_data[6])) ):
        RESULT_MODIF.append(m_data) #<--- Los datos que son de interes se almacenan en la lista de RESULT_MODIF.
#---

#---
print("Datos Modificados")
print("Tamaño = ",len(RESULT_MODIF))
#---

#------------- Inicia Consulta a BD para Obtener Datos Almacenados. -------------#
#--- Se obtiene el ultimo registro del portal especificado en la tabla SCR_PORTALES_DETALLES
def get_last_regis(id_port):
    #---
    try:
        #---
        last_result = [] #<--- Lista donde se almacena el ultimo resulta de SCR_PORTALES_DETALLES, del portal especificado.
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
            last_result = cursor.fetchone() #<--- Se obtiene el utlimo registro del portal especigficado.
            #---
            #print(PORTAL)
            print('Correcto -> Extracción de los datos del "portal" a usar.')
    #---
    except _mssql.MssqlDatabaseException as e:
        print('Error -> Número de error: ',e.number,' - ','Severidad: ', e.severity)
    #---
    # print(last_result)
    return last_result #<--- Devuelve los ultimos datos registrados en PORTALES_DETALLES
    #---
#---

#------------- Funcion para estimar los calculos mensuales por portal. -------------#
#---
def get_portal_data(ID,PORTAL,nombre,days_verif): #<--- ID del portal, Datos del PORTAL, Nombre del PORTAL, días de vrificación del PORTAL
    #---
    i = 0 #<--- variable para iterear.
    #---    
    dates = []  #<--- Mes y Año de Inicio
    #---
    actual_month = (int(today.month) - 1) #<--- se obtiene el mes actual basado en el dia de hoy.
    #---

    #---
    for i in range(len(PORTAL)): #<--- ciclo por mes.
        #---
        l_regis = get_last_regis(ID) #<--- Id del ultimo registro en la tabla SCR_PORTALES_DETALLES.
        #--- Variables de los datos estadisticos a calcular.
        #--- Precio
        NUM_RESERVA = None #<--- Cantidad de reservas por mes.
        PRECIO_MEDIA = None #<--- Precio medio por noche.
        PRECIO_MEDIANA = None #<--- Mediana del precio por noche.
        PRECIO_DESV = None #<--- Desviacion estandar del precio.
        #--- Tiempo de reserva.
        RESERVA_MEDIA = None  #<--- Media del tiempo de reserva.
        RESERVA_MODA = []  #<--- Tiempos de reservas mas comunes.
        RESERVA_MEDIANA = None  #<--- Mediana de lo tiempos de reserva.
        RESERVA_DESV = None  #<--- Desviación estandar del tiempo de reserva.
        #--- Anticipo de reserva.
        ANT_RESERVA_MEDIA = None #<--- Media del tiempo de anticipacion de una reserva.
        ANT_RESERVA_MODA = [] #<--- Moda de los tiempos de anticipación de una reserva.
        ANT_RESERVA_MEDIANA = None #<--- Mediana de los tiempos de anticiapación de una reserva.
        ANT_RESERVA_DES = None #<--- Desviación estandar de los tiempos de anticipación de una reerva.
        #--- Adultos.
        ADULTOS_MEDIA = None #<--- Media de adultos por mes.
        ADULTOS_MODA = [] #<--- Moda de la cantidad mas comun de adultos por reserva.
        #--- Ninos.
        NIÑOS_MEDIA = None #<--- Media de los ninos por mes.
        NIÑOS_MODA = [] #<--- Moda de la ninos por mes.
        #--- Precio Extra.
        PRECIOEXTRA_MEDIA = None #<--- Media del precio extra por reserva en el mes.
        PRECIOEXTRA_MODA = [] #<--- Moda del precio extra por reserva en el mes.
        PRECIOEXTRA_MEDIANA = None #<--- Mediana del precio extra por reserva en el mes.
        PRECIOEXTRA_DESV = None #<--- Desviación estandar del precio extra por reserva en el mes.
        #--- Comisión.
        COMISION_MEDIA = None #<--- Media de la comisión por reserva en el mes.
        COMISION_MODA = [] #<--- Moda de la comisión por reserva en el mes.
        COMISION_MEDIANA = None #<--- Mediana de la comisión por reserva en el mes.
        COMISION_DESV = None #<--- Desviación estandar de la comision por reserva al mes.
        #--- Gasto de energía.
        GASTOENERGIABAS_MEDIA = None #<--- Media del gasto de energia por reserva.
        GASTOENERGIABAS_MODA = [] #<--- Moda del gasto de energia por reserva.
        GASTOENERGIABAS_MEDIANA = None #<--- Mediana del gasto en energía por reserva.
        GASTOENERGIABAS_DESV = None #<--- Desviación estandar del gasto de energia por reserva
        #--- Gasto de limpieza.
        GASTOLIMREAL_MEDIA = None #<--- Media de gastos de limpieza por reserva en el mes.
        GASTOLIMREAL_MODA = [] #<--- Moda del gasto de limpieza por reserva.
        GASTOLIMREAL_MEDIANA = None #<--- Mediana del gasto de limpieza por reserva.
        GASTOLIMREAL_DESV = None #<--- Desviación estandar del gasto de limpieza por reserva.
        #---
        allow = False #<-- Variable para poder realizar verificaciones dentro del rango de días asignados.
        end_ite = False #<--- Variable para finalizar las iteraciones en caso que no sea necesario calcular nada.
        UPDATE_VALUE = False #<--- Variable para permitir actualizaciones de los registro, en caso contrario solo se hara un insert.
        #---

        #--- Se verifican los datos registrados en la tabla SCR_PORTALES_DETALLE
        if (l_regis == None): #<--- En caso que no haya ningun registro.
            #---
            dates = get_init_date(PORTAL) #<--- se asigna la fecha inicial basado en la fecha del primer registro del portal.
            end_ite = False #<--- Se le dal Fal a end_int para que no termine la iteración.
            #---
            print('No hay registros anteriores')
        #---
        elif (l_regis[2] == actual_month and l_regis[3] == today.year): #<--- En caso que si haya registros y estos sean igual al mes pasado y al anio actual.
        #---
            td_month = actual_month + 1 #<--- Se le asigna el mes actual.           
            date_init_verif =  datetime.datetime.strptime(('01' + str(td_month) + str(today.year)), "%d%m%Y").date() #<--- Se fija el primer dia del mes actual.
            date_end_verif =  date_init_verif + datetime.timedelta(days=days_verif) #<--- Se calcula la fecha limite basado en el rango de días maximo de la tabla SCR_PORTALES.
            #---
            print('Fecha Fin: ',date_end_verif) 
            print('Fecha Inicio: ',today)
            #---
            if (today < date_end_verif): #<--- Se compara si el día de hoy es menos al la fecha limite, si es asi.
                allow = True #<--- Para permitir recalcular los datos del ultimo mes.
                end_ite = False #<--- Para continuar con la iteración.  
                #---                             
                dates.append(l_regis[2]) #<--- se asigna el mes del ultimo registro.
                dates.append(l_regis[3]) #<--- se asigna el anio del ultimo registro.
            #---
            else: #<--- En caso que la fecha actual se mayor a la fecha maxima de verificación.
                allow = False #<--- No se necesita recalcular los datos del ulimo mes.
                end_ite = True #<--- Para permitir finalizar la iteración.
        #---
        elif (l_regis[3] < today.year): #<--- En caso de que el ultimo registro sea de anios anteriores al actual.
        #---
            end_ite = False #<--- Para continuar la iteración.
            #---
            if (l_regis[2] == 12): #<--- En caso que el mes sea igual a 12.
                dates.append(1) #<--- El mes se le asigna 1 (enero).
                dates.append(l_regis[3] + 1) #<--- Se le suma una al anio.
            #---
            else: #<--- En caso contrario solo:                
                dates.append( l_regis[2] + 1) #<--- se le suma uno al ultimo mes registrado.
                dates.append(l_regis[3]) #<--- Se le asigna el valor del anio del ultimo registro.
        #---
        elif (l_regis[2] < actual_month and l_regis[3] == today.year): #<--- En caso que el mes sea menor al mes actual y el anio igual al actual.
        #---
            end_ite = False #<--- Para continuar la iteración.
            #---
            if (l_regis[2] == 12): #<--- En caso que el mes sea igual a 12.
                dates.append(1) #<--- El mes se le asigna 1 (enero).
                dates.append(l_regis[3] + 1) #<--- Se le suma una al anio.
            #---
            else: #<--- En caso contrario solo:                    
                dates.append(l_regis[2] + 1) #<--- se le suma uno al ultimo mes registrado.
                dates.append(l_regis[3]) #<--- Se le asigna el valor del anio del ultimo registro.
            #---
                        
        #-------
        if (end_ite == True): #<--- Si la variable es igual a True, se finaliza la iteración.
            break
        #---

        #---     
        data = get_data_month(PORTAL, dates[0], dates[1]) #<--- Se saca la información de un mes y anio especifico.
        MONTH_DATA_UPD = [] #<--- se limpia la lista donde se almacenaran los datos de la ultima modificacion de la ultima reserva del mes.
        #---

        #---
        for data_check in data: #<--- Ciclo para obtener los datos de la ultima modificación de la ultima reserva.
        #---
            for data_modf in RESULT_MODIF:
                #---
                if (data_check[0] == data_modf[0]):
                    MONTH_DATA_UPD = data_modf #<--- se deja los datos de la ultima modiciación.
        #---

        #---
        if (len(MONTH_DATA_UPD) == 0): #<--- Si no hay ninguna modificacion (datos en la lista MONTH_DATA_UPD)
        #---
            i = 0 
            #---
            while i <= 9: #<--- se le agregan 9 Nones a la lista para evitar un error en la inserción.
                MONTH_DATA_UPD.append(None)
                i += 1
            #---
        #---
        elif (allow == True): #<--- En caso que si hayan datos y que allo sea verdadero:
            #---
            print('FECHA DE REGISTRO = ',l_regis[44].date(),'--- FECHA DE NUEVO REGISTRO = ',MONTH_DATA_UPD[5].date())
            #---
            if (l_regis[44].date() < MONTH_DATA_UPD[5].date()): #<--- Se compara a fecha de la ultima modificación en el registro de SCR_PORTALES_DETALLES con la fecha de la ultima modificacion en la lista MONTHDATA_UPD, en caso de se mayor esta ultima:
                UPDATE_VALUE = True #<--- Permite la recalculación y actualizacion del ultimo registro.
                allow = True
            #---
            else: #<--- en caso que sean iguales o inferior.
                allow = False
                UPDATE_VALUE = False #<--- No se permite actualizar.
                end_ite = True #<--- Termina la iteración.
            #---

        #---   
        if (allow == False and end_ite == True): #<--- en caso de que end_ite sea true, se finaliza la iteración.
            break
        #---
        print('-------------MODIFICACIÓN----------------')
        print(MONTH_DATA_UPD)
        #---
        
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
    print(change_State(True))
#---
print(change_State(True))
