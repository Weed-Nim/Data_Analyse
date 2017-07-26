# calcular de mediana
# calcular de la media aritmetica
# calcular la moda

print("Datos a tratar: ")
data=[1,4,6,7,2,3,4,7,1,2,4,3,4,5,
      6,5,2,3,1,2,5,6,7,3,4,1,5,7,
      1,7,6,5,3,4,3,4,5,5,5,6,7,3,
      4,5,3,4,5,4,5,6,3,4,3,4,5,3,
      4,5,3,4,5,3,4,5,3,4,5,3,1,3,
      4,5,3,4,5,6,5,4,6,5,6,5,1,7]

print(data)
dOrder=sorted(data)

n=len(dOrder)
middle=n/2
print(middle)
# codigo para calcular la mediana
if n%2 == 0:
	mediana=(dOrder[int(middle)+1] + dOrder[int(middle)+2]) / 2
else:
	mediana=dOrder[middle+1]*1

print('')
print('Total datos', n)
print('Mediana: ', mediana)

# codigo para calcular la media aritmetica
print('Mediana Aritmetica: ', round(sum(data)*1.0/n,2))

# codigo para calcular la moda
repetir = 0                                                                         
for i in data:                                                                              
    aparece = data.count(i)                                                             
    if aparece > repetir:                                                       
        repetir = aparece                                                       
                                                                                         
moda = []                                                                               
for i in data:                                                                              
    aparece = data.count(i)                                                             
    if aparece == repetir and i not in moda:                                   
        moda.append(i)                                                                  
                                                                                         
print("moda:", moda)