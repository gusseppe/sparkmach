
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from datetime import datetime
import time


# In[2]:

def countTab(x):
        c=0
        for i in x:
                for j in i:
                        if (j=='\t'):
                                c=c+1
        return c


def cleanEl(x):
        r=''
        for i in x:
                for j in i:
                        if (j!='"'):
                                r=r+j
        return r




def forTim(x):
        fecha=datetime.strptime(x[2], '%Y-%m-%d %H:%M:%S')
        x[2]=time.mktime(fecha.timetuple())
        return x

#probar
def hours(x):
        fecha=datetime.strptime(x[2], '%Y-%m-%d %H:%M:%S')
        x[2]=fecha.hour
        x.append(fecha.weekday())
        return x


def toHourBucket(x):
        horaEnSeg=60*60
        timeStamp=x[4]
        x[4]= timeStamp//horaEnSeg
        return x
    
    
def toHalfHourBucket(x):
        hora= (x[4] // (60*60))%24
        minuto= (x[4] //60)%60
        if (minuto < 30):
            minuto=0.0
        else:
            minuto=0.5 
        valor= hora+minuto
        x[4]=valor
        return x
    
# La funcion ordenara los Timestamps y debera agruparlos por periodos similares,
# los periodos son conformados por TS en orden ascendente cuya diferencia entre valores contiguos
# no sea mayor a 120 segundos

# los periodos que solo tienen 1 TS figuran con 0 (verificar si se mantiene de esta manera) 

def groupByPeriod(x):
    x=sorted(x)
    tiemposRecorrido=[]
    
    tsMinimo=x[0]
    tsMaximo=x[0]
    cMediana=1
    periodoTs=[x[0]]
    for i in range(0,len(x)-1):
                 
            
        if (x[i+1]-x[i] <= 120):
            
            tsMaximo=x[i+1]
            periodoTs.append(x[i+1])
            cMediana+=1
            
            if (i == len(x)-2):
                
                if (cMediana%2==0):
                    medianaTs=(periodoTs[(cMediana//2)-1] + periodoTs[cMediana//2])//2
                else:
                    medianaTs= periodoTs[cMediana//2]
                    
                tiemposRecorrido.append((medianaTs, tsMaximo-tsMinimo))
            
                
        else:
                
                
            if (cMediana%2==0):
                medianaTs=(periodoTs[(cMediana//2)-1] + periodoTs[cMediana//2])//2
            else:
                medianaTs= periodoTs[cMediana//2]
                
            tiemposRecorrido.append((medianaTs, tsMaximo-tsMinimo))
            tsMinimo=x[i+1]
            tsMaximo=x[i+1]
            cMediana=1
            periodoTs=[x[i+1]]
            
            if (i == len(x)-2):
                tiemposRecorrido.append((tsMinimo, 0))
    
    #return x    
    return tiemposRecorrido



def mainFilter(rdd):
	#limpia ingresos repetidos
    cleaned=rdd.distinct()


	#descarta lineas corruptas que son aquellas que no cumplen las 26 comillas
    discardCorrupted= cleaned.filter(lambda x: countTab(x)==10)

	#va a mapear cada ingreso y lo  partira en elementos a traves de la tabulacin
    filteredT= discardCorrupted.map(lambda x:x.split('\t'))

	#descarta las lineas que son headers
    discardHeaders=filteredT.filter(lambda x : x[0]!="latitude" )

	#formatea los tiempos a segundos en UNX
    formatTime= discardHeaders.map(lambda x: forTim(x))
	
	#formatea los tiempos a horas 
    #formatTime= discardHeaders.map(lambda x: hours(x))
    
    #devolver solo de un bus
    #getResultsFromOneBus= formatTime.filter(lambda x: x[3]=='4215')
    
    #getResultsFromOneRoute= getResultsFromOneBus.filter(lambda x: x[7]=='MTA NYCT_Q20B' )
    
    #getResultsFromOneStop= getResultsFromOneRoute.filter(lambda x: x[10] =='MTA_505010') 
    
    #RDD con llaves
    keyedTuple= formatTime.map(lambda x: ((x[3],(x[7],x[10],x[5])),x[2]))
    
    groupedKeys=keyedTuple.groupByKey().mapValues(lambda x: groupByPeriod(x)).cache()
    
    
    ##Armar tupla final utilizando indices de arrays en python
    flattenedGroups= groupedKeys.flatMapValues(lambda x: x).map(lambda x: (x[0][0],x[0][1][0],x[0][1][1],x[0][1][2],x[1][0],x[1][1]))
            
    #lambda x: (max(x),min(x),max(x)-min(x))
    #RDD
	#Se genera una tupla para clasificacion con los valores hora, diaSemana, busID,orientation, nextStop, Route
	#classTuple= formatTime.map(lambda x: (x[2],x[11],x[3],x[5],x[10],x[7]))
    
    #CSV
    #Se genera una tupla para clasificacion con los valores hora, diaSemana, busID,orientation, nextStop, Route
    #classTuple= formatTime.map(lambda x: str(x[2])+","+str(x[11])+","+x[3]+","+x[5]+","+x[10]+","+x[7])

    return flattenedGroups






