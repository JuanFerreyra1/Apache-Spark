import re

from pyspark.sql import SparkSession

class Insertion():
    def __init__(self,tab,par,partby,zone,delt):
        self.tab = tab
        self.part = par.split(",")
        self.partby = partby.split(",")
        self.zone = zone
        self.deltas = delt
        self.sesion_de_spark = SparkSession.builder\
                  .enableHiveSupport()\
                  .appName("PowerfulMotorSparkEngine")\
                  .config('spark.executors.cores', '4')\
                  .config('spark.hadoop.hive.exec.dynamic.partition', 'true')\
                  .config('spark.hadoop.hive.exec.dynamic.partition.mode', 'nonstrict')\
                  .getOrCreate()
       

    def raw_without_deltas(self):
       self.zonel = 'goa'#input("Coloque si su/s archivo/s esta en HDFS o en GOA (ej>hdfs,ej>goa): ") 
       self.path = '/home/jferreyra/j_YYYYMMDD.txt' #input("Coloque la ruta de su/s archivo/s incluendo su nombre y reeemplanzado la fecha por YYYYMMDD: (ej: /home/jferreyra/archivo_YYYYMMDD.txt): ")
       self.separator = '|'#|#input("Coloque el separador de su archivo (ej>|, ej>,): ")
       self.header ='true'#input("El archivo posee header (ej>true, ej>false): ")

       for x in range(len(self.part)):
            self.npath =  re.sub('YYYYMMDD',self.part[x],self.path)
            if self.zonel == 'goa':
                 self.data_frame = self.sesion_de_spark.read.csv("""file://%s"""% (self.npath),header="""%s"""% (self.header),sep="""%s"""% (self.separator))
            else:
                 self.data_frame = self.sesion_de_spark.read.csv("""hdfs://%s""" % (self.npath),header="""%s"""% (self.header),sep="""%s"""% (self.separator))

            self.quantity_of_records_1 = self.data_frame.count()

            self.data_frame.createOrReplaceTempView('data_a_insertar')

            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s') SELECT * FROM data_a_insertar""" % (self.tab,self.partby[0],self.part[x]))
            print("\n")
            print("\n")   
            print("\033[;36m"+"Se han insertado {0} registros en la tabla '{1}' con fecha_proceso {2}".format(self.quantity_of_records_1,self.tab,self.part[x]))    
            print("\033[;37m")


    def raw_with_deltas(self):
       self.zonel = 'goa'#input("Coloque si su/s archivo/s esta en HDFS o en GOA (ej>hdfs,ej>goa): ") 
       self.path = '/home/jferreyra/query-hive-YYYYMMDD.csv' #input("Coloque la ruta de su/s archivo/s incluendo su nombre y reeemplanzado la fecha por YYYYMMDD: (ej: /home/jferreyra/archivo_YYYYMMDD.txt): ")
       self.separator = ','#|#input("Coloque el separador de su archivo (ej>|, ej>,): ")
       self.header ='false'#input("El archivo posee header (ej>true, ej>false): ")

       self.deltas = [col.strip() for col in deltas.split(',')] 
       self.columns_raw = self.sesion_de_spark.table(self.tab).schema.names[:-len(self.partby)]
       def get_delta_statement(deltas, columns):
            if deltas[0] == '*':
                self.delta_columns = (("nvl(a.%s,'null')" % col, "nvl(b.%s,'null')" % col) for col in self.columns_raw)
            else:
                self.delta_columns = (("nvl(a.%s,'null')" % col,"nvl(b.%s,'null')" % col) for col in self.deltas)
            
            self.on_statement = ' AND '.join((' = '.join(cols) for cols in self.delta_columns))
            return self.on_statement

       self.delta_statement = get_delta_statement(self.deltas, self.columns_raw)

       for x in range(len(self.part)):
            self.npath =  re.sub('YYYYMMDD',self.part[x],self.path)

            if self.zonel == 'goa':
                self.data_frame = self.sesion_de_spark.read.csv("""file://%s"""% (self.npath),header="""%s"""% (self.header),sep="""%s"""% (self.separator))
            else:
                self.data_frame = self.sesion_de_spark.read.csv("""hdfs://%s""" % (self.npath),header="""%s"""% (self.header),sep="""%s"""% (self.separator))


            self.data_frame.createOrReplaceTempView('data_delta')

            self.delta_query = """SELECT * FROM data_delta a
                         LEFT ANTI JOIN %s b 
                         ON %s                         
                        """ % (self.tab, self.delta_statement)

            self.data_frame_deltas = self.sesion_de_spark.sql(self.delta_query) 
            self.data_frame_deltas.createOrReplaceTempView('df_insert_deltas')
            self.quantity_of_records = self.data_frame_deltas.count()

            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s')
                SELECT * FROM df_insert_deltas""" % (self.tab,self.partby[0],self.part[x]))

            print("\n")
            print("\n")   
            print("\033[;36m"+"Se han insertado {0} registros en la tabla '{1}' con fecha_proceso {2}".format(self.quantity_of_records,self.tab,self.part[x]))    
            print("\033[;37m")


    def cur_without_deltas(self):

       for x in range(len(self.part)):
            self.query_input = input("Ingrese la query a insertar en curado (respetando el parametro $FECHA_PROCESO): ")
            self.updated_query_one = re.sub(r'\$FECHA_PROCESO',self.part[x],self.query_input)
            self.query = self.sesion_de_spark.sql(self.updated_query_one)
            self.view = self.query.createOrReplaceTempView('new')
        

            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s')
                       SELECT * FROM new""" % (self.tab,self.partby[0],self.part[x]))

            self.quantity_of_records_1 = self.query.count()
            print("\n")
            print("\n")   
            print("\033[;36m"+"Se han insertado {0} registros en la tabla '{1}' con fecha_proceso {2}".format(self.quantity_of_records_1,self.tab,self.part[x]))    
            print("\033[;37m")



    def cur_with_deltas(self):

       self.deltas = [col.strip() for col in deltas.split(',')] 
       self.columns_raw = self.sesion_de_spark.table(self.tab).schema.names[:-len(self.partby)]
       def get_delta_statement(deltas, columns):
            if deltas[0] == '*':
                self.delta_columns = (("nvl(a.%s,'null')" % col, "nvl(b.%s,'null')" % col) for col in self.columns_raw)
            else:
                self.delta_columns = (("nvl(a.%s,'null')" % col,"nvl(b.%s,'null')" % col) for col in self.deltas)
            
            self.on_statement = ' AND '.join((' = '.join(cols) for cols in self.delta_columns))
            return self.on_statement

       self.delta_statement = get_delta_statement(self.deltas, self.columns_raw)

       for x in range(len(self.part)):
            self.query_input = input("Ingrese la query a insertar en curado (respetando el parametro $FECHA_PROCESO): ")
            self.updated_query_one = re.sub(r'\$FECHA_PROCESO',self.part[x],self.query_input)
            self.query = self.sesion_de_spark.sql(self.updated_query_one)
            self.view = self.query.createOrReplaceTempView('data_delta')

            self.delta_query = """SELECT * FROM data_delta a
                         LEFT ANTI JOIN %s b 
                         ON %s                         
                        """ % (self.tab, self.delta_statement)

            self.data_frame_deltas = self.sesion_de_spark.sql(self.delta_query) 
            self.quantity_of_records = self.data_frame_deltas.count()

            self.data_frame_deltas.createOrReplaceTempView('df_insert_deltas')

            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s')
                SELECT * FROM df_insert_deltas""" % (self.tab,self.partby[0],self.part[x]))


            print("\n")
            print("\n")   
            print("\033[;36m"+"Se han insertado {0} registros en la tabla '{1}' con fecha_proceso {2}".format(self.quantity_of_records,self.tab,self.part[x]))    
            print("\033[;37m")





#aca iria todo lo que es el main
##################################################################################################################################################################
task_parameter = input('''Ingrese la operacion a realizar:
1.Insertion: ''')
if task_parameter == '1' or task_parameter == 1:
    table = 'test.pysparkprueba_cur'#input("Nombre de tabla: ")
    partition  = "20220520" #input("Fecha(s) del proceso (ej>20220101,20220102,20210103): ")
    partitionby = 'fecha_proceso'#input("Columna/s de particion/es: (ej>fecha_proceso,id_persona): ")
    zone_of_insertion = 'cur' #input("Coloque donde se insertarian datos: (ej>raw,ej>cur,ej>ref): ")
    deltas = '*'#input("""Coloque si hay deltas en la insercion (ej>*(todas),ej>no,ej>nombre,dni)
    #,y considere que si la particion a insertar es la primera,no deberia tener deltas: """)   



insertion1 = Insertion(table,partition,partitionby,zone_of_insertion,deltas)

if zone_of_insertion == 'raw' and deltas == 'no':
    insertion1.raw_without_deltas()
if zone_of_insertion == 'raw' and deltas != 'no':
    insertion1.raw_with_deltas()
if zone_of_insertion == 'cur' and deltas == 'no':
    insertion1.cur_without_deltas()
if zone_of_insertion == 'cur' and deltas != 'no':
    insertion1.cur_with_deltas()
##################################################################################################################################################################
















#product backlog

'''ver tema subentorno, entidad en curado queries'''



'''cambiar color de inputs (en todos).
agregar carpeta con inserciones como historial 
'''



'''PRIORIDAD ALTA'''
'''ver tema particiones dinamicas'''
'''contemplar caso de arcivos comprimidos (todos basicamente)'''
'''ver tema encoding'''
'''EL CAMPO QUE LE PASE AL LEFT ANTI JOIN SERA 
AQUEL QUE SE FIJARA SI ESTA O NO EN LA TABLA'''
'''comprobar esto>esta comprobado:
 los deltas no cuentan la feca proceso cuando es asterisco en deltas'''

''''
ej: 
feca 1:


ver el tema de multiple partitions, o sea basada la insercion en mas de una partiticion. Como aria ai 

'''

'''ver tema del encoding'''


'''agregar colores a los outputs
'''
'''ver tema de particion repetida y eso al momento de insertar'''


'''mediciones: 12 segundos por cada 33 mb por cada particion
entonces: 12 minutos 60 particiones de 33mb, equivalente a 2gb 
'''


'''agregar el se an insertado'''


'''cambiar los inputs para cuando sea por terminal linux y que quede con los sysargv , sysargvs[1]..'''



'''path x /home/jferreyra/prueba.txt'''

'''
dfnn = self.sesion_de_spark.sql("select * from de_bsf_2cur.ft_riesgos_antecedentes_negativos")
dfnn.show()
'''       

'''harcodear el nombre del archivo'''


''' avisar si la particion a insertar ya esta'''


'''
def main():
    task_parameter = input("Ingrese la operacion a realizar: ")
    if task_parameter =='Insertion':
        insercion_x = Insertion()

        
data_frame_originado_por_archivo = sesion_de_spark.sql("select * from de_bsf_2cur.rel_cliente_core_documentos")

data_frame_originado_por_archivo .show()

}

data_frame_originado_por_archivo = sesion_de_spark.read.csv('file:///home/jferreyra/ACTAB1_20220103.txt',header='true',sep ="|")

data_frame_originado_por_archivo.show(truncate=False)


'''


''' caso de no traer las columnas particionadas es para cur no para raw'''
