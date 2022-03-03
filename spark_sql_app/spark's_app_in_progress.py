import os
from pyspark.sql import SparkSession
import re
import time



class Insertion():
    def __init__(self,zon,delta):
        self.tab = 'test.pysparkprueba3'#input("Put table's name: ")
        self.prepar = "20220105" #input("Put process date (ej>20220101,20220102,20210103): ")
        self.part = self.prepar.split(",")
        self.prepartby ='fecha_proceso'#input("Put partition columns (ej>fecha_proceso,id_persona): ")
        self.partby = self.prepartby.split(",")
        self.zone = zon
        self.deltas = delta
        self.file_status ='si' #input("Put whethere the file is compressed or not (ej>no,si): ")
        self.sesion_de_spark = SparkSession.builder\
                  .enableHiveSupport()\
                  .appName("PowerfulMotorSparkEngine")\
                  .config('spark.executors.cores', '4')\
                  .config('spark.hadoop.hive.exec.dynamic.partition', 'true')\
                  .config('spark.hadoop.hive.exec.dynamic.partition.mode', 'nonstrict')\
                  .getOrCreate()



    def raw_without_deltas(self):

        self.zonel = 'goa'#input("Put the location of the file/s. HDFS or GOA(ej>hdfs,ej>goa): ") 
        self.path = '/home/jferreyra/pruebacom/CACCTABF_20220105.txt.gz' #input("Put file/s path, including its name and replacing date with YYYYMMDD(ej: /home/jferreyra/archivo_YYYYMMDD.txt): ")
        self.separator = '|'#|#input("Put file's delimiter(ej>|, ej>,): ")
        self.header ='true'#input("Put whether the file has header or not(ej>true, ej>false): ")

        def set_up_file(compressed_file_prepath,fechas):
            def find_encoding(pat_sea):
                command = os.popen("file -i {}".format(pat_sea),'r')
                command_value = command.read()
                value = re.findall('(?<=charset=)(.*)(?=\\n)',command_value)
                return value[0]

            for x in fechas:
                compressed_file_path = re.sub('YYYYMMDD',x,compressed_file_prepath)
                print("\033[;36m"+"Descomprimiendo archivo..."+"\033[;37m")
                time.sleep(3)
                os.popen("gzip -d {}".format(compressed_file_path),'r')
                os.wait()
                uncompressed_file_path = re.sub('.gz','',compressed_file_path)
                encoding = find_encoding(uncompressed_file_path)
                date = re.search('[0-9]{6,8}',uncompressed_file_path)[0]
                uncompressed_file_path_random = re.sub(date,date+'c',uncompressed_file_path)
                os.popen("iconv -f {0} -t utf8 {1} > {2}".format(encoding,uncompressed_file_path,uncompressed_file_path_random))
                os.popen("rm {}".format(uncompressed_file_path))
                os.popen("mv {0} {1}".format(uncompressed_file_path_random,uncompressed_file_path))



        if self.file_status == 'si':
            set_up_file(self.path,self.part)

        for x in range(len(self.part)):
            if self.file_status == 'si':
                self.new_path = re.sub('.gz','',self.path)
            else:
                self.new_path = self.path
            self.npath =  re.sub('YYYYMMDD',self.part[x],self.new_path)
            if self.zonel == 'goa':
                 self.data_frame = self.sesion_de_spark.read.csv("""file://%s"""% (self.npath),header="""%s"""% (self.header),sep="""%s"""% (self.separator))
            else:
                 self.data_frame = self.sesion_de_spark.read.csv("""hdfs://%s""" % (self.npath),header="""%s"""% (self.header),sep="""%s"""% (self.separator))

            self.quantity_of_records_1 = self.data_frame.count()

            self.data_frame.createOrReplaceTempView('data_a_insertar')

            print("\033[;36m"+"Inicio de la insercion..."+"\033[;37m")
            time.sleep(2)
            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s') SELECT * FROM data_a_insertar""" % (self.tab,self.partby[0],self.part[x]))  
            print("\n"+"\n"+"\033[;36m"+"{0} records have been inserted in the table '{1}' with fecha_proceso {2}".format(self.quantity_of_records_1,self.tab,self.part[x]))    
            print("\033[;37m")

        
        



        
    def raw_with_deltas(self):

       self.zonel = 'goa'#input("Put the location of the file/s. HDFS or GOA(ej>hdfs,ej>goa): ")
       self.path = '/home/jferreyra/pruebacom/j_YYYYMMDD.txt.gz'  #input("Put file/s path, including its name and replacing date with YYYYMMDD(ej: /home/jferreyra/archivo_YYYYMMDD.txt): ")
       self.separator = '|'#|#input("Put file's delimiter(ej>|, ej>,): ")
       self.header ='True'#input("Put whether the file has header or not(ej>true, ej>false): ")

       def set_up_file(compressed_file_prepath,fechas):
            def find_encoding(pat_sea):
                command = os.popen("file -i {}".format(pat_sea),'r')
                command_value = command.read()
                value = re.findall('(?<=charset=)(.*)(?=\\n)',command_value)
                return value[0]

            for x in fechas:
                compressed_file_path = re.sub('YYYYMMDD',x,compressed_file_prepath)
                print("\033[;36m"+"Descomprimiendo archivo..."+"\033[;37m")
                time.sleep(3)
                os.popen("gzip -d {}".format(compressed_file_path),'r')
                os.wait()
                uncompressed_file_path = re.sub('.gz','',compressed_file_path)
                encoding = find_encoding(uncompressed_file_path)
                date = re.search('[0-9]{6,8}',uncompressed_file_path)[0]
                uncompressed_file_path_random = re.sub(date,date+'c',uncompressed_file_path)
                os.popen("iconv -f {0} -t utf8 {1} > {2}".format(encoding,uncompressed_file_path,uncompressed_file_path_random))
                os.popen("rm {}".format(uncompressed_file_path))
                os.popen("mv {0} {1}".format(uncompressed_file_path_random,uncompressed_file_path))

       if self.file_status == 'si':
            set_up_file(self.path,self.part)

       self.deltas = [col.strip() for col in self.deltas.split(',')] 
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
            if self.file_status == 'si':
                self.new_path = re.sub('.gz','',self.path)
            else:
                self.new_path = self.path
            
            self.npath =  re.sub('YYYYMMDD',self.part[x],self.new_path)

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

            print("\033[;36m"+"Inicio de la insercion..."+"\033[;37m")
            time.sleep(2)

            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s')
                SELECT * FROM df_insert_deltas""" % (self.tab,self.partby[0],self.part[x]))
            print("\n"+"\n"+"\033[;36m"+"{0} records have been inserted in the table '{1}' with fecha_proceso {2}".format(self.quantity_of_records,self.tab,self.part[x]))    
            print("\033[;37m")



    def cur_without_deltas(self):

       for x in range(len(self.part)):
            self.query_input = input("Put query to be inserted in cur (take $FECHA_PROCESO parameter into account: ")
            self.updated_query_one = re.sub(r'\$FECHA_PROCESO',self.part[x],self.query_input)
            self.query = self.sesion_de_spark.sql(self.updated_query_one)
            self.view = self.query.createOrReplaceTempView('new')

            print("\033[;36m"+"Inicio de la insercion..."+"\033[;37m")
            time.sleep(2)

            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s')
                       SELECT * FROM new""" % (self.tab,self.partby[0],self.part[x]))


            self.quantity_of_records_1 = self.query.count()
            print("\n"+"\n"+"\033[;36m"+"{0} records have been inserted in the table '{1}' with fecha_proceso {2}".format(self.quantity_of_records_1,self.tab,self.part[x]))    
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
            self.query_input = input("Put query to be inserted in cur (take $FECHA_PROCESO parameter into account: ")
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


            print("\033[;36m"+"Inicio de la insercion..."+"\033[;37m")
            time.sleep(2)

            self.sesion_de_spark.sql("""INSERT INTO %s PARTITION (%s = '%s')
                SELECT * FROM df_insert_deltas""" % (self.tab,self.partby[0],self.part[x]))


            print("\n"+"\n"+"\033[;36m"+"{0} records have been inserted in the table '{1}' with fecha_proceso {2}".format(self.quantity_of_records,self.tab,self.part[x]))    
            print("\033[;37m")



class Adjustment():
        
        def __init__(self):
                self.sesion_de_spark = SparkSession.builder\
                  .enableHiveSupport()\
                  .appName("PowerfulMotorSparkEngine")\
                  .config('spark.executors.cores', '4')\
                  .config('spark.hadoop.hive.exec.dynamic.partition', 'true')\
                  .config('spark.hadoop.hive.exec.dynamic.partition.mode', 'nonstrict')\
                  .getOrCreate()
                self.tab = 'de_bsf_1raw.sbi_cacctabf'#input("Put table's name: ")
                self.dir_search = '/home/jferreyra/dir_bus'#input("Put dir's name in which there are files with incorrect header: ")
                self.target_dir ='/home/jferreyra/target_dir'#input("Put target dir's name in which all the fixed files are going to be: ")
                self.separator = '|' #input("Put file's separator: ")   

        def adjust_header(self):
                self.columns_raw_uppercase = [col.upper() for col in self.sesion_de_spark.table("""%s""" %(self.tab)).schema.names[:-1]]
                self.columns_final = """%s"""%(self.separator).join(self.columns_raw_uppercase)
                self.files_list = os.listdir("""%s"""%(self.dir_search))
                self.target_dir_created = os.mkdir(self.target_dir)
                
                
                for x in self.files_list:
                        self.path_search = os.path.join(self.dir_search,x)
                        self.target_path = os.path.join(self.target_dir,x)
                        self.encoding = self.find_encoding(self.path_search)

                        with open("""%s"""%(self.path_search),"r",encoding = """"%s""" %(self.encoding)) as f:
                                with open("""%s"""%(self.target_path),"w",encoding = """"%s""" %(self.encoding)) as a:
                                        a.write(self.columns_final+'\n')
                                        self.ff = f.readline()
                                        while self.ff:
                                                try:                        
                                                        self.list_of_conditions = []
                                                        for x in self.columns_raw_uppercase:
                                                                self.ff_re = re.findall("{}".format(x),self.ff)
                                                                if len(self.ff_re)!=0:
                                                                        self.list_of_conditions.append(True)
                                                                if len(self.ff_re) == 0:
                                                                        self.list_of_conditions.append(False)
                                                        self.condition = any(self.list_of_conditions)
                                                        if self.condition == False:
                                                                a.write(self.ff)
                                                        self.ff = f.readline()
                                                except:
                                                        break

                print("\n"+"\033[;36m"+"Operation done properly "+"\033[;37m")

        def find_encoding(self,pat_sea):
                self.pat_sea = pat_sea
                self.command = os.popen("file -i {}".format(self.pat_sea),'r')
                self.command_value = self.command.read()
                self.value = re.findall('(?<=charset=)(.*)(?=\\n)',self.command_value)
                return self.value[0]


class Querying():
    def __init__(self):
            self.sesion_de_spark = SparkSession.builder\
            .enableHiveSupport()\
            .appName("PowerfulMotorSparkEngine")\
            .config('spark.executors.cores', '4')\
            .config('spark.hadoop.hive.exec.dynamic.partition', 'true')\
            .config('spark.hadoop.hive.exec.dynamic.partition.mode', 'nonstrict')\
            .getOrCreate()
            
       
    def fast_visualization(self):
        self.query = input("Put query to be saved:")
        self.df = self.sesion_de_spark.sql(self.query)
        self.df.show()
    def saving(self):
        self.query = input("Put query to be saved:")
        self.df = self.sesion_de_spark.sql(self.query)
        self.df.write.csv(self.path)




def main():
    ##################################################################################################################################################################
         #print"\033[;36m"   
          #  print("\033[;37m")

    task_parameter = input("\033[;36m"+'''Put the operation to perform:
    1.Insertion:
    2.Adjustment:
    3.Querying:
    '''+"\033[;37m")

    ###################esto iria dentro de lo que es main>insertion
    if task_parameter == '1' or task_parameter == 1:
        zone = 'raw' #input("Put where the data is going to be inserted (ej>raw,ej>cur,ej>ref): ")
        delt = 'no'#input("""Put whether there are deltas or not (ej>*(todas),ej>no,ej>nombre,dni)
        #,and consider that if the partitions is the first one, it should not contain deltas: """)
        insertion1 = Insertion(zone,delt)
        if zone == 'raw' and delt == 'no':
            insertion1.raw_without_deltas()
        if zone == 'raw' and delt != 'no':
            insertion1.raw_with_deltas()
        if zone == 'cur' and delt == 'no':
            insertion1.cur_without_deltas()
        if zone == 'cur' and delt != 'no':
            insertion1.cur_with_deltas()


    ########################## iria dentro de lo que es el main >adjustment
    if task_parameter == '2' or task_parameter == 2:
        adjustment1 = Adjustment()
        adjustment1.adjust_header()



    ########################## iria dentro de lo que es el main >querying
    if task_parameter == '3' or task_parameter == 3:
        querying1 = Querying()
        task = input("""Put what to do:
        a.Querying and getting fast result:
        b.Querying and saving it to a file in a specified path: """)
        if task == 'a':
            querying1.fast_visualization()
        if task == 'b':
            querying1.saving()
    ##################################################################################################################################################################



main()


'''ver tema de emprlolijar llamadas por fuera de las clases y eso'''
'''ver tema de insercion en hbase'''
'''agregar carpeta con inserciones como historial 
'''
'''PRIORIDAD ALTA'''
'''ver tema de particion repetida y eso al momento de insertar'''
'''mediciones: 12 segundos por cada 33 mb por cada particion
entonces: 12 minutos 60 particiones de 33mb, equivalente a 2gb 
'''
'''cambiar los inputs para cuando sea por terminal linux y que quede con los sysargv , sysargvs[1]..'''
'''dividir script en modulos '''
'''contemplar subidas a hdfs'''
''''ver caso de extension gzip'''