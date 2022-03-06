import os
from pyspark.sql import SparkSession
import re
import time

class Insertion():
    def __init__(self,zon,delta):
        self.tab = input("Put table's name: ")
        self.prepar = input("Put process date (ej>20220101,20220102,20210103): ")
        self.part = self.prepar.split(",")
        self.prepartby =input("Put partition columns (ej>fecha_proceso,id_persona): ")
        self.partby = self.prepartby.split(",")
        self.zone = zon
        self.deltas = delta
        self.file_status =input("Put whethere the file is compressed or not (ej>no,si): ")
        self.sesion_de_spark = SparkSession.builder\
                  .enableHiveSupport()\
                  .appName("PowerfulMotorSparkEngine")\
                  .config('spark.executors.cores', '4')\
                  .config('spark.hadoop.hive.exec.dynamic.partition', 'true')\
                  .config('spark.hadoop.hive.exec.dynamic.partition.mode', 'nonstrict')\
                  .getOrCreate()



    def raw_without_deltas(self):

        self.zonel = input("Put the location of the file/s. HDFS or GOA(ej>hdfs,ej>goa): ") 
        self.path = input("Put file/s path, including its name and replacing date with YYYYMMDD(ej: /home/jferreyra/archivo_YYYYMMDD.txt): ")
        self.separator = input("Put file's delimiter(ej>|, ej>,): ")
        self.header =input("Put whether the file has header or not(ej>true, ej>false): ")

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
                time.sleep(3)
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

       self.zonel = input("Put the location of the file/s. HDFS or GOA(ej>hdfs,ej>goa): ")
       self.path =input("Put file/s path, including its name and replacing date with YYYYMMDD(ej: /home/jferreyra/archivo_YYYYMMDD.txt): ")
       self.separator = input("Put file's delimiter(ej>|, ej>,): ")
       self.header =input("Put whether the file has header or not(ej>true, ej>false): ")

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
