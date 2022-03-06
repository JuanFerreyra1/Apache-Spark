import os
from pyspark.sql import SparkSession
import re

class Adjustment():
        
        def __init__(self):
                self.sesion_de_spark = SparkSession.builder\
                  .enableHiveSupport()\
                  .appName("PowerfulMotorSparkEngine")\
                  .config('spark.executors.cores', '4')\
                  .config('spark.hadoop.hive.exec.dynamic.partition', 'true')\
                  .config('spark.hadoop.hive.exec.dynamic.partition.mode', 'nonstrict')\
                  .getOrCreate()
                self.tab = input("Put table's name: ")
                self.dir_search = input("Put dir's name in which there are files with incorrect header: ")
                self.target_dir = input("Put target dir's name in which all the fixed files are going to be: ")
                self.separator = input("Put file's separator: ")   

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