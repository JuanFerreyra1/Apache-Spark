from pyspark.sql import SparkSession


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