import csv

class Utils:
    def read_csv(self,spark,path_name):
        df_result_csv = spark.read.options(header='True',inferSchema='True',delimiter=',',escape='"',quote='"',multiLine=True).\
        csv(path_name)
        return df_result_csv
    
    def read_csv_rdd(self,sc,path_name,titles):
        rdd = sc.textFile(path_name)
        rdd = rdd.mapPartitions(lambda x: csv.reader(x))
        firstRow=rdd.first()
        df_result_csv=rdd.filter(lambda row:row != firstRow).toDF(titles)
        return df_result_csv
    
    def write_df_parquet(self,df,path_name):
        df.write.mode("overwrite").parquet(path_name)
       
    def read_parquet(self,spark,path_name):
        df_result_parquet=spark.read.parquet(path_name)
        return df_result_parquet