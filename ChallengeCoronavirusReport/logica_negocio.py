from pyspark.sql.types import *
from pyspark.sql.functions import *

class BusinessLogic:   
    def join_df(self,dfa,dfb,como):
        dfa_with_dfb=dfa.join(dfb,dfa.CountryRegion==dfb.CountryRegion,how=como).drop(dfb.CountryRegion)
        return dfa_with_dfb
    
    def transformacion_tipos_datos_covid(self,df):
        df_result_tipos_datos=df.select(col('Country/Region').cast(StringType()).alias('CountryRegion'),
                  col('Continent').cast(StringType()).alias('Continent'),
                  col('Population').cast(IntegerType()).alias('Population'),
                  col('TotalCases').cast(IntegerType()).alias('TotalCases'),
                  col('NewCases').cast(IntegerType()).alias('NewCases'),
                  col('TotalDeaths').cast(IntegerType()).alias('TotalDeaths'),
                  col('NewDeaths').cast(IntegerType()).alias('NewDeaths'),
                  col('TotalRecovered').cast(IntegerType()).alias('TotalRecovered'),
                  col('NewRecovered').cast(IntegerType()).alias('NewRecovered'),
                  col('ActiveCases').cast(IntegerType()).alias('ActiveCases'),
                  col('Serious,Critical').cast(IntegerType()).alias('Serious/Critical'),
                  col('Tot Cases/1M pop').cast(IntegerType()).alias('TotCases/1Mpop'),
                  col('Deaths/1M pop').cast(IntegerType()).alias('Deaths/1Mpop'),
                  col('TotalTests').cast(IntegerType()).alias('TotalTests'),
                  col('Tests/1M pop').cast(IntegerType()).alias('Tests/1Mpop'),
                  col('WHO Region').cast(StringType()).alias('WHORegion'))
        return df_result_tipos_datos
    
    def transformacion_tipos_datos_covid_grouped(self,df):
        df_result_tipos_datos=df.select(col('Date').cast(StringType()).alias('Date'),
                                       col('Country/Region').cast(StringType()).alias('CountryRegion'),
                                       col('Confirmed').cast(IntegerType()).alias('Confirmed'),
                                       col('Deaths').cast(IntegerType()).alias('Deaths'),
                                       col('Recovered').cast(IntegerType()).alias('Recovered'),
                                       col('Active').cast(IntegerType()).alias('Active'),
                                       col('New cases').cast(IntegerType()).alias('NewCases'),
                                       col('New deaths').cast(IntegerType()).alias('NewDeaths'),
                                       col('New recovered').cast(IntegerType()).alias('NewRecovered'),
                                       col('WHO Region').cast(StringType()).alias('WHORegion'))
        return df_result_tipos_datos
    
    def calcular_casos_por_mes(self,df):
        df_result_casos=df.groupBy('Continent',
                   'CountryRegion',
                   'Population',
                   'Anio',
                   'Mes').agg(sum('Confirmed').alias('Confirmed'),
                              sum('Deaths').alias('Deaths'),
                              sum('Recovered').alias('Recovered'),
                              sum('Active').alias('Active'),
                              sum('NewCases').alias('NewCases'),
                              sum('NewDeaths').alias('NewDeaths'),
                              sum('NewRecovered').alias('NewRecovered')
                             ).orderBy('CountryRegion','Anio','Mes')
        return df_result_casos