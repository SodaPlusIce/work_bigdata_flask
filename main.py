import tushare as ts
import pandas as pd
from pyspark import SparkConf, SparkContext
import warnings
ts.set_token('96dfb6f8b1fd4d5e4972d66a49f72523746fd493cc56d921675a406a')
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
# 此函数是将传入的startdate和enddate转换为时间戳的字符串形式,即‘YYYY-MM-DD’
def convert_timestamp(df):
    temp=df.iloc[:, 1].tolist()
    for i in range(len(temp)):
        temp[i] = temp[i][0:4] + '-' + temp[i][4:6] + '-' + temp[i][6:8]
    # print('temp', temp)
    df.iloc[:, 1] = temp
    return df
def count_ma(ts_code,startdate,enddate):
    spark=SparkSession.builder.appName("count_ma5").master("local[*]").getOrCreate()
    sc=spark.sparkContext
    pro = ts.pro_api()
    df=pro.query('daily', ts_code=ts_code, start_date=startdate, end_date=enddate)

    df=pd.DataFrame(df)
    df = convert_timestamp(df)
    df=spark.createDataFrame(df)

    df=df.withColumn('trade_date',df.trade_date.cast('timestamp'))
    day=lambda x:x*86400
    dayitem=['5','10','20','30']
    for ma_day in dayitem:
     winSpec = Window.partitionBy('ts_code').orderBy(F.col('trade_date').cast('long')).rangeBetween(-day(int(ma_day)), 0)
     df = df.withColumn('ma'+ma_day, F.avg('close').over(winSpec))
    return df

if __name__ == '__main__':
   ma_result=count_ma('000001.SZ','20220230','20220330')# args explanation:股票代码、开始时间、结束时间
                                                     # 请注意传来的开始结束时间,是‘YYYYMMDD’的字符串形式
   ma_result.show()
   ma_result=ma_result.toPandas() # 此行是将spark的df转换为pandas的df，如果你很熟悉spark的df，那么可以不用加,不过还是建议用pandas的dataframe
   print(ma_result["trade_date"][0])
   print(ma_result["ma5"][0])