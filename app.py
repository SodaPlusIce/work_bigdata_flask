from flask import Flask
from flask import request
import requests
import json
from flask_cors import CORS
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
import tushare as ts
import pandas as pd
import pymysql

app = Flask(__name__)

# 与mysql的连接
connect = pymysql.connect(host='120.46.152.35', user='root', password='123456',
                          database='Quantitative_Trading_Service_System',charset='utf8')
cursor = connect.cursor()

# 以下是spark的处理
ts.set_token('96dfb6f8b1fd4d5e4972d66a49f72523746fd493cc56d921675a406a')
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
def calMa5BySpark(ts_code,datebefore,date):
    ma_result = count_ma(ts_code, datebefore, date)  # args explanation:股票代码、开始时间、结束时间
    # 请注意传来的开始结束时间,是‘YYYYMMDD’的字符串形式
    # ma_result.show()
    ma_result = ma_result.toPandas()  # 此行是将spark的df转换为pandas的df，如果你很熟悉spark的df，那么可以不用加,不过还是建议用pandas的dataframe
    # print(ma_result)
    return ma_result

CORS(app, resources=r'/*') # 解决跨域

@app.route('/getDataByCode')
def getDataByCode():  # put application's code here
    url = 'http://api.tushare.pro'
    ts_code = request.args["ts_code"]
    # ts_code = "000001.SZ"
    body = {
        "api_name": "daily",
        "token": "0d43464d84c399737c36d0dc8a2b32a32be9a193e8203230668f66f2",
        "params": {"ts_code": ts_code},
        "fields": ""
    }
    res = requests.post(url, data=json.dumps(body))
    # json1 = res.content.decode('utf-8')
    res_json = res.json()
    data = res_json["data"]["items"]
    i=0
    # 获取ma5等值
    # data=data[:34]# 取近34天（0-33）的数据（第30天的要用到30-34的数据来计算ma5）
    data=data[:30]# 取近30天（0-29）的数据
    madata=calMa5BySpark(ts_code,data[29][1],data[0][1])
    while(i<len(data)):
        vol=data[i][9]
        data[i]=data[i][:6]
        data[i].append(vol)
        data[i].append(round(madata["ma5"][i],2))
        data[i].append(round(madata["ma10"][i],2))
        data[i].append(round(madata["ma20"][i],2))
        data[i].append(round(madata["ma30"][i],2))
        sql = "INSERT INTO stocks_basicinfo ( ts_code, trade_date,open,high,low,close,vol,ma5,ma10,ma20,ma30 )" \
              " VALUES ( %s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s );"
        tempdata=[data[i][0], data[i][1],data[i][2],data[i][3],data[i][4],data[i][5],data[i][6],
                  data[i][7],data[i][8],data[i][9],data[i][10]]
        cursor.execute(sql,tempdata)
        connect.commit()
        i+=1
    cursor.close()
    connect.close()
    return json.dumps(data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
