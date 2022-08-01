from flask import Flask
from flask import request
import requests
import json
from flask_cors import CORS

app = Flask(__name__)

CORS(app, resources=r'/*')

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
    while(i<len(data)):
        vol=data[i][9]
        data[i]=data[i][:6]
        data[i].append(vol)
        i+=1
    data=data[:34]# 取近34天（0-33）的数据（第30天的要用到30-34的数据来计算ma5）
    return json.dumps(data)


if __name__ == '__main__':
    app.run()
