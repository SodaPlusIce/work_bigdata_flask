from flask import Flask
from flask import request
import requests
import json

app = Flask(__name__)


@app.route('/getDataByCode')
def getDataByCode():  # put application's code here
    url = 'http://api.tushare.pro'
    # ts_code = request.json.get("ts_code").strip() # 接受参数
    ts_code = "000001.SZ"
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
    return json.dumps(data)


if __name__ == '__main__':
    app.run()
