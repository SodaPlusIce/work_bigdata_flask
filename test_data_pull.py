import requests
import json


url = 'http://api.tushare.pro'
body = {
    "api_name": "stock_basic",
    "token":"0d43464d84c399737c36d0dc8a2b32a32be9a193e8203230668f66f2",
    "params":{"list_status":"L"},
    "fields":""
}

response = requests.post(url, data = json.dumps(body))
json1 = response.content.decode('utf-8')
print(json1)


# 导入tushare
import tushare as ts

# 初始化pro接口
pro = ts.pro_api('0d43464d84c399737c36d0dc8a2b32a32be9a193e8203230668f66f2')

# 拉取数据
df = pro.stock_basic(**{
    "ts_code": "",
    "name": "",
    "exchange": "",
    "market": "",
    "is_hs": "",
    "list_status": "",
    "limit": "",
    "offset": ""
}, fields=[
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "market",
    "list_date"
])
print(df)

# try it

