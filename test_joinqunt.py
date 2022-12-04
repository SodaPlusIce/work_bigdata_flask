from jqdatasdk import *

auth('13383909875','13383909875Zc') #账号是申请时所填写的手机号；密码为聚宽官网登录密码
# count=get_query_count()
# print(count)
#获取平安银行按1分钟为周期以“2015-01-30 14:00:00”为基础前4个时间单位的数据
df = get_price('000001.XSHE', end_date='2022-08-10 14:00:00',count=4, frequency='minute', fields=['open','close','high','low','volume','money'])
print(df)