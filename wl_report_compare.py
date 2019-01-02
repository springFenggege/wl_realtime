# -*- coding: utf-8 -*-
import requests
import json
import datetime
import pymysql
from requests.cookies import RequestsCookieJar

db=pymysql.connect("*******",  #Host
                           "****",        #数据库账号
                           "*****",     #s数据库密码
                           "*****" ,  # 数据库名称
                           charset='utf8'
                           )
cursor=db.cursor()                  # 使用cursor（）方法创建一个游标对象
cursor.execute(
           "SELECT name,code FROM `wl_instrument`where name in ('集水井提升泵A','集水井提升泵B','调节提升泵A','调节提升泵B','回转风机A','回转风机B','产水泵A','产水泵B','污泥提升泵A','污泥提升泵B','清水泵A','紫外线消毒A','加药泵A','潜水搅拌器A') AND is_deleted=0 AND device_id=103;"
        )
data=cursor.fetchall ()
data_list={}
a={}
ret1={
        '集水井提升泵A':'catchpitLiftPumpARunTime',
		'集水井提升泵B':'catchpitLiftPumpBRunTime',
		'调节提升泵A':'adjustLiftPumpARunTime',
		'调节提升泵B':'adjustLiftPumpBRunTime',
		'回转风机A':'rotaryFanARunTime',
		'回转风机B':'rotaryFanBRunTime',
		'产水泵A':'waterPumpARunTime',
		'产水泵B':'waterPumpBRunTime',
		'污泥提升泵A':'sludgeLiftPumpARunTime',
		'污泥提升泵B':'sludgeLiftPumpBRunTime',
		'清水泵A':'rinsingPumpARunTime',
		'紫外线消毒A':'ultravioletSterilizeARunTime',
		'加药泵A':'medicatePumpARunTime',
		'潜水搅拌器A':'dosingStirrerARunTime'
}

for i in data:
    key=(i[0])
    value=i[1]
    a[key]=value
    data_list.update(a)

url='http://***.envcloud.com.cn:9011/sds/deviceFactorData/getHistoryDataRaw'
parm={
        'startDatetime':1541606400000,
        'factorCodes':3000525,
        'pageSize':'1000000',
        'sort':'asc',
        'deviceId':'DTUMB201807230183',
        'pageIndex':'1',
        'endDatetime':1541692799000
        }
starttime=1546300800000
ci=[]
history_data={}


for i in range(0,24):
    b = []
    # starttime=1541606400000
    endtime=starttime+3600000
    parm['startDatetime']=starttime
    parm['endDatetime']=endtime

    for key in data_list:
        factorCodes=data_list[key]
        parm['factorCodes']=factorCodes
        # print(parm['factorCodes'])


        res=requests.get(url=url,params=parm)
        data=res.json()
        ret = data['ret']
        items = ret['items']
        item_list = []

        for i in range(0, len(items)):
            c = items[i][0]
            item_list.append(c)
        # 初始化时间和value
        time = item_list[0]['time']
        time2 = 0
        if item_list[0]['value'] == 0:
            value = '0'
            for a in range(0, len(items)):
                if value == item_list[a]['value']:
                    continue
                else:
                    time1 = int(item_list[a]['time'])
                    time3 = int(item_list[a - 1]['time'])
                    time2 = time2 + (time1 - time3)
                    # print (time2/1000/60)
        else:
            value = '1'
            for a in range(0, len(items) - 1):
                if value == item_list[a]['value']:
                    time1 = int(item_list[a + 1]['time'])
                    time3 = int(item_list[a]['time'])
                    time2 = time2 + (time1 - time3)

                else:
                    continue
                    # print(time6 / 1000 / 60)
        date = datetime.datetime.fromtimestamp(starttime / 1000)
        code=key

            # print(" time={date} , value={time},code={code}".format(date=date, time=time2 / 1000 / 60,code=key))
        a = {}
        a.setdefault(code,round(time2/1000/60,2))
        # print("a={}".format(a))


        b.append(a)
    starttime = endtime
    # print("b={}".format(b))

    # history_data.setdefault(starttime,b)
    ci.append(b)
# print(ci)


wl_url='http://***.31.250.17:7610/wl/overview/hourlyReportData/selectAllTwo'
wl_pram={
    'startTime':'1546272000000',
    'deviceId':'103'
}
cookies={
    'cookie':'JSESSIONID=node0ejqgraaloh6j19f9n9wlam02y26.node0'
}
headers={
    'accept':'application/json, text/plain, */*',
    'content-type':'application/json;charset=UTF-8',
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
  'Cookie':'JSESSIONID=node0ejqgraaloh6j19f9n9wlam02y26.node0',
    'Referer':'http://118.31.250.17:7001/wl_web/'
}
cookie_jar = RequestsCookieJar()
cookie_jar.set('JSESSIONID','node0ejqgraaloh6j19f9n9wlam02y26.node0')

wl_res=requests.get(url=wl_url,params=wl_pram)
wl_data=wl_res.json()
wl_ret=wl_data['ret']
# print(wl_ret)
wl_dayReportDTOList=wl_ret['dayReportDTOList']
wl_nightReportDTOList=wl_ret['nightReportDTOList']
for hour_data in range(0,11):
    for i in ci[hour_data]:

        for key in sorted(i.keys()) :
            ret1_value=ret1[key]
            history_value=i[key]

        wl_value=wl_dayReportDTOList[hour_data][ret1_value]
        wl_time=wl_dayReportDTOList[hour_data]['hourStr']
        # print(wl_value)
        if int(history_value) == int(wl_value):
                print('true')
        else :
            # print('false')
            print('hour_data is {} , history_data is {},wl_data is {}，code id {}'.format(wl_time,history_value,wl_value,key))


    print('-------------早班------------------')
sor=-1
for hour_data in range(12,23):
    if hour_data >=12:
        sor=sor+1
    for i in ci[hour_data]:

        for key in sorted(i.keys()) :
            ret1_value=ret1[key]
            history_value=i[key]

        wl_value=wl_nightReportDTOList[sor][ret1_value]
        wl_time = wl_nightReportDTOList[sor]['hourStr']
        # print(wl_value)
        if int(history_value) == int(wl_value):
                print('true')
        else :
            # print('false')
            print('hour_data is {} , history_data is {},wl_data is {},code is {}'.format(wl_time,history_value,wl_value,key))


    print('---------------晚班----------------')
