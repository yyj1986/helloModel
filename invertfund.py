from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from datetime import datetime, timedelta

import akshare as ak
import pandas.io.sql as sql
import pandas as pd

# class CassDB:

#     def __init__(self):
#         auth_provider = PlainTextAuthProvider(
#             username="cassandra",
#             password="cass@20080808"
#         )

#         self.cluster = Cluster(['10.239.1.215'], auth_provider=auth_provider)
#         self.session = self.cluster.connect('finance')

#     def close(self):
#         self.cluster.shutdown()

def get_rante(start, range, netlist):

    #设定定投日期
    tran_date = start

    #设定定投时长
    time_range=range

    end_date =tran_date - timedelta(days=time_range)

    #设定定投周期
    period='周一'

    offset = tran_date.weekday()
    d = tran_date - timedelta(days=offset )

    data_list=[]
    data_list.append(d.strftime('%Y-%m-%d'))

    while d > end_date:
        d = d- timedelta(days=7)
        if d.strftime('%Y-%m-%d') =='2020-10-05':
            data_list.append('2020-10-09')
        elif d.strftime('%Y-%m-%d') =='2021-02-15':
            data_list.append('2021-02-18')
        elif d.strftime('%Y-%m-%d') =='2021-04-05':
            data_list.append('2021-04-06')
        elif d.strftime('%Y-%m-%d') =='2021-05-03': 
            data_list.append('2021-05-06') 
        elif d.strftime('%Y-%m-%d') =='2021-06-14': 
            data_list.append('2021-06-15')      
        else: 
            data_list.append(d.strftime('%Y-%m-%d'))

    print(data_list)



    # #获取净值历史数据
    # #例子“S05009400015” --东方新能源汽车主题
    # rows = self.session.execute('''
    #     select  product_id, trandate, netvalue from t02_fund_netvalue_h
    #         where product_id=%s order by trandate desc
    #     ''',(product,))

    value_list=[]

    quot = 0

    percount= 0

    for r in netlist: 
        t = str(r['fund_dt'])
        s = r['fund_netvalue']
        if  t in data_list:
            netvalue = r['fund_netvalue']
            quot= quot + round(1/netvalue, 4)
            percount= percount + 1
            value_list.append(s)
    print(value_list)        

    print(percount,quot,value_list[0])

    #计算收益率
    
    rate=round((quot*value_list[0]-percount)/percount*100,2)

    print(rate)

def get_fund_netvalue():
    '''获取基金净值'''
    data = ak.fund_em_open_fund_info(fund="217019", indicator="单位净值走势")

    df = pd.DataFrame(data)

    df.columns=['dt','netvalue','']

    print(df)

    results = []
    # 从iterrows转itertuples
    for r in df.itertuples():
        
        d = dict()
        d['fund_dt'] = str(r.dt)
        d['fund_netvalue'] = r.netvalue
        
        results.append(d)

    results.sort(key=lambda x: x['fund_dt'], reverse=True)

    return results

if __name__  == '__main__':
    #获取收益率
    net_list=get_fund_netvalue()
    #print(net_list)
    get_rante(datetime.today(),365,net_list)

         









