import json
import pprint
import requests
import numpy as np
import pandas as pd
import warnings
import time

# 遍历所有文件
from concurrent.futures import ThreadPoolExecutor


warnings.filterwarnings('ignore') 


# 获取所有 项目ID
def get_stageid_df(token):
    url = "http://s.vanke.com/api/object/salesOrg/lookup/companyId/01dd9dee7e92465da650bd0fc56817b4/1/1000/ASC/name"


    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": token,
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Cookie": "zg_did=%7B%22did%22%3A%20%2216a1059f48d1ff-00b64bccdb224f-661f1574-13c680-16a1059f48e1f1%22%7D; TY_SESSION_ID=4a4cb282-2622-444a-ab9d-c6694715ce94; LtpaToken=AAECAzVDQzhFNzU0NUNDOTkwMTR2MDI1MDMwN5dSpgVoshVL4pNyQJOITjjU/Olq; zg_a3cdecc7e3d74f2eb46869b9e73f96f5=%7B%22sid%22%3A%201556692355076%2C%22updated%22%3A%201556692618281%2C%22info%22%3A%201556592606682%2C%22superProperty%22%3A%20%22%7B%5C%22%E5%BA%94%E7%94%A8%E5%90%8D%E7%A7%B0%5C%22%3A%20%5C%22%E7%BF%BC%E9%94%80%E5%94%AE%5C%22%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%2210.0.3.182%22%2C%22cuid%22%3A%20%220731bc6d8301404993a008edaf6dd2cc%22%7D",
        "Host": "s.vanke.com",
        "Pragma": "no-cache",
        "Referer": "http://s.vanke.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
        "X-Tingyun-Id": "p35OnrDoP8k;r=%i" % (int(time.time()*1000) % 1e9)
        }
    stageStructure_json = requests.get(url,headers=headers).json()
    stageid_l = []
    stargename_l = []
    for stage in stageStructure_json['body']['list']:
        stageid_l.append(stage['id'])
        stargename_l.append(stage['name'])

    return pd.DataFrame({"分期名称":stargename_l,"分期ID":stageid_l})
    

# 返回 已签约 tradeID 集合
def get_contract_svc_set(stageid,token):
    headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Authorization":token ,
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Length": "433",
    "Content-Type": "application/json;charset=UTF-8",
    "Cookie": "zg_did=%7B%22did%22%3A%20%2216a1059f48d1ff-00b64bccdb224f-661f1574-13c680-16a1059f48e1f1%22%7D; TY_SESSION_ID=cbbee443-bd96-4435-b561-331b7ede2dc1; LtpaToken=AAECAzVDQ0E0NkQ2NUNDQUVGOTZ2MDI1MDMwN6Nel145+PHA8OkpH6abmLdasWb0; zg_a3cdecc7e3d74f2eb46869b9e73f96f5=%7B%22sid%22%3A%201556760289459%2C%22updated%22%3A%201556760863799%2C%22info%22%3A%201556592606682%2C%22superProperty%22%3A%20%22%7B%5C%22%E5%BA%94%E7%94%A8%E5%90%8D%E7%A7%B0%5C%22%3A%20%5C%22%E7%BF%BC%E9%94%80%E5%94%AE%5C%22%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22%22%2C%22cuid%22%3A%20%220731bc6d8301404993a008edaf6dd2cc%22%7D",
    "Host": "s.vanke.com",
    "Origin": "http://s.vanke.com",
    "Pragma": "no-cache",
    "Referer": "http://s.vanke.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
    "X-Tingyun-Id": "p35OnrDoP8k;r=%i" % (int(time.time()*1000) % 1e9)
    }
    orderlist = {'condition': {'businessTypeId': 'SD2001003',
                   'consultantIdList': '',
                   'consultantNames': '',
                   'consultantTeamIdList': '',
                   'contractNo': '',
                   'flag': '1',
                   'relateOrderNo': '',
                   'roomName': '',
                   'salesTeamIdList': '',
                   'signDateEnd': '',
                   'signDateStart': '',
                   'stageId': stageid,
                   'tradeCustomerNames': '',
                   'tradeStatusId': 'SD2002001,SD2002002'},
     'number': 1,
     'relation': 'and',
     'size':1,
     'sortDirection': 'DESC',
     'sortProperties': ['sign_date']}


    url = "http://s.vanke.com/api/saleMemberSvc/getOrderIdListByItem"
    tradeid_json = requests.post(url,headers=headers,data=json.dumps(orderlist)).json()

    total_count = tradeid_json['body']["totalCount"]
    
    orderlist['size'] = total_count

    tradeid_json = requests.post(url,headers=headers,data=json.dumps(orderlist)).json()

    tradeid_l = tradeid_json['body']['list']
    return {d['id'] for d in tradeid_l}



# 认购 状态下的 tradeID 集合
def get_order_svc_set(stageid,token):
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Authorization": token,
        "Connection": "keep-alive",
        "Content-Length": "491",
        "Content-Type": "application/json;charset=UTF-8",
        "Cookie": "zg_did=%7B%22did%22%3A%20%2216a1059f48d1ff-00b64bccdb224f-661f1574-13c680-16a1059f48e1f1%22%7D; TY_SESSION_ID=4a4cb282-2622-444a-ab9d-c6694715ce94; LtpaToken=AAECAzVDQzhFNzU0NUNDOTkwMTR2MDI1MDMwN5dSpgVoshVL4pNyQJOITjjU/Olq; zg_a3cdecc7e3d74f2eb46869b9e73f96f5=%7B%22sid%22%3A%201556673961916%2C%22updated%22%3A%201556675326129%2C%22info%22%3A%201556592606682%2C%22superProperty%22%3A%20%22%7B%5C%22%E5%BA%94%E7%94%A8%E5%90%8D%E7%A7%B0%5C%22%3A%20%5C%22%E7%BF%BC%E9%94%80%E5%94%AE%5C%22%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%2210.0.3.182%22%2C%22cuid%22%3A%20%220731bc6d8301404993a008edaf6dd2cc%22%7D",
        "Host": "s.vanke.com",
        "Origin": "http://s.vanke.com",
        "Referer": "http://s.vanke.com/",
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
        "X-Tingyun-Id": "p35OnrDoP8k;r=%i" % (int(time.time()*1000) % 1e9)
    }

    orderlist = {'condition': {'approvalFlag': '',
                   'businessTypeId': 'SD2001002',
                   'consultantIdList': '',
                   'consultantNames': '',
                   'consultantTeamIdList': '',
                   'expectSignDateEnd': '',
                   'expectSignDateStart': '',
                   'flag': '0',
                   'lateFlag': '',
                   'orderNo': '',
                   'roomName': "",
                   'salesTeamIdList': '',
                   'signDateEnd': '',
                   'signDateStart': '',
                   'stageId':stageid,
                   'tradeCustomerNames': '',
                   'tradeStatusId': 'SD2002001,SD2002002'},
     'number': 1,
     'relation': 'and',
     'size': 1,
     'sortDirection': 'DESC',
     'sortProperties': ['sign_date']}




    url = "http://s.vanke.com/api/saleMemberSvc/getOrderIdListByItem"
    tradeid_json = requests.post(url,headers=headers,data=json.dumps(orderlist)).json()

    total_count = tradeid_json['body']["totalCount"]
    total_count
    orderlist['size'] = total_count

    tradeid_json = requests.post(url,headers=headers,data=json.dumps(orderlist)).json()

    tradeid_l = tradeid_json['body']['list']
    return {d['id'] for d in tradeid_l}


# 提取json 数据
def get_trade_info(body,show_info=False):
    if show_info:
        print(body)

    if body['body']["orderContractBasic"]['tradeStatusIdName'] == "作废":
        return None

    d = {}




    # 房间编码
    d['房间编码'] = body['body']['orderContractBasic']['roomOutCode']

    # 交易价格
    d["买卖合同总价"] =  body['body']["orderContractBasic"]['dealPriceWithTax']

    # 首付 （实际免税）
    d["首付金额"]  = body['body']["orderContractBasic"]['actualMoneyWithTax']

    # 装修总价
    d["装修总价"] =  body['body']["orderContractBasic"]['decorationTotal']

    # 毛坯总价
    d["毛坯总价"] = body['body']["orderContractBasic"]['housePriceWithTax']
    
    try:
        # 含税贷款额
        d["贷款金额"] = body['body']["contractLoan"]['loanAmountWithTax']
    except:
         d["贷款金额"] = 0

    # 交易面积
    d['建筑面积'] = body['body']["orderContractBasic"]['salesArea']

    # 认购日期
    d['认购日期'] = body['body']["orderContractBasic"]['createDate']

    try:

        d['付款方式'] = body['body']["contractLoan"]['loanName']
        if d["付款方式"] == "":
            d['付款方式'] = body['body']["contractLoan"]['housingFundBankName']
    except:
        try:
            d['付款方式'] = body['body']["contractLoan"]['housingFundBankName']
        except:
            d['付款方式'] = ""

    # 项目名称
    d['项目'] = body['body']['orderContractBasic']['roomIdObject']['projectStructureName']

    # 房源信息
	#     body['body']['orderContractBasic']['roomIdObject']['proStructureLongName']


    # 分期名称
    d['分期名称'] = body['body']['orderContractBasic']['roomIdObject']['stageStructureName']

    # 楼栋
    d['楼栋'] = body['body']['orderContractBasic']['roomIdObject']['buildingStructureName']

    # 房号
    d['房号'] = body['body']['orderContractBasic']['roomIdObject']['contractHouseId']


    # 类型
	#     d['类型'] = body['body']['houseResource']['productTypeName']
    
    d['类型'] = body['body']['orderContractBasic']['productType']
    
    # 置业顾问
    consultant_name_l = []
    for consultant in body['body']['consultantDetailList']:
        consultant_name_l.append(consultant['consultantName'])
    d['置业顾问'] =  ';'.join(consultant_name_l)
    
    name_l = []
    phone_num_l = []
    addr_s = set({})
    id_l = []
    for user_dic in body['body']['customerTransactionList']:
        name_l.append(user_dic['name'])
        id_l.append(user_dic['certificateNumber'])
        phone_num_l.append(user_dic['phoneNumber'])

        addr_s.add(user_dic['contactAddress'])



    d["姓名"] = ','.join(name_l)
    d['身份证号码'] = ','.join(id_l)
    d['联系电话'] = ','.join(phone_num_l)
    d['联系地址'] = ';'.join(addr_s)
   


    
    return d

# 返回 数据字典
def get_data_dic(tradeid,token,area= "orderSvc"):
    """
    认购签约
    area = "orderSvc" 
    已签约
    area= "contractSvc"
    """
    url = "http://s.vanke.com/api/%s/%s" %(area,tradeid)
    headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Authorization": token,
    "Connection": "keep-alive",
    "Cookie": "zg_did=%7B%22did%22%3A%20%2216a1059f48d1ff-00b64bccdb224f-661f1574-13c680-16a1059f48e1f1%22%7D; TY_SESSION_ID=4a4cb282-2622-444a-ab9d-c6694715ce94; LtpaToken=AAECAzVDQzhFNzU0NUNDOTkwMTR2MDI1MDMwN5dSpgVoshVL4pNyQJOITjjU/Olq; zg_a3cdecc7e3d74f2eb46869b9e73f96f5=%7B%22sid%22%3A%201556679356629%2C%22updated%22%3A%201556679419554%2C%22info%22%3A%201556592606682%2C%22superProperty%22%3A%20%22%7B%5C%22%E5%BA%94%E7%94%A8%E5%90%8D%E7%A7%B0%5C%22%3A%20%5C%22%E7%BF%BC%E9%94%80%E5%94%AE%5C%22%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%2210.0.3.182%22%2C%22cuid%22%3A%20%220731bc6d8301404993a008edaf6dd2cc%22%7D",
    "Host": "s.vanke.com",
    "Referer": "http://s.vanke.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36",
    "X-Tingyun-Id": "p35OnrDoP8k;r=%i" % (int(time.time()*1000) % 1e9)
    }

    body = requests.get(url,headers=headers).json()

    data_dic = get_trade_info(body)
    
    return data_dic



# 返回 爬取的数据
def get_data(tradeid_set,token,area= "orderSvc",max_workers=30):
    """
    认购签约
    area = "orderSvc" 
    已签约
    area= "contractSvc"
    """
    
    futures=[]
    executor=ThreadPoolExecutor(max_workers=max_workers)
    t_start = time.time()

    for tradeid in tradeid_set:

    	future = executor.submit(get_data_dic,tradeid,token,area)
    	futures.append(future)
    executor.shutdown(True)
    data_l = []
    for future in futures:
        item = future.result()
        if item:
    	    data_l.append(future.result())
    t_end = time.time()

    print("%ss" %(t_end - t_start))
    data = pd.DataFrame(data_l)
    return data

def save_data_map(data,path,sheet_name="data"):
    writer = pd.ExcelWriter(path)
    data.to_excel(writer,sheet_name=sheet_name,index=False)
    writer.save()



# 装饰 映射数据
def dec_data(data,pro_map_dic=None):
    if pro_map_dic:
        data.replace({"项目":pro_map_dic},inplace=True)
    data['类型'] = data['类型'].str[:2]
    # 生成楼盘的合并房号
    data['房号'] = data['房号'].fillna("").astype(str)

    data['房号'] = data['房号'].str.extract("([a-zA-Z\d\-]+)",expand=False)
    data['楼栋'] = data['楼栋'].fillna("").astype(str).str.extract('(?P<ld>[BS]?\d+)',expand=False)
    data['合并房号'] = data['楼栋'] + "-" + data['房号']
    data['合并房号'][data['类型']=='车位'] = data['房号'][data['类型']=='车位'].fillna("").astype(str)
    data['认购日期'] = pd.to_datetime(data['认购日期']).dt.floor('d')
    data.reset_index(drop=True)
    ld_dic= {
    "11":"GX190012",
    "12":"GX190013",
    "13":"GX190010",
    "14":"GX190011",
    "15":"GX190016",
    "16":"GX190015",
    "17":"GX190014",
    }
    data['合同编号'] = np.nan
    l = ['11', '12', '13', '14', '15', '16', '17']

    data['合同编号'][(data['类型']!='车位') & (data['楼栋'].isin(l))] = \
    data['楼栋'][(data['类型']!='车位') & (data['楼栋'].isin(l))].apply(lambda x:ld_dic[x]) + data['房号'][data['类型']!='车位']
    
    data['装修总价'][(data['建筑面积'] > 97) & (data['建筑面积'] < 104)] = 2182 *  data['建筑面积'][(data['建筑面积'] > 97) & (data['建筑面积'] < 104)]
    data['装修总价'][(data['建筑面积'] > 115) & (data['建筑面积'] < 121)] = 2357 *  data['建筑面积'][(data['建筑面积'] > 115) & (data['建筑面积'] < 121)]
    data['装修总价'][(data['建筑面积'] > 133) & (data['建筑面积'] < 139)] = 2278 *  data['建筑面积'][(data['建筑面积'] > 133) & (data['建筑面积'] < 139)]
    
    data['装修总价'] = np.around(data['装修总价'])
    data['毛坯总价'] = data['买卖合同总价'] - data['装修总价']
    
    data = data[['房间编码','合并房号',"付款方式", '买卖合同总价', '分期名称', '姓名', '建筑面积', '房号', '楼栋', '毛坯总价', '类型',
       '置业顾问', '联系地址', '联系电话', '装修总价', '认购日期', '贷款金额', '身份证号码', '项目', '首付金额',
        '合同编号']]



    return data