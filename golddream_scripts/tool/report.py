import datetime

import numpy as np
import pandas as pd
from .tools import _hz,get_7days,get_interval_data,clear_type
from .data_merge import mx_lp_merge
"""转签约


"""
# 今日新增认购
def rg_newly_t(df_lp,date,index_l=None,is_agg=True):
    data = df_lp[df_lp['认购日期'] == date]
    
    if is_agg:
        data.rename(columns={"项目名称":"项目","成交总价":"买卖合同总价"},inplace=True)
        return _hz(data,'今日新增认购',index_l)
    return data

# 认购未签
def rg_no_qy(df_mx,df_lp,date,index_l=None,is_agg=True):
    df_lp_rg = df_lp[(df_lp['销售状态']=='认购')].dropna(subset=['房间编码'])
    df_lp_rg = df_lp_rg[df_lp_rg['认购日期'] <= date]
    s = set(df_lp_rg['房间编码']) - set(df_mx['房间编码'])
    df_lp_rg = df_lp_rg[df_lp_rg['房间编码'].isin(s)]
    
    if is_agg:
        df_lp_rg.rename(columns={"项目名称":"项目","成交总价":"买卖合同总价"},inplace=True)
        return _hz(df_lp_rg,'认购未签',index_l)
    return df_lp_rg



# 已草签未转销售系统签约
def ocq_no_sys(df_mxlp,date,index_l=None,is_agg=True,is_break_rules=False):

    df_mxlp = df_mxlp[df_mxlp['签约日期'] <= date]

    # 只有 不遵守流程的 才会出现 签约了 还存在未认购的状态 
    if is_break_rules:
        df_mxlp = df_mxlp[df_mxlp['销售状态']=='认购']
    data =  df_mxlp[
                    (df_mxlp['签约日期'].notnull()) & 
                    (df_mxlp['转签约日期'].isnull()) &
                    (df_mxlp['网签日期'].isnull())     
                  ]
    if is_agg:
        return _hz(data,'已草签未转',index_l)
    return data



# 网签待转
def wq_wait_sys(df_mxlp,date,index_l=None,is_agg=True,is_break_rules=False):
    df_mxlp = df_mxlp[df_mxlp['网签日期'] <= date]

    # 只有 不遵守流程的 才会出现 签约了 还存在未认购的状态 
    if is_break_rules:
        df_mxlp = df_mxlp[df_mxlp['销售状态']=='认购']

    data = df_mxlp[
                     (df_mxlp['网签日期'].notnull()) & 
                      (df_mxlp['转签约日期'].isnull())

                      ]
    if is_agg:
        return _hz(data,'网签待转',index_l)
    return data

# 已签未转
def qy_no_sys(df_mxlp,date,index_l=None,is_agg=True):
    df_mxlp = df_mxlp[df_mxlp['签约日期'] <= date]
    data = df_mxlp[
                    (df_mxlp['签约日期'].notnull()) & 
                      (df_mxlp['转签约日期'].isnull())
                      ]
    if is_agg:
        return _hz(data,'已签未转',index_l)
    return data

# 转签约
def zqy_t(df_mxlp,date,index_l=None,is_agg=True):
    data = df_mxlp[df_mxlp['转签约日期']==date]
    if is_agg:
        return _hz(data,'今日转签约',index_l)
    return data

# 本月转签约
def zqy_m(df_mxlp,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(df_mxlp,"转签约日期",date,start_date)
    if is_agg:
        return _hz(data,'本月转签约',index_l)
    return data

# 已草签待转
def cq_wait_sys(df_mxlp,date,index_l=None,is_agg=True,is_break_rules = False):
    df_mxlp = df_mxlp[df_mxlp['签约日期'] <= date]

    # 只有 不遵守流程的 才会出现 签约了 还存在未认购的状态 
    if is_break_rules:
        df_mxlp = df_mxlp[df_mxlp['销售状态']=='认购']
    data =  df_mxlp[
                    (df_mxlp['类型']=='车位') &
                    (df_mxlp['签约日期'].notnull()) & 
                    (df_mxlp['转签约日期'].isnull())   
                  ]

    if is_agg:
        return _hz(data,'已草签待转',index_l)
    return data

# 已草签待网签
def cq_wait_wq(df_mxlp,date,index_l=None,is_agg=True):

    df_mxlp = df_mxlp[df_mxlp['签约日期'] <= date]

    data =  df_mxlp[
                    (df_mxlp['类型']!='车位') &
                    (df_mxlp['网签日期'].isnull()) & 
                    (df_mxlp['转签约日期'].isnull()) 
 
                  ]

    if is_agg:
        return _hz(data,'已草签待网签',index_l)
    return data


# 已签待转
def qy_wait_sys(df_mxlp,date,index_l=None,is_agg=True):

    data1 = cq_wait_sys(df_mxlp,date,is_agg=False)
    
    data2 = wq_wait_sys(df_mxlp,date,is_agg=False)
    data = pd.concat([data1,data2])
    if is_agg:
        return _hz(data,'已签待转',index_l)
    return data


"""签约

"""
# 今日
 
# 签约
def qy_t(data,date,index_l=None,is_agg=True):
    data = data[data['签约日期'] == date]

    if is_agg:
        return _hz(data,'今日签约',index_l)
    return data

# 商业住宅签约
def qy_sz_t(data,date,index_l=None,is_agg=True):
    data = data[data['类型'] != '车位']
    data = data[data['签约日期'] == date]

    if is_agg:
        return _hz(data,'今日_住宅/商铺',index_l)
    return data

# 车位
def qy_cw_t(data,date,index_l=None,is_agg=True):
    data = data[data['类型'] == '车位']
    data = data[data['签约日期'] == date]

    if is_agg:
        return _hz(data,'今日_车位',index_l)
    return data

# 本月
 
# 签约
def qy_m(data,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'签约日期',date,start_date)

    if is_agg:
        return _hz(data,'本月签约',index_l)
    return data

# 
def qy_sz_m(data,date,index_l=None,is_agg=True):
    data = data[data['类型'] != '车位']
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'签约日期',date,start_date)

    if is_agg:
        return _hz(data,'本月_住宅/商铺',index_l)
    return data

def qy_cw_m(data,date,index_l=None,is_agg=True):
    data = data[data['类型'] == '车位']
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'签约日期',date,start_date)

    if is_agg:
        return _hz(data,'本月_车位',index_l)
    return data

"""网签

"""
def wq_t(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期'] == date]

    if is_agg:
        return _hz(data,"今日网签",index_l)
    return data

def wq_m(data,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'网签日期',date,start_date)

    if is_agg:
        return _hz(data,"本月网签",index_l)
    return data

def wq_y(data,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,1,1))
    data = get_interval_data(data,'网签日期',date,start_date)

    if is_agg:  
        return _hz(data,"本年网签",index_l)
    return data

# 往年签约今年网签
def fyqy_tywq(data,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,1,1))
    data = get_interval_data(data,'网签日期',date,start_date)
    data = data[data['签约日期'] < start_date]

    if is_agg:
        return _hz(data,"往年签约今年网签",index_l)
    return data

# 今年签约今年网签
def tyqy_tywq(data,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,1,1))
    data = get_interval_data(data,'网签日期',date,start_date)
    data = data[data['签约日期']>=start_date]

    if is_agg:
        return _hz(data,"今年签约今年网签",index_l)
    return data


def wqp_no_sign(data,date,index_l=None,is_agg=True):
    data = data[data['备案异常情况'].astype(str).str.contains('未签字|未网签')
               ]
    if is_agg:  
        return _hz(data,"已通过网签未签字正式合同",index_l)
    return data


"""完成网签


"""
def wcwq_t(data,date,index_l=None,is_agg=True):
    data = data[data['完成网签日期'] == date]

    if is_agg:
        return _hz(data,"今日完成网签",index_l)
    return data

def wcwq_m(data,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'完成网签日期',date,start_date)

    if is_agg:
        return _hz(data,"本月完成网签",index_l)
    return data

def wcwq_zq(data,date,index_l=None,is_agg=True):
    interval = get_7days(date)
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,interval[0]))
    target_date = pd.to_datetime(datetime.datetime(date.year,date.month,interval[1]))
    data = get_interval_data(data,'完成网签日期',date,start_date)

    if is_agg:
        return _hz(data,"%(m)s.%(sd)s-%(m)s.%(ed)s完成网签" %{'m':date.month,'sd':interval[0],'ed':interval[1]},index_l)
    return data
"""备案


"""
def ba_t(data,date,index_l=None,is_agg=True):
    data = data[data['送备案时间'] == date]

    if is_agg:
        return _hz(data,"今日备案",index_l)
    return data

def ba_m(data,date,index_l=None,is_agg=True):
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'送备案时间',date,start_date)

    if is_agg:
        return _hz(data,"本月备案",index_l)
    return data

def ba_zq(data,date,index_l=None,is_agg=True):
    interval = get_7days(date)
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,interval[0]))
    target_date = pd.to_datetime(datetime.datetime(date.year,date.month,interval[1]))
    data = get_interval_data(data,'送备案时间',date,start_date)

    if is_agg:
        return _hz(data,"%(m)s.%(sd)s-%(m)s.%(ed)s备案" %{'m':date.month,'sd':interval[0],'ed':interval[1]},index_l)
    return data

"""银行

汇总
"""
def bank_loan_t(data,date,is_agg=True,is_sum=False):
    data = data[data['付款方式'] != "一次性"]

    data = data[data['签约日期'] == date]
    
    if is_agg:
        return _hz(data,"本日银行按揭",index_name="付款方式",agg_name="贷款金额",is_sum=is_sum)
    return data

def bank_loan_m(data,date,is_agg=True,is_sum=False):
    data = data[data['付款方式'] != "一次性"]

    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,"签约日期",date,start_date)
    
    if is_agg:
        return _hz(data,"本月银行按揭",index_name="付款方式",agg_name="贷款金额",is_sum=is_sum)
    return data

"""未回款

"""
# 认购未签
def rg_no_qy_rp(df_mx,df_lp,date,index_l=None,is_agg=True):
    data = rg_no_qy(df_mx,df_lp,date,is_agg=False)
    data.rename(columns={'项目名称':"项目"},inplace=True)
    data = data[['项目','欠款']]
    data = data[data['欠款'] > 0]
    if is_agg:
        return _hz(data,"认购未签",index_l=index_l,agg_name="欠款")
    return data

# 已草签未转
def ocq_no_sys_rp(df_mxlp,date,index_l=None,is_agg=True):
    data = ocq_no_sys(df_mxlp,date,is_agg=False)
    data = data[['项目','欠款'] ]
    data = data[data['欠款'] > 0]
    if is_agg:
        return _hz(data,"已草签未转",index_l=index_l,agg_name="欠款")
    return data

# 网签待转
def wq_wait_sys_rp(df_mxlp,date,index_l=None,is_agg=True):
    data = wq_wait_sys(df_mxlp,date,is_agg=False)
    data = data[['项目','欠款'] ]
    data = data[data['欠款'] > 0]
    if is_agg:
        return _hz(data,"网签待转",index_l=index_l,agg_name="欠款")
    return data

# 已通过网签未签字正式合同
def wqp_no_sign_rp(df_mxlp,date,index_l=None,is_agg=True):
    data = wqp_no_sign(df_mxlp,date,is_agg=False)
    data = data[['项目','欠款'] ]
    data = data[data['欠款'] > 0]
    if is_agg:
        return _hz(data,"已通过网签未签字正式合同",index_l=index_l,agg_name="欠款")
    return data


"""定金-楼款 剩余欠款


"""

def pay_user_loan_t(df_lp,df_ssmx,date,index_l=None,is_agg=True):
    data = df_ssmx[['房间ID','款项类型','款项名称','缴费日期', '业务类型','实收金额(元)']]
    data = data[data['款项类型'] == '非贷款类房款']
    data = data[data['款项名称'].isin([  '定金', '首期', '楼款'])]
    
    data = data[data['缴费日期'] == date ]
    id_set = set(data['房间ID'])
    data = df_lp[['项目名称','房间编码','客户名称','楼盘_合并房号','欠款','房间ID']]
    data.rename(columns={"项目名称":"项目"},inplace=True)
    data = data[data['房间ID'].isin(id_set)]
    data = data[data['欠款']>0]
    if is_agg:
        return _hz(data,"今日交付-定金-楼款",index_l=index_l,agg_name="欠款")
    return data

def pay_user_loan_m(df_lp,df_ssmx,date,index_l=None,is_agg=True):
    data = df_ssmx[['房间ID','款项类型','款项名称','缴费日期', '业务类型','实收金额(元)']]
    data = data[data['款项类型'] == '非贷款类房款']
    data = data[data['款项名称'].isin([  '定金', '首期', '楼款'])]
    
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,"缴费日期",date,start_date)

    id_set = set(data['房间ID'])
    data = df_lp[['项目名称','房间编码','客户名称','楼盘_合并房号','欠款','房间ID']]
    data.rename(columns={"项目名称":"项目"},inplace=True)
    data = data[data['房间ID'].isin(id_set)]
    data = data[data['欠款']>0]
    if is_agg:
        return _hz(data,"本月交付-定金-楼款",index_l=index_l,agg_name="欠款")
    return data

"""贷款


"""
# 贷款今日网签
def wq_loan_t(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]
    data = wq_t(data,date,is_agg=False)
    
    
    data = data[data['贷款金额']>0]
    if is_agg:
        data = data[['项目','贷款金额']]
        return _hz(data,'贷款_今日网签',index_l=index_l,agg_name="贷款金额")
    return data

# 贷款 _ 本月网签
def wq_loan_m(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]

    data = wq_m(data,date,is_agg=False)
    
    
    data = data[data['贷款金额']>0]
    if is_agg:
        data = data[['项目','贷款金额']]
        return _hz(data,'贷款_本月网签',index_l=index_l,agg_name="贷款金额")
    return data

# 本日 客户契税/维修基金
def scottare_t(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]
    data = data[data['客户契税/维修基金'] == date]
    if is_agg:
        return _hz(data,"本日_缴税",index_l=index_l)
    return data

# 本月 客户契税/维修基金
def scottare_m(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'客户契税/维修基金',date,start_date)
    if is_agg:
        return _hz(data,"本月_缴税",index_l=index_l)
    return data



# 本日_贷款银行资料
def fund_loan_t(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]
    data = clear_type(data,'银行备案资料',str)
    data['银行备案资料']  = pd.to_datetime(data['银行备案资料'])
    data = data[data['银行备案资料'] == date]
    
    data = data[data['贷款金额']>0]
    if is_agg:
        data = data[['项目','贷款金额']]
        return _hz(data,"贷款_本日银行资料",index_l=index_l,agg_name="贷款金额")
    return data


# 本月_贷款银行资料

def fund_loan_m(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]
    data = clear_type(data,'银行备案资料',str)
    data['银行备案资料']  = pd.to_datetime(data['银行备案资料'])
    
    start_date = pd.to_datetime(datetime.datetime(date.year,date.month,1))
    data = get_interval_data(data,'银行备案资料',date,start_date)


    
    data = data[data['贷款金额']>0]
    if is_agg:
        data = data[['项目','贷款金额']]
        return _hz(data,"贷款_本月银行资料",index_l=index_l,agg_name="贷款金额")
    return data

# 本日完成网签
def ba_loan_t(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]
    data = ba_t(data,date,is_agg=False)
    
    data = data[data['贷款金额']>0]
    if is_agg:
        data = data[['项目','贷款金额']]
        return _hz(data,'贷款_本日备案',index_l=index_l,agg_name="贷款金额")
    return data

# 本月完成网签
def ba_loan_m(data,date,index_l=None,is_agg=True):
    data = data[data['网签日期']>=pd.to_datetime("2018-12-1")]
    data = ba_m(data,date,is_agg=False)
    
    data = data[data['贷款金额']>0]
    if is_agg:
        data = data[['项目','贷款金额']]
        return _hz(data,'贷款_本月备案',index_l=index_l,agg_name="贷款金额")
    return data

"""
合并所需 的子项汇总

"""
def hb_hz(hz_ls):
    df_hz = pd.concat(hz_ls,axis=1).fillna(0)
    return df_hz


def hz(data_dic,today,index_ls,save_dir=None):
    df_mx = data_dic['签约明细']
    df_lp = data_dic['财务楼盘']
    df_ssmx = data_dic['实收明细']

    mx_l = ['房间编码','项目','类型', '签约日期','网签日期','买卖合同总价', '付款方式', '贷款金额', '首付金额', '贷款银行','备案异常情况']
    lp_l = ['房间编码','项目名称','客户名称','认购日期','转签约日期','销售状态','产品类型全称','成交总价','营销楼栋名称','房号','实收','欠款']
    df_lp_r = df_lp[lp_l]
    df_mx_r = df_mx[mx_l]
    df_mxlp = pd.merge(df_lp_r,df_mx_r,how='right',on="房间编码")
    df_mxlp.dropna(subset=["房间编码"],inplace=True)
    df_mxlp.reset_index(drop=True,inplace=True)
    
    index_l = index_ls[0]
    sjtb_index_l =index_ls[1] 


    d = {}

    zqy_l = []
    zqy_l.append(rg_newly_t(df_lp_r,today,index_l=index_l))
    zqy_l.append(rg_no_qy(df_mx_r,df_lp_r,today,index_l=index_l))
    zqy_l.append(ocq_no_sys(df_mxlp,today,index_l=index_l))
    zqy_l.append(wq_wait_sys(df_mxlp,today,index_l=index_l))
    zqy_l.append(qy_no_sys(df_mxlp,today,index_l=index_l))
    zqy_l.append(zqy_t(df_mxlp,today,index_l=index_l))
    zqy_l.append(zqy_m(df_mxlp,today,index_l=index_l))
    zqy_l.append(cq_wait_sys(df_mxlp,today,index_l=index_l))
    zqy_l.append(cq_wait_wq(df_mxlp,today,index_l=index_l))
    zqy_l.append(qy_wait_sys(df_mxlp,today,index_l=index_l))


    d['转签约'] = hb_hz(zqy_l)
    d['转签约'].columns.names = ["转签约",""]

    rp_l = []
    rp_l.append(rg_no_qy_rp(df_mx,df_lp,today,index_l=index_l))
    rp_l.append(ocq_no_sys_rp(df_mxlp,today,index_l=index_l))
    rp_l.append(wq_wait_sys_rp(df_mxlp,today,index_l=index_l))
    rp_l.append(wqp_no_sign_rp(df_mxlp,today,index_l=index_l))

    d['未回款'] = hb_hz(rp_l)
    d['未回款'].columns.names = ["未回款",""]


    qy_l = []
    qy_l.append(qy_sz_t(df_mx,today,index_l=index_l))
    qy_l.append(qy_sz_m(df_mx,today,index_l=index_l))
    qy_l.append(qy_cw_t(df_mx,today,index_l=index_l))
    qy_l.append(qy_cw_m(df_mx,today,index_l=index_l))
    qy_l.append(qy_t(df_mx,today,index_l=index_l))
    qy_l.append(qy_m(df_mx,today,index_l=index_l))
    d['签约'] = hb_hz(qy_l)
    d['签约'].columns.names = ["签约",""]
    
    wq_l = []
    wq_l.append(wq_t(df_mx,today,index_l=index_l))
    wq_l.append(wq_m(df_mx,today,index_l=index_l))
    wq_l.append(wq_y(df_mx,today,index_l=index_l))
    wq_l.append(fyqy_tywq(df_mx,today,index_l=index_l))
    wq_l.append(tyqy_tywq(df_mx,today,index_l=index_l))
    wq_l.append(wqp_no_sign(df_mx,today,index_l=index_l))
    d['网签'] = hb_hz(wq_l)
    d['网签'].columns.names = ["网签",""]

    wcwq_ba_l = []
    wcwq_ba_l.append(wcwq_t(df_mx,today,index_l=sjtb_index_l))
    wcwq_ba_l.append(wcwq_zq(df_mx,today,index_l=sjtb_index_l))
    wcwq_ba_l.append(wcwq_m(df_mx,today,index_l=sjtb_index_l))
    wcwq_ba_l.append(ba_t(df_mx,today,index_l=sjtb_index_l))
    wcwq_ba_l.append(ba_zq(df_mx,today,index_l=sjtb_index_l))
    wcwq_ba_l.append(ba_m(df_mx,today,index_l=sjtb_index_l))
    d['完成网签和备案'] = hb_hz(wcwq_ba_l)
    d['完成网签和备案'].columns.names = ["完成网签和备案",""]

    bank_loan_l = []
    bank_loan_l.append(bank_loan_t(df_mx,today))
    bank_loan_l.append(bank_loan_m(df_mx,today))

    d['银行按揭'] = hb_hz(bank_loan_l)
    d['银行按揭'].columns.names = ["银行按揭",""]


    # 欠款
    Arrears_l = []
    Arrears_l.append(pay_user_loan_t(df_lp,df_ssmx,today,index_l=index_l))
    Arrears_l.append(pay_user_loan_m(df_lp,df_ssmx,today,index_l=index_l))


    d['欠款'] = hb_hz(Arrears_l)
    d['欠款'].columns.names = ["欠款",""]

    # 贷款_缴税 
    loan_l = []

    loan_l.append(wq_loan_t(df_mx,today,index_l=index_l))
    loan_l.append(wq_loan_m(df_mx,today,index_l=index_l))
    loan_l.append(scottare_t(df_mx,today,index_l=index_l))
    loan_l.append(scottare_m(df_mx,today,index_l=index_l))
    loan_l.append(fund_loan_t(df_mx,today,index_l=index_l))
    loan_l.append(fund_loan_m(df_mx,today,index_l=index_l))
    loan_l.append(ba_loan_t(df_mx,today,index_l=index_l))
    loan_l.append(ba_loan_m(df_mx,today,index_l=index_l))
    d['贷款_缴税'] = hb_hz(loan_l)
    d['贷款_缴税'].columns.names = ["贷款_缴税",""]

    if save_dir:
        save_path = save_dir + r'\work\%s-%s-%s汇总表.xlsx' %(today.year,today.month,today.day)
        writer = pd.ExcelWriter(save_path)
        start_row = 0
        d['转签约'].to_excel(writer,sheet_name="report",startrow = start_row)
        start_row = start_row + len(d['转签约']) + 4


        d['未回款'].to_excel(writer,sheet_name="report",startrow = start_row)
        start_row = start_row + len(d['未回款']) + 4

        d['签约'].to_excel(writer,sheet_name="report",startrow = start_row)
        start_row = start_row + len(d['签约']) + 4

        d['网签'].to_excel(writer,sheet_name="report",startrow = start_row)
        start_row = start_row + len(d['网签']) + 4

        d['完成网签和备案'].to_excel(writer,sheet_name="report",startrow = start_row)
        start_row = start_row + len(d['完成网签和备案']) + 4

        d['银行按揭'].T.to_excel(writer,sheet_name="report",startrow = start_row)
        
        start_row = 0
        d['欠款'].to_excel(writer,'欠款贷款缴税',startrow = start_row)
        start_row = start_row + len(d['欠款']) + 4

        d['贷款_缴税'].to_excel(writer,sheet_name="欠款贷款缴税",startrow = start_row)
        


        writer.save()


    return d

def rep_map(data_dic,today,index_ls,save_dir=None):
    df_mx = data_dic['签约明细']
    df_lp = data_dic['财务楼盘']
    df_ssmx = data_dic['实收明细']

    mx_l = ['房间编码','项目','类型', '签约日期','网签日期','买卖合同总价', '付款方式', '贷款金额', '首付金额', '贷款银行','备案异常情况']
    lp_l = ['房间编码','项目名称','客户名称','认购日期','转签约日期','销售状态','产品类型全称','成交总价','营销楼栋名称','房号','实收','欠款']
    df_lp_r = df_lp[lp_l]
    df_mx_r = df_mx[mx_l]
    df_mxlp = pd.merge(df_lp_r,df_mx_r,how='right',on="房间编码")
    df_mxlp.dropna(subset=["房间编码"],inplace=True)
    df_mxlp.reset_index(drop=True,inplace=True)
    
    index_l = index_ls[0]
    sjtb_index_l =index_ls[1] 

    
    
    qy_control_l = []
    d= {}
    qy_control_l.append(pay_user_loan_t(df_lp,df_ssmx,today,index_l=index_l))
    qy_control_l.append(pay_user_loan_m(df_lp,df_ssmx,today,index_l=index_l))

    qy_control_l.append(wq_loan_t(df_mx,today,index_l=index_l))
    qy_control_l.append(wq_loan_m(df_mx,today,index_l=index_l))

    df_t = scottare_t(df_mx,today,index_l=index_l)
    df_t.drop(columns=['金额(万元)'],level=1,inplace=True)
    qy_control_l.append(df_t)

    df_t = scottare_m(df_mx,today,index_l=index_l)
    df_t.drop(columns=['金额(万元)'],level=1,inplace=True)
    qy_control_l.append(df_t)

    qy_control_l.append(fund_loan_t(df_mx,today,index_l=index_l))
    qy_control_l.append(fund_loan_m(df_mx,today,index_l=index_l))
    qy_control_l.append(ba_loan_t(df_mx,today,index_l=index_l))
    qy_control_l.append(ba_loan_m(df_mx,today,index_l=index_l))
    
    # 信息反馈     
    qy_l = []
    qy_l.append(qy_sz_t(df_mx,today,index_l=index_l))
    qy_l.append(qy_sz_m(df_mx,today,index_l=index_l))
    qy_l.append(qy_cw_m(df_mx,today,index_l=index_l))
    qy_l.append(qy_m(df_mx,today,index_l=index_l))
    d['签约'] = hb_hz(qy_l)
    d['签约'].columns.names = ["签约",""]
    
    d['签约总控'] = hb_hz(qy_control_l)
    d['签约总控'].columns.names = ["签约总控",""]
    
    
    # 数据通报
    tb_l = []
    tb_l.append(rg_newly_t(df_lp_r,today,index_l=index_l))
    tb_l.append(rg_no_qy(df_mx_r,df_lp_r,today,index_l=index_l))
    tb_l.append(qy_no_sys(df_mxlp,today,index_l=index_l))



    tb_l.append(qy_sz_t(df_mx,today,index_l=index_l))
    tb_l.append(qy_cw_t(df_mx,today,index_l=index_l))
    tb_l.append(qy_sz_m(df_mx,today,index_l=index_l))
    tb_l.append(qy_cw_m(df_mx,today,index_l=index_l))




    tb_l.append(zqy_t(df_mxlp,today,index_l=index_l))
    tb_l.append(zqy_m(df_mxlp,today,index_l=index_l))

    d['短信报表'] = hb_hz(tb_l)
    d['短信报表'].columns.names = ["短信报表",""]
    x = {
        '^中.+?湖大.*?$':"中湖",
        '^长沙.*?$':"长沙银行",
        '农业银行天心区支行':'农天',
        '^中信.*?$':"中信",
        '^邮[政储].+?$':"邮政",
        '^光大.*?$':"光大",
        '^工行.*?$|^工商.*?$':"工行",
        '^(?:招行|招商).*?$':'招行',
        '^.*?组合贷$':"组合贷",
        '^市公积金$':"市公",
        '^省公积金$':"省公",
        '^交通.*?$':"交行",
        '^民生.*?$':"民生",
        '^.*?一次性$|一次性转账':"一次性",
        '农业银行芙蓉区支行':"农芙",
        '^华融.*?$':'华融'
    }
    df_mx['付款方式'] = df_mx['付款方式'].str.strip()
    df_mx.replace({'付款方式':x},regex=True)['付款方式'].unique()

    l  = ["光大", "民生", "农芙", "中湖", "中信", "邮政", 
          "工行", "交行", "农天", "长沙银行", "招行", "市公", "省公", "部队公积金", "华夏", "民生湘府路支行", "组合贷"]

    df_mx_loan = df_mx[['项目','类型','贷款金额','签约日期','付款方式']]


    bank_loan_l = []
    df_mx_loan.replace({'付款方式':x},regex=True,inplace=True)
    bank_loan_l.append(bank_loan_t(df_mx_loan,today))
    bank_loan_l.append(bank_loan_m(df_mx_loan,today))

    d['银行按揭'] = hb_hz(bank_loan_l)
    d['银行按揭'].columns.names = ["银行按揭",""]
    data = d['银行按揭']
    if len(d['银行按揭']) == 0:
        d['银行按揭'].loc[l[0]] = 0
    d['银行按揭'] = d['银行按揭'].loc[l].fillna(0)



    # 认购签约
    rg_qy_l = []
    is_break_rules = True
    rg_qy_l.append(rg_no_qy(df_mx_r,df_lp_r,today,index_l=index_l))
    rg_qy_l.append(ocq_no_sys(df_mxlp,today,index_l=index_l,is_break_rules=is_break_rules))
    rg_qy_l.append(wq_wait_sys(df_mxlp,today,index_l=index_l,is_break_rules=is_break_rules))
    rg_qy_l.append(wqp_no_sign(df_mx,today,index_l=index_l))
    d['认购签约'] = hb_hz(rg_qy_l)
    d['认购签约'].columns.names = ["认购签约",""]
    
    # 网签
    wq_l = []
    wq_l.append(wq_t(df_mx,today,index_l=index_l))
    wq_l.append(wq_m(df_mx,today,index_l=index_l))
    wq_l.append(wq_y(df_mx,today,index_l=index_l))
    wq_l.append(fyqy_tywq(df_mx,today,index_l=index_l))
    wq_l.append(tyqy_tywq(df_mx,today,index_l=index_l))
    
    
    
    d['网签'] = hb_hz(wq_l)
    d['网签'].columns.names = ["网签",""]
    
    
    
    # 未回款
    rp_l = []
    rp_l.append(rg_no_qy_rp(df_mx,df_lp,today,index_l=index_l))
    rp_l.append(ocq_no_sys_rp(df_mxlp,today,index_l=index_l))
    rp_l.append(wq_wait_sys_rp(df_mxlp,today,index_l=index_l))
    rp_l.append(wqp_no_sign_rp(df_mxlp,today,index_l=index_l))

    
    d['未回款'] = hb_hz(rp_l)
    d['未回款'].columns.names = ["未回款",""]
    
    
    sjtb_l = []
    sjtb_l.append(wcwq_t(df_mx,today,index_l=sjtb_index_l))
    sjtb_l.append(wcwq_zq(df_mx,today,index_l=sjtb_index_l))
    sjtb_l.append(wcwq_m(df_mx,today,index_l=sjtb_index_l))
    sjtb_l.append(ba_t(df_mx,today,index_l=sjtb_index_l))
    sjtb_l.append(ba_zq(df_mx,today,index_l=sjtb_index_l))
    sjtb_l.append(ba_m(df_mx,today,index_l=sjtb_index_l))

    data_x = rg_no_qy_rp(df_mx_r,df_lp_r,today,is_agg=False)
    data_y = ocq_no_sys_rp(df_mxlp,today,is_agg=False)

    data = pd.concat([data_x,data_y])

    sjtb_l.append(_hz(data,"认购未回款",index_l = sjtb_index_l,index_name='项目',agg_name="欠款"))

    d['数据通报'] = hb_hz(sjtb_l)
    d['数据通报'].columns.names = ["数据通报",""]

   



    
    if save_dir:
        writer = pd.ExcelWriter(save_dir + r'\map_rep\rep_map.xlsx')
        d['签约总控'].loc[['总和']].to_excel(writer,sheet_name="签约总控")
        d['签约'].to_excel(writer,sheet_name="信息反馈")
        d['网签'].to_excel(writer,sheet_name="网签")
        d['数据通报'].to_excel(writer,sheet_name="数据通报")
        d['未回款'].to_excel(writer,sheet_name="未回款")
        d['短信报表'].to_excel(writer,sheet_name="短信报表")
        d['认购签约'].to_excel(writer,sheet_name="认购签约")
        d['银行按揭'].T.to_excel(writer,sheet_name="短信报表_银行按揭")

        writer.save()

    return d