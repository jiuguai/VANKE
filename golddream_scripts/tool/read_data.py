import re
import os 
import sys
import datetime
import time
import numpy as np
import pandas as pd

def _read_mx(mx_dir,patt):
    path = mx_dir

    #  合并表格

    df_mx = pd.DataFrame()
    df_cwmx = pd.DataFrame()
    com = re.compile(patt)

    
    file_dic = {}
    for x in os.listdir(path):
        z = com.match(x)
        book_name = re.sub("\.xls[xm]?","",x)
        if '~' not in x and '河西未转'not in x and z: 
            area = z.group('area')
            print('工作簿' + book_name )
            try:
                print('读取' + area + '商业签约明细')
                
                dfsy = pd.read_excel(path + "\/" + x,sheetname = area+"商业签约明细")
                dfsy['book_name'] = book_name
                print('合并' + area + '商业签约明细')
                df_mx = pd.concat([df_mx, dfsy]) 
            except Exception as e:
                print('->无'+ area + '商业签约明细表')

            try:
                print('读取' + area + '签约明细')
                dfzf = pd.read_excel(path + "\/" + x,sheetname = area+"签约明细")
               
                dfzf['book_name'] = book_name
                print('合并' + area + '签约明细')
                df_mx = pd.concat([df_mx, dfzf])
            except Exception as e:
                print('--->无'+ area + '签约明细表')
                
            try:
                print('读取' + area + '车位明细')
                dfcw = pd.read_excel(path + "\/" + x,sheetname = area+"车位明细")
                dfcw['book_name'] = book_name
                print('合并' + area + '车位明细')
                df_cwmx = pd.concat([df_cwmx, dfcw])
            except Exception as e:
                print('--->无'+ area + '车位明细表')


            print()
            print('明细表读取完毕')

    return df_mx,df_cwmx



def _handle_mx(df_mx):
    if len(df_mx) == 0:
        return df_mx

    df_mx1 = df_mx

    #  去除无效字段
    df_mx1.dropna(subset=['序号'],inplace=True)

    df_mx1.reset_index(drop=True,inplace=True)


    df_mx1['签约日期'] = pd.to_datetime(df_mx1['签约日期'])
    """
    新增加字段 网签日期
    """

    f_date = datetime.datetime(2017,3,17)
    df_mx1['网签签字日期'] = pd.to_datetime(df_mx1['备案异常情况'].astype(str).str.extract(r'(20\d{2}/\d{1,2}/\d{1,2})(?=已签字|已网签)',expand=False))
    df_mx1['网签日期'] = pd.to_datetime(df_mx1['单机改网签日期'])

    # 行业和住宅 签约日期 = 网签日期(小于2017-3-17)
    # df_mx1['网签日期'][df_mx1['签约日期'] < f_date] = \
    #     df_mx1['签约日期'][df_mx1['签约日期'] < f_date]

    # 住宅 2017-3-17 签约日期 = 网签日期
    df_mx1['网签日期'][(df_mx1['类型'] == "住宅") & (df_mx1['签约日期'] < f_date)] = \
        pd.to_datetime(df_mx1['签约日期'][(df_mx1['类型'] == "住宅") & (df_mx1['签约日期'] < f_date)])

    # 商业 网签日期 = 签约日期
    df_mx1['网签日期'][df_mx1['类型'] == "商业"  ] = pd.to_datetime(df_mx1['签约日期'][df_mx1['类型'] == "商业"])
    

    df_t = df_mx1[['网签签字日期','网签日期', '客户契税/维修基金']]

    df_t1 = df_t.copy()
    df_t1['完成网签日期'] = np.nan

    try:
        df_t1['完成网签日期'][(df_t1['网签签字日期'].notnull()) & (df_t1['网签日期'].notnull()) & (df_t1['客户契税/维修基金'].notnull())] = \
        df_t[(df_t['网签签字日期'].notnull()) & (df_t['网签日期'].notnull()) & (df_t['客户契税/维修基金'].notnull())].apply(lambda x:max(x),axis=1)
    except NotImplementedError:
        pass

    df_mx1['完成网签日期'] = pd.to_datetime(df_t1['完成网签日期'])

    

    # 生成 key
    df_mx1['key'] = df_mx1['项目']+'-'+df_mx1['类型']+'-'+df_mx1['合并房号']
    df_mx1['key_r'] = df_mx1['项目']+'-'+df_mx1['类型']+'-'+df_mx1['合并房号'].fillna("").astype(str).str.strip()


    return df_mx1

# 处理车位签约明细
def _handle_cwmx(df_cwmx):
    if len(df_cwmx) == 0:
        return df_cwmx
    re_col = {
        "首次转明源日期":"首次转签约日期",
        "明源转签约日期":"转签约日期",
        "车位签约日期":"签约日期",
        "车位业主姓名":"姓名",
        "车位号码":"房号",
        "车位总价":"买卖合同总价",
        "地址":"联系地址",
        "已付金额":"首付金额",   
        # "交付时间":"交房时间",
    }
    df_cwmx.rename(columns=re_col,inplace=True)
    df_cwmx.reset_index(drop=True,inplace=True)
    df_cwmx['签约日期'] = \
        pd.to_datetime(df_cwmx['签约日期'].fillna("").astype(str).str.strip().str.extract(r"(20\d{2}-\d{1,2}-\d{1,2})",expand=False))

    # df_cwmx['项目'] = df_cwmx['项目'].str.replace('金域(滨江)|金域(缇香)',r"\1").str.replace('金域国际','百汇')
    df_cwmx['key'] = df_cwmx['项目'] + "-" +df_cwmx['房号'].fillna("").astype(str) + "-" + df_cwmx['姓名'] 

    df_cwmx['房号'] = df_cwmx['房号'].fillna("").astype(str).str.replace("\s","").\
                    str.extract("([a-zA-Z0-9]+(?:[-=][a-zA-Z0-9]+)?)")

    # 滨江明细表特殊处理
    df_cwmx['房号'][df_cwmx['项目']=="滨江"] = \
    df_cwmx['房号'][df_cwmx['项目']=="滨江"].fillna("").astype(str).str.replace('[a-zA-Z]',"")

    df_cwmx['姓名'] = df_cwmx['姓名'].str.strip().str.replace("\s+|，|;|；",",")
    df_cwmx['key_r'] = df_cwmx['项目'] + "-" +df_cwmx['房号'].fillna("").astype(str) + "-" + df_cwmx['姓名'] 

    df_cwmx['key_r'] = df_cwmx['key_r'].str.strip("；;,，")
    df_cwmx['key_r'] = df_cwmx['key_r'].str.replace("[a-zA-Z0-9]+=","")
    df_cwmx['key_r'] = df_cwmx['key_r'].str.replace("([a-zA-Z])-",r"\1")


    return df_cwmx
def _handle_zmx(data):
    if len(data) == 0:
        return data
    if "移交情况" in data.columns:
        data['移交情况'] = data['移交情况'].fillna("").astype(str).str.replace("00:00:00","").str.strip()
        data['移交日期']  = pd.to_datetime(data['移交情况'].str.extract("(20\d{2}[\-\./]\d{1,2}[\-\./]\d{1,2})",expand=False))
        data['移交状态']  = data['移交情况'].str.replace("20\d{2}[\-\./]\d{1,2}[\-\./]\d{1,2}(?:已移交)?|已移交","已移交")

    if "备案_业务宗号" in data.columns:
        data['备案_业务宗号'] = data['备案_业务宗号'].fillna("").astype(str).str.replace("\.0",'')

    # 转签约异常 字段处理

    if "转签约异常" in data.columns:
        data['转签约异常'] = data['转签约异常'].fillna("").astype(str).str.strip().fillna("")
    else:
        data['转签约异常'] = ""
    return data.reset_index(drop=True)

def read_mx(mx_dir,area=".+?"):
    patt="(?!<$)(?P<area>(?:"+ area + r"))签约明细.*?\.xls[xm]?"
    df_mx,df_cwmx = _read_mx(mx_dir,patt)
    df_mx1 = _handle_mx(df_mx)
    df_cwmx1 = _handle_cwmx(df_cwmx)
    df_zmx = pd.concat([df_mx1,df_cwmx1]).reset_index(drop=True)
    df_zmx = _handle_zmx(df_zmx)
    return df_zmx

def file_timestamp2date(dir_name,timestamp_handle=lambda x:x*1000000 + 9*3600*1000000):
    com_patt = r'^(?P<f_pre>[^_]+?)_(?P<date>\d+)(?P<f_ext>\.xls[xm])$'
    com = re.compile(com_patt)
    for f_name in os.listdir(dir_name):
        x = com.match(f_name)
        if x:
            str_d = x.groupdict().get('date')
            temp_date = pd.to_datetime(timestamp_handle(int(str_d)))
            new_name = os.path.join(dir_name,x.groupdict().get('f_pre') + temp_date.strftime("%Y-%m-%d %H_%M_%S") + x.groupdict().get('f_ext'))
            old_name = os.path.join(dir_name,f_name)
            os.rename(old_name,new_name)

def update_vk_rep_path(dir_name,com_patt,format_date,extend_name = 'xlsx'):

    patt = com_patt
    com = re.compile(patt)
    date = pd.to_datetime('1900-1-1')
    file_name = None
    for f_name in os.listdir(dir_name):
        x = com.match(f_name)
        if x:
            str_d = x.groupdict().get('date')
            temp_date = pd.to_datetime(str_d,format=format_date)
            date = date 
            if date < temp_date:
                file_name = f_name
                date = temp_date
                
    if file_name:

        return os.path.join(dir_name,file_name)
    else:
        return None


def read_vanke_rep(rep_dir,com_patt,format_date,rep_name,header=0):
    path = update_vk_rep_path(rep_dir,com_patt,format_date)
    if path:
        print('读取最新%s' %rep_name)
        print(path)
        start_time = time.time()
        data = pd.read_excel(path,header=header)
        # data_temp = pd.read_excel(path,sheet_name=sheet_name,nrows=0)
        # if '房间ID' in data_temp.columns:
        #     data = pd.read_excel(path,sheet_name=sheet_name)
        # else:
        #     data = pd.read_excel(path,sheet_name=sheet_name,skiprows=1)
        end_time = time.time()

        print('耗时%0.2f秒' %(end_time - start_time))

    else:
        print('请确认%s目录' %rep_name)
        
        return pd.DataFrame()

    return data

def read_lp(cwlp_dir, xm_lp_map_mx,header=0):
    com_patt = r'^财务楼盘表(?P<date>20\d{2}-\d{2}-\d{2} \d{2}_\d{2}_\d{2})\.xlsx$'
    format_date = '%Y-%m-%d %H_%M_%S'

    rep_name = "财务楼盘表"
    df_lp = read_vanke_rep(cwlp_dir,com_patt,format_date,rep_name,header=header)

    df_lp = _handle_lp(df_lp,xm_lp_map_mx)
    return df_lp

def read_ssmx(ssmx_dir, xm_lp_map_mx,header=0):
    com_patt = r'^实收款明细表(?P<date>20\d{2}-\d{2}-\d{2} \d{2}_\d{2}_\d{2})\.xlsx$'
    format_date = '%Y-%m-%d %H_%M_%S'
   
    rep_name = "实收款明细表"
    df_ssmx = read_vanke_rep(ssmx_dir,com_patt,format_date,rep_name,header=header)

    df_ssmx = _handle_ssmx(df_ssmx,xm_lp_map_mx)

    return df_ssmx



def _handle_lp(data, xm_lp_map_mx):
    # 
    data['认购日期'] = pd.to_datetime(data['认购日期'])
    data = data[data['项目名称'].isin(xm_lp_map_mx.keys())]
    data.replace({"项目名称":xm_lp_map_mx},inplace=True)

    data['签约日期'] = pd.to_datetime(data['签约日期'])
    data.rename(columns={"认购日期":"认购时间",'签约日期':"转签约日期"},inplace=True)
    data['认购日期'] = data['认购时间'].dt.floor('d')
    data['产品类型'] = data['产品类型全称'].str[:2]

    # 生成楼盘的合并房号
    data['房号'] = data['房号'].fillna("").astype(str)
    data['房号'] = data['房号'].str.extract("([a-zA-Z\d\-]+)",expand=False)
    data['楼栋'] = data['营销楼栋名称'].fillna("").astype(str).str.extract('(?P<ld>[BS]?\d+)',expand=False)
    data['楼盘_合并房号'] = data['楼栋'] + "-" + data['房号']
    data['楼盘_合并房号'][data['产品类型']=='车位'] = data['房号'][data['产品类型']=='车位'].fillna("").astype(str)

    data['客户名称'] = data['客户名称'].fillna("").astype(str)

    data.rename(columns={'欠款金额(不含面积补差）':"欠款",'实收款合计':"实收"},inplace=True)
    
    # 商业和住宅 lp_key生成
    data_sz = data[data['产品类型'] != "车位"]

    data_sz['lp_key']= data_sz['项目名称'] + '-'+data_sz['产品类型'] + '-' + data_sz['楼栋'] + '-'+ data_sz['房号']
    data_sz['lp_key'] = data_sz['lp_key'].str.replace('(?<=-)-','S')

    # 楼盘车位 lp_key生成
    data_cw = data[data['产品类型']=="车位"]

    data_cw['lp_key'] = data_cw['项目名称'] + "-" + data_cw['房号'] + "-" + data_cw['客户名称']


    data = pd.concat([data_sz,data_cw])
    return data.reset_index(drop=True)


def _handle_ssmx(data, xm_lp_map_mx):
    data = data[data['项目名称'].isin(xm_lp_map_mx.keys())]
    data.replace({"项目名称":xm_lp_map_mx},inplace=True)


    return data.reset_index(drop=True)