import pandas as pd
from .data_merge import mx_lp_merge
from .report import *
from .tools import add_order,not_unique_part,unique_part

def to_msg(save_dir,date,df_mx):
    data = pd.DataFrame(columns=["content"])
    data.index.name = "message"
    data.loc['今日备案量'] = len(df_mx['送备案时间'][df_mx['送备案时间'] == date])
    data.loc['本年网签'] = len(df_mx['网签日期'][(df_mx['网签日期'].dt.year == date.year) & (df_mx['网签日期'] <= date)])
    data.loc['今日网签'] = len(df_mx['网签日期'][df_mx['网签日期'] == date])
    writer = pd.ExcelWriter(save_dir+r"\msg_data\msg_data.xlsx")
    data.to_excel(writer,sheet_name="email")
    writer.save()


def trace_cs_qy(df_mx,result_dir):
    fields = ['项目','楼栋','房号','合并房号','姓名','贷款金额','付款方式','放款时间','客户契税/维修基金','类型']

    data = df_mx[fields]
    data = data[data['类型'] != '车位']
    data.rename(columns={"付款方式":"银行","贷款金额":"按揭金额",'姓名':'业主姓名','放款时间':"放款日期","客户契税/维修基金":"客户领走合同日期"},inplace=True)
    data['城市'] = "长沙"
    data['首次还款金额'] = np.nan
    data['首次还款日期'] = np.nan
    data['签约资料欠缺情况'] = '齐'
    data['贷款资料欠缺情况'] = "齐"
    x = ['城市','项目', '楼栋', '房号', '合并房号', '业主姓名', '按揭金额', '银行', '签约资料欠缺情况', '贷款资料欠缺情况', '放款日期', '客户领走合同日期', '首次还款金额', '首次还款日期']
    data = data[x]
    writer = pd.ExcelWriter(result_dir + r"\temp\长沙签约数据.xlsx")
    data.to_excel(writer,sheet_name="长沙签约数据",index=False)
    writer.save()


def trace_qy_control(df_mx,df_lp,date,save_dir):
    fields = ['房间编码',"项目", "合并房号", "姓名", "买卖合同总价", "首付金额", "类型", "付款方式", "贷款金额", "认购日期", "签约日期", "网签日期", "网签签字日期",
              "客户契税/维修基金", "银行备案资料", "送备案时间", "放款时间",'备注'
    ]
    data_mx = df_mx[fields]
    data_lp = df_lp[['房间编码','欠款']]

    data = pd.merge(data_mx,data_lp,how='left',on="房间编码")
    data_cw = data[data['类型']== "车位"]
    data_sz = data[data['类型']!="车位"]
    data_cw = get_interval_data(data_cw,'签约日期',date,pd.to_datetime("2018-12-1"))
    data_sz = get_interval_data(data_sz,'网签日期',date,pd.to_datetime("2018-12-1"))
    data = pd.concat([data_cw,data_sz])
    # data['欠款'] = data['欠款'].fillna(0)
    for i in range(1,9):
        data["s" + str(i)] = np.nan
    fields_new = ["项目", "合并房号", "姓名", "买卖合同总价", "首付金额", "类型", "付款方式", "贷款金额", "欠款", "s1", "s2", "认购日期", "签约日期", "网签日期", "s3", "s4", "网签签字日期", "客户契税/维修基金",
                 "s5", "s6", "s7", "银行备案资料", "送备案时间", "放款时间", "s8", "备注"]
    writer = pd.ExcelWriter(save_dir + r"\data\签约总控表_data.xlsx")
    data[fields_new].sort_values(['项目','类型']).to_excel(writer,sheet_name="data",index=False)
    writer.save()


def trace_zqy(df_mx,df_lp,date,result_dir,delay_day=7):
    df_mxlp = mx_lp_merge(df_mx,df_lp)
    
    #认购未签
    lp_l = ['房间编码','项目名称','相关房源',  '客户名称','认购时间','转签约日期','销售状态','产品类型','成交总价','业务员','营销楼栋名称','楼盘_合并房号','实收','欠款']
    rg_wq_fields = [ '业务员','项目名称', '产品类型', '楼盘_合并房号',  '客户名称','成交总价','实收','欠款','认购时间','房间编码', '相关房源']
    df_rgwq = rg_no_qy(df_mx,df_lp,date,is_agg=False)[lp_l]

    df_rgwq = df_rgwq[rg_wq_fields]
    df_rgwq.sort_values(['业务员','项目名称'],inplace=True)
    df_rgwq = add_order(df_rgwq)
    
    # 待网签
    wait_wq_fields = ['项目','类型',"楼盘_合并房号",'姓名','签约日期',"买卖合同总价", '首付金额', '付款方式', '贷款金额','欠款','置业顾问',"业务员",'认购时间',"房间编码"]
    df_wait_wq = cq_wait_wq(df_mxlp,date,is_agg=False)[wait_wq_fields]
    df_wait_wq = add_order(df_wait_wq)
    
    """逾期 和 待签"""

    now = datetime.datetime.now()
    df_qywait_sys = qy_wait_sys(df_mxlp,date,is_agg=False)
    df_qywait_sys['逾期时间'] = df_qywait_sys['认购时间'] + datetime.timedelta(delay_day)
    df_qywait_sys['逾期时差'] = (df_qywait_sys['逾期时间'] - now).dt.total_seconds()//(3600*24)
    qy_wait_fields = ['项目','类型',"楼盘_合并房号",'姓名','签约日期',"买卖合同总价", '首付金额', '付款方式', '贷款金额','欠款','置业顾问',"业务员",'逾期时差','认购时间',"逾期时间","转签约异常","房间编码",]
    df_qywait_sys = df_qywait_sys[qy_wait_fields]
    
    # 逾期
    df_yq = df_qywait_sys[(df_qywait_sys['逾期时间'] <= now) | (df_qywait_sys['转签约异常']!="")]
    df_yq['逾期时差'] = - df_yq['逾期时差']
    df_yq.rename(columns={"逾期时差":"逾期(天)"},inplace=True)
    df_yq = add_order(df_yq)

    # 已签未认购
    df_qy_no_rg = df_qywait_sys[df_qywait_sys['认购时间'].isnull()]
    del df_qy_no_rg['逾期时差']
    del df_qy_no_rg['转签约异常']
    del df_qy_no_rg['逾期时间']
    del df_qy_no_rg['认购时间']
    df_qy_no_rg = add_order(df_qy_no_rg)

    # 已签待转
    df_yqdz = df_qywait_sys[(df_qywait_sys['逾期时间'] > now) & (df_qywait_sys['转签约异常']=="")]
    df_yqdz.rename(columns={"逾期时差":"距逾期(天)"},inplace=True)
    df_yqdz.sort_values(['项目','距逾期(天)'],inplace=True)
    df_yqdz = add_order(df_yqdz)
    del df_yqdz['转签约异常']
    
    # writer = pd.ExcelWriter(result_dir + r"\签约明细\河西未转签约明细表%s.xlsx" %(date.strftime("%Y-%m-%d")))

    writer = pd.ExcelWriter(result_dir + r"\temp\河西未转签约明细.xlsx" )

    df_rgwq.to_excel(writer,'认购未签',index=False)
    if len(df_qy_no_rg):
        df_qy_no_rg.to_excel(writer,'已签未认购',index=False)

    if len(df_wait_wq):
        df_wait_wq.to_excel(writer,'待网签',index=False)
    if len(df_yqdz):
        df_yqdz.to_excel(writer,'已签待转',index=False)
    df_yq.to_excel(writer,'已逾期',index=False)
    writer.save()
    
# 备案跟踪    
def trace_record(df_mx,save_dir):
    fields = ['项目','类型','合并房号','姓名','买卖合同总价','付款方式','备案_业务宗号','送备案时间','出证日期','登记证号','移交状态','移交日期','房间编码']
    data = df_mx[fields]
    data = data[data['送备案时间'].notnull()]
    data.rename(columns={"备案_业务宗号":"业务宗号","买卖合同总价":"房款",'合并房号':"房号","送备案时间":"进窗时间"},inplace=True)
    data.sort_values(['项目','房号'],inplace=True)
    data = add_order(data)
    writer = pd.ExcelWriter(save_dir + r"\temp\河西已进窗领证总表.xlsx")
    data.to_excel(writer,sheet_name="进窗领证",index=False)
    writer.save()

# 未备案跟踪 
def trace_no_record(df_mx,save_dir):
    fields = ['项目','类型', "合并房号","签约日期", "姓名", "建筑面积", "首付金额", "付款方式", "贷款金额",
          "置业顾问", "客户契税/维修基金", "银行备案资料", "备注",'房间编码','送备案时间']
    data = df_mx[df_mx['类型']!="车位"][fields]
    data = data[data['送备案时间'].isnull()].sort_values(['项目','类型'])
    data = add_order(data)
    data.rename(columns={"合并房号":"房号","建筑面积":"面积","银行备案资料":"银行资料"},inplace=True)
    del data['送备案时间']
    writer = pd.ExcelWriter(save_dir + r"\temp\河西未备案表.xlsx")
    data.to_excel(writer,sheet_name="未备案",index=False)
    writer.save()
    
def trace_vanke_pro(df_mx,save_dir):
    data_fields = ['类型','房间编码','签约日期','网签日期','项目','合并房号','姓名','建筑面积','买卖合同总价','付款方式','联系电话','送备案时间'
            ,'放款时间','备案完成时间'
            ]
    data = df_mx[data_fields]
    data.rename(columns={"送备案时间":"备案送件日期","放款时间":"全款到账日期","备案完成时间":"预告出件日期"},inplace=True)

    data_r = data[data['类型']=="住宅"]
    data_zz = add_order(data_r)

    data_r = data[data['类型']=="商业"]
    data_sy = add_order(data_r)

    writer = pd.ExcelWriter(save_dir+r"\temp\长沙万科项目.xlsx")
    data_zz.to_excel(writer,sheet_name="住宅",index=False)
    data_sy.to_excel(writer,sheet_name="商业",index=False)
    writer.save()

def mx_map_lp(df_mx,df_lp,save_dir):
    mx_fileds = ['房间编码','付款方式','姓名','转签约日期','合并房号','联系地址' ,'项目','类型','签约日期','key','key_r']
    df_mx = df_mx[mx_fileds]
    df_mx.rename(columns={"房间编码":'明细_房间编码'},inplace=True)

    
    cwlp_fields = [ '房间编码', '项目名称','分期名称', '产品类型全称', '营销楼栋名称','房号','客户名称','相关房源','lp_key']
    df_lp = df_lp[cwlp_fields]
    df_lp.rename(columns={"房间编码":'楼盘_房间编码'},inplace=True)

    
    # 明细原始key 不唯一部分
    mx_not_unique = not_unique_part(df_mx,'key')
    mx_not_unique = mx_not_unique.sort_values("key")
    
    
    # 明细转换key_r 部位一部分
    mx_not_unique_r = not_unique_part(df_mx,'key_r')
    mx_not_unique_r = mx_not_unique.sort_values("key_r")
    
    # 财务楼盘重复部分
    cwlp_not_unique = not_unique_part(df_lp,'lp_key')
    cwlp_not_unique = cwlp_not_unique[cwlp_not_unique['客户名称']!=""].sort_values("lp_key")

    # 楼盘唯一部分
    cwlp_unique = unique_part(df_lp,"lp_key")

    # 明细表唯一部分
    mx_unique = unique_part(df_mx,"key")
    mx_unique = unique_part(mx_unique,"key_r")

    df_mxlp = pd.merge(mx_unique,cwlp_unique,left_on="key_r",right_on="lp_key",how="left")

     # roomcodemap
    mx_match = ['项目','类型','姓名','楼盘_房间编码','key_r','key','lp_key']
    df_match = df_mxlp[df_mxlp['房号'].notnull()][mx_match].reset_index(drop=True)


    # key_r 无法与 lp_key 匹配 房间编码 也不属于 楼盘子集
    mx_not_match = ['项目','签约日期','类型','姓名','明细_房间编码','key_r','key']
    df_not_match = df_mxlp[df_mxlp['房号'].isna()][mx_not_match]
    not_match_part = set(df_not_match['明细_房间编码']) - (set(df_lp['楼盘_房间编码'])-set(df_match['楼盘_房间编码']))
    df_not_match = df_not_match[df_not_match['明细_房间编码'].isin(not_match_part)].reset_index(drop=True)

   
    
    
    writer = pd.ExcelWriter(save_dir + r'\mx_vs_lp.xlsx')

    df_match.to_excel(writer,sheet_name='roomcodemap',index=False)

    df_not_match.to_excel(writer,sheet_name='mx_not_match_lp',index=False)

    if len(mx_not_unique):
        mx_not_unique.to_excel(writer,sheet_name='key重复',index=False)
    if len(mx_not_unique_r):
        mx_not_unique_r.to_excel(writer,sheet_name='key_r重复',index=False) 

    if len(cwlp_not_unique):
        cwlp_not_unique.to_excel(writer,sheet_name='lp_key重复',index=False)


    writer.save()
    