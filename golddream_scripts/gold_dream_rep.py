import numpy as np
import pandas as pd
import os
import datetime
import warnings
import time
# 遍历所有文件

warnings.filterwarnings('ignore') 

def single_hz(data,title,index_l=None,index_name="项目",agg_name="买卖合同总价",is_sum=True,is_ten_thousand=True,count_field="套数",sum_field="金额(万元)"):
    data = data.groupby(index_name).agg({agg_name:{count_field:'count',sum_field:'sum'}})

    data.columns= pd.MultiIndex.from_product( [[title],[count_field,sum_field]])

    data.index.name = index_name

    if index_l:
        if len(data) == 0:
            data.loc[index_l[0]] = 0
        data = data.loc[index_l].fillna(0)
    
    if is_ten_thousand:
        for lv1_col in data.columns.levels[0]:
            data[lv1_col,data.columns.levels[1][1]] = np.around(data[lv1_col,data.columns.levels[1][1]]/10000)
    if is_sum:
        data.loc['总和'] = data.apply(lambda x:sum(x))
    return data


def get_ld_info_hz(data,ld_name):
    data_temp = data
    l = []

    data = data_temp
    data =  single_hz(data,ld_name,index_name="契税缴纳",sum_field="金额",is_ten_thousand=False,
                      is_sum=False,index_l=['已交契税','未交契税']).T
   
    l.append(data)

    data = data_temp
    data =  single_hz(data,ld_name,index_name="网签签字",sum_field="金额",is_ten_thousand=False,
                      is_sum=False,index_l=['网签签字','网签未签字']).T
    l.append(data)
    
    data = data_temp
    data =  single_hz(data,ld_name,index_name="备案状态",sum_field="金额",is_ten_thousand=False,
                      is_sum=False,index_l=['已备案','未备案']).T
    l.append(data)

    data = pd.concat(l,axis=1)
    data.index = pd.MultiIndex.from_product( [[ld_name],['套数','金额']])

    return data


def gd_ld_trace(data,writer,ld_l):

    df_gd_r  = data[['楼栋','买卖合同总价','客户契税/维修基金','送备案时间','网签签字日期','备案异常情况','类型', '网签日期']]
    df_gd_r.dropna(subset=['楼栋'],inplace=True)
    df_gd_r['楼栋'] = df_gd_r['楼栋'].astype(int).astype(str).str.strip()
    df_gd_r = df_gd_r[df_gd_r['类型'] != '车位']


    df_gd_r['备案状态'] = np.nan

    df_gd_r['备案状态'][df_gd_r['送备案时间'].isnull()] = "未备案"

    df_gd_r['备案状态'][df_gd_r['送备案时间'].notnull()] = "已备案"

    df_gd_r['契税缴纳'] = np.nan
    df_gd_r['契税缴纳'][df_gd_r['客户契税/维修基金'].notnull()] = "已交契税"

    df_gd_r['契税缴纳'][df_gd_r['客户契税/维修基金'].isnull()] = "未交契税"


    df_gd_r['网签签字'] = np.nan
    df_gd_r['网签签字'][df_gd_r['网签签字日期'].notnull()] = "网签签字"

    df_gd_r['网签签字'][df_gd_r['备案异常情况'].fillna("").astype(str).str.contains("^未签字")] = "网签未签字"
    start_row = 0
    for ld_name,ld_num_s in ld_l:
        if not isinstance(ld_num_s,(tuple,list)):
            ld_num_s = [ld_num_s]


        data_temp = get_ld_info_hz(df_gd_r[df_gd_r['楼栋'].isin(ld_num_s)],ld_name)
        #     data_temp = fields
        data_temp.to_excel(writer,sheet_name="楼栋信息",startrow=start_row)
        start_row = start_row + 4
    return writer
    




def pro_real_hz(data,writer,sheet_name):
    data_hz_l = []
    type_l = ['住宅','商业','车位']
    
    # 签约汇总
    data_qy = data[['买卖合同总价','类型']]
    data_qy_hz = single_hz(data_qy,'签约汇总',index_name="类型",index_l = type_l,agg_name="买卖合同总价",is_ten_thousand=False,sum_field="合同金额(元)")
    data_hz_l.append(data_qy_hz)
    
    
    
    # 按揭汇总

    data_aj_ht = data[['付款方式','买卖合同总价','类型']]
    data_aj_ht = data_aj_ht[(data_aj_ht['付款方式'] != '一次性') & (data_aj_ht['付款方式'] != '一次性分期')]
    data_aj_ht_hz = single_hz(data_aj_ht,'按揭汇总',index_name="类型",index_l = type_l,agg_name="买卖合同总价",is_ten_thousand=False,sum_field="合同金额(元)")
    

    data_aj_loan = data[['付款方式','贷款金额','类型']]
    data_aj_loan = data_aj_loan[(data_aj_loan['付款方式'] != '一次性') & (data_aj_loan['付款方式'] != '一次性分期')]
    data_aj_loan_hz = single_hz(data_aj_loan,'按揭汇总',index_name="类型",index_l = type_l,agg_name="贷款金额",
                                is_ten_thousand=False,sum_field="贷款金额(元)")
    data_aj_loan_hz = data_aj_loan_hz['按揭汇总',"贷款金额(元)"].to_frame()
    
    data_aj_hz = pd.concat([data_aj_ht_hz,data_aj_loan_hz['按揭汇总',"贷款金额(元)"].to_frame()],axis=1)
    data_hz_l.append(data_aj_hz)
   
    
    # 一次性汇总

    data_allin = data[['付款方式','买卖合同总价','类型']]
    data_allin = data_allin[data_allin['付款方式'].fillna("").astype(str).str.contains("^一次性")]
    data_allin_hz = single_hz(data_allin,'一次性汇总',index_name="类型",index_l = type_l,agg_name="买卖合同总价",is_ten_thousand=False,sum_field="合同金额(元)")
    data_hz_l.append(data_allin_hz)

    # 网签汇总
    data_wq = data[['网签日期','买卖合同总价','类型']]
    data_wq = data_wq[data_wq['网签日期'].notnull()]
    data_wq_hz = single_hz(data_wq,'网签汇总',index_name="类型",index_l = type_l,agg_name="买卖合同总价",is_ten_thousand=False,sum_field="合同金额(元)")
    data_hz_l.append(data_wq_hz)
    
    data_hz  = pd.concat(data_hz_l,axis=1)
    
    data_aj = data[['付款方式','贷款金额']]
    data_aj = data_aj[(data_aj['付款方式'] != '一次性') & (data_aj['付款方式'] != '一次性分期')]
    data_aj_fun_hz = single_hz(data_aj,'按揭统计',index_name="付款方式",agg_name="贷款金额",is_ten_thousand=False,sum_field="贷款金额(元)")
    aj_total = data_aj_fun_hz.loc['总和']['按揭统计','贷款金额(元)']
    data_aj_fun_hz['按揭统计','金额占比']  = data_aj_fun_hz['按揭统计','贷款金额(元)'] / aj_total
    
    data_aj_fun_hz.to_excel(writer,sheet_name=sheet_name,startcol=0)
    data_hz.to_excel(writer,sheet_name=sheet_name,startcol=5)
    return writer











