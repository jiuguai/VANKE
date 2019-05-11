import pandas as pd
def mx_lp_merge(df_mx,df_lp):
    mx_l = ['房间编码','项目','类型', "姓名","置业顾问",'签约日期','网签日期','买卖合同总价', '付款方式', '贷款金额', '首付金额', '贷款银行',"转签约异常"]
    lp_l = ['房间编码','项目名称',"分期名称","楼盘_合并房号","业务员",'认购日期','认购时间','转签约日期','销售状态','产品类型全称','成交总价','营销楼栋名称','房号','实收','欠款']
    df_lp_r = df_lp[lp_l]
    df_mx_r = df_mx[mx_l]
    df_mxlp = pd.merge(df_lp_r,df_mx_r,how='right',on="房间编码")
    df_mxlp.dropna(subset=["房间编码"],inplace=True)
    return df_mxlp.reset_index(drop=True)