__all__ = ["get_check","get_7days","not_unique_part","unique_part","get_types","single_hz","clear_folder"]


import pandas as pd
import datetime
import numpy as np
import os

def clear_folder(target_dir,is_recursion=True):
    for file in os.listdir(target_dir):
        path = os.path.join(target_dir,file)
        if os.path.isfile(path):
            os.remove(path)
        elif is_recursion and os.path.isdir(path):
            try:
                os.rmdir(path)
            except OSError:
                clear_folder(path)
                os.rmdir(path)

def single_hz(data,title,index_l=None,index_name="项目",agg_name="买卖合同总价",is_sum=True):
    data = data.groupby(index_name).agg({agg_name:{"套数":'count',"金额(万元)":'sum'}})
    data.columns= pd.MultiIndex.from_product( [[title],['套数','金额(万元)']])
    data.index.name = index_name

    if index_l:
        if len(data) == 0:
            data.loc[index_l[0]] = 0
        data = data.loc[index_l].fillna(0)
    

    for lv1_col in data.columns.levels[0]:
        data[lv1_col,data.columns.levels[1][1]] = np.around(data[lv1_col,data.columns.levels[1][1]]/10000)
    if is_sum:
    	data.loc['总和'] = data.apply(lambda x:sum(x))
    return data

_hz = single_hz
   
def get_interval_data(df_lp,field_name,target_date,start_date=None):
    start_date = start_date or target_date 
    data = df_lp[(df_lp[field_name] <= target_date ) &
                (df_lp[field_name] >= start_date)
                ]
    
    return data

def clear_type(data,col_name,col_type):
    data = data[data[col_name].apply(lambda x:type(x) != col_type)]
    return data

def get_types(data,field):

    return data[field].apply(lambda x:type(x)).unique()

def get_check(data,field,field_type):
    fields = [field]
    data = data[fields]
    print(data[data[field].apply(lambda x:type(x)==field_type)].groupby(['项目','类型']).count())
    return data[data[field].apply(lambda x:type(x)==field_type)]


def add_order(data):
    data = data.reset_index(drop=True)
    data.index.name = "序号"
    data = data.reset_index()
    data['序号']  = data['序号'] + 1
    return data




# 给出日期属于 第几个 七天   最多 4个 过 4 为 4
def get_7days(date):
    zs = date.day//7
    ys = date.day % 7

    if ys == 0:zs = zs - 1
    if zs > 3:zs = 3

    start_day = zs * 7 + 1
    end_day =  (zs + 1) * 7 if zs < 3 else date.daysinmonth
    return start_day,end_day

def not_unique_part(data,key):
    x = data[key].value_counts()
    return data[data[key].isin(x[x>1].index)].sort_values(key).reset_index(drop=True)

def unique_part(data,key):
    x = data[key].value_counts()
    return data[data[key].isin(x[x==1].index)].sort_values(key).reset_index(drop=True)