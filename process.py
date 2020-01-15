
#去掉数据集中的重复u,i对
#data:datafram
def drop_dup(data,userkey,itemkey):
    return data.drop_duplicates([userkey,itemkey])


#过滤数据，保留交互数据大于等于minl的user,item
def filter_n(data,minl,userkey,itemkey):
    flag=True
    while flag:
        print('filter start')
        fil_u=[]
        u_group=data.groupby(by=userkey)
        for u,his in u_group:
            if len(his)<minl:
                fil_u.extend(list(his.index))
        data.drop(fil_u,inplace=True)

        fil_i=[]
        i_group=data.groupby(by=itemkey)
        for i,his in i_group:
            if len(his)<minl:
                fil_i.extend(list(his.index))
        data.drop(fil_i,inplace=True)
        if len(fil_u)==0 and len(fil_i)==0:
            break
    return data

#将id值映射到从0开始
def remap(data,key):
    u_key = sorted(data[key].unique().tolist())
    u_len = len(u_key)
    u_map = dict(zip(u_key, range(u_len)))
    data[key] = data[key].map(lambda x: u_map[x])
    return data

#按[uid,time]排序
def sortV(data,userkey,timekey):
    data.sort_values(by=[userkey, timekey], inplace=True)
    return data

#写入csv文件
def writeCSV(data,file):
    data.to_csv(file, index=None, sep='\t', header=None)