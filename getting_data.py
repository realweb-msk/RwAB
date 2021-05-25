#from google.cloud import bigquery
import pandas as pd

#source - принимает 2 значения: GA или OWOX. fullTableName -полное название таблички с данными 
#start/end - начало/конец периода в формате yyyymmdd. *condiitons - условия для выгрузки данных

def query(source,fullTableName,start,end,*conditions):
    cond=conditions.split(",")
    client = bigquery.Client()
    if (cond==''):
        my_string='1=1'
    else: 
        my_string=' AND '.join(cond)
    if (source=="GA"):
        query='''with meow as (
        select visitid 
        ,max(hits.time)/1000 as duration 
        ,max(totals.pageviews) as pageviews 
        ,max(totals.bounces) as bounce 
        ,max(totals.totaltransactionrevenue)/1000000 as revenue 
        ,max(totals.transactions) as cnt_transactions 
        from `{}.ga_sessions_*` 
        ,unnest(hits) as hits 
        where 1=1 
        and _table_suffix between '{}' and '{}' 
        group by visitid) 
        select 
        fullvisitorid 
        ,a.visitid 
        ,totals.newvisits 
        ,date 
        ,b.duration 
        ,b.pageviews 
        ,b.bounce 
        ,b.revenue 
        ,b.cnt_transactions 
        ,device.devicecategory 
        ,device.operatingsystem 
        ,device.operatingsystemversion 
        ,geonetwork.country 
        ,geonetwork.region 
        ,trafficsource.campaign 
        ,trafficsource.campaigncode 
        ,trafficsource.source 
        ,trafficsource.medium 
        ,channelgrouping 
        ,hits.type 
        ,hits.eventinfo.eventcategory 
        ,hits.eventinfo.eventaction 
        ,hits.eventinfo.eventlabel 
        ,hits.eventinfo.eventvalue 
        ,hits.referer 
        ,hits.page.pagepath 
        ,customdimensions.index 
        ,customdimensions.value 
        ,hits.experiment.experimentid 
        ,hits.experiment.experimentvariant 
        /*,promotion.promocreative 
        ,promotion.promoid 
        ,promotion.promoname 
        ,promotion.promoposition 
        ,promotionactioninfo.promoisview 
        ,promotionactioninfo.promoisclick*/ 
        from `{}.ga_sessions_*` a 
        ,unnest(hits) as hits 
        ,unnest(hits.customdimensions) as customdimensions 
        left join meow b on a.visitid=b.visitid 
        /*,unnest(hits.promotion) as promotion 
        ,unnest(hits.promotionactioninfo) as promotionactioninfo*/ 
        where 1=1 
        and _table_suffix between '{}' and '{}' 
        AND {}'''.format(fullTableName,start,end,fullTableName,start,end,my_string)
        #results = sql.result()
    elif (source=="OWOX"):
        query='''with meow as (
        select sessionid 
        ,max(hits.time)/1000 as duration 
        ,max(totals.pageviews) as pageviews 
        ,sum(transaction.transactionrevenue) as revenue 
        ,max(totals.transactions) as cnt_transactions 
        ,max(totals.events) as events 
        from `{}.owoxbi_sessions_*` 
        ,unnest(hits) as hits 
        where 1=1 
        and _table_suffix between '{}' and '{}' 
        group by sessionid) 
        select 
        clientid 
        ,a.sessionid 
        ,newvisits 
        ,date 
        ,b.duration 
        ,b.pageviews 
        ,case when b.events<=1 then 1 else 0 end as bounce 
        ,b.revenue 
        ,b.cnt_transactions 
        ,a.device.devicecategory 
        ,a.device.operatingsystem 
        ,a.device.operatingsystemversion 
        ,geonetwork.country 
        ,geonetwork.region 
        ,trafficsource.campaign 
        ,trafficsource.source 
        ,trafficsource.medium 
        ,trafficsource.channelgrouping 
        ,hits.type 
        ,hits.pagepath 
        ,landingpage 
        ,hits.eventinfo.eventcategory 
        ,hits.eventinfo.eventaction 
        ,hits.eventinfo.eventlabel 
        ,hits.eventinfo.eventvalue 
        ,hits.referer 
        ,customdimensions.index 
        ,customdimensions.value 
        ,experiment.experimentid 
        ,experiment.experimentvariant 
        /*,hits.promotion.promocreative 
        ,hits.promotion.promoid 
        ,hits.promotion.promoname 
        ,hits.promotion.promoposition 
        ,hits.promotionactioninfo*/ 
        from `{}.owoxbi_sessions_*` a 
        left join meow b on a.sessionid=b.sessionid 
        ,unnest(hits) as hits 
        ,unnest(hits.customdimensions) as customdimensions 
        where 1=1 
        and _table_suffix between '{}' and '{}' 
        AND {}'''.format(fullTableName,start,end,fullTableName,start,end,my_string)
        #results = sql.result()
    else:
        print("Invalid source")



def access (way_to_json, project_id, query=query):
    credentials = service_account.Credentials.from_service_account_file(way_to_json)
    df = pd.read_gbq(query, project_id=project_id, credentials=credentials, dialect='standard')

#df- табличка с данными; fn - метрика для группера; col_name - колонка для метрики; *groups - перечисление столбцов для group by
def grouper(df,*groups,**funs):
    a=[*groups]
    out = {}
    for k, v in funs.items():
        if isinstance(v, list):
            for x in v:
                out[x] = k
        else:
            out[v] = k
    data_total = df.groupby(a, as_index=False).agg(out)
    print(data_total)
