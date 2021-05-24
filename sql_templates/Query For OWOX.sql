WITH meow AS
(SELECT sessionId
,MAX(hits.time)/1000 AS duration
,MAX(totals.pageviews) AS pageviews
,SUM(transaction.transactionRevenue) AS revenue
,MAX(totals.transactions) AS cnt_transactions
,MAX(totals.events) as events
FROM `project_name.dataset_name.ga_sessions_*`
,UNNEST(hits) AS hits
WHERE 1=1
AND _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
GROUP BY sessionId)


	
SELECT

--инфа о сессии/клиенте
clientId
,a.sessionId
,newVisits     --1 when new
,date
,b.duration
,b.pageviews
,CASE WHEN b.events<=1 THEN 1
ELSE 0 END as bounce
,b.revenue
,b.cnt_transactions


--устройство
,a.device.deviceCategory
,a.device.operatingSystem
,a.device.operatingSystemVersion


--георграфия
,geoNetwork.country
,geoNetwork.region


--источник
,trafficSource.campaign
--,trafficSource.campaignCode
,trafficSource.source
,trafficSource.medium
,trafficSource.channelGrouping



--события
,hits.type
,hits.pagePath
,landingPage
,hits.eventInfo.eventCategory
,hits.eventInfo.eventAction
,hits.eventInfo.eventLabel
,hits.eventInfo.eventValue	
,hits.referer
,customDimensions.index
,customDimensions.value


--эксперимент
/*,experiment.experimentId
,experiment.experimentVariant*/

--промо
/*,hits.promotion.promoCreative
,hits.promotion.promoId
,hits.promotion.promoName
,hits.promotion.promoPosition
,hits.promotionActionInfo*/

FROM `project_name:dataset_name.owoxbi_sessions_*` a
LEFT JOIN meow b ON a.sessionId=b.sessionId
,UNNEST(hits) AS hits
,UNNEST(hits.customDimensions) as customDimensions
WHERE 1=1
AND _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'


