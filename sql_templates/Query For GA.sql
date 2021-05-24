WITH meow AS
(SELECT visitId
,MAX(hits.time)/1000 AS duration
,MAX(totals.pageviews) AS pageviews
,MAX(totals.bounces	) AS bounce
,MAX(totals.totalTransactionRevenue	)/1000000 AS revenue
,MAX(totals.transactions) AS cnt_transactions
FROM `project_name.dataset_name.ga_sessions_*`
,UNNEST(hits) AS hits
WHERE 1=1
AND _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
GROUP BY visitId)






SELECT
--инфа о сессии/клиенте
fullVisitorId
,a.visitId
,totals.newVisits       --1 when new
,date
,b.duration
,b.pageviews
,b.bounce
,b.revenue
,b.cnt_transactions

--устройство
,device.deviceCategory
,device.operatingSystem
,device.operatingSystemVersion


--георрафия
,geoNetwork.country
,geoNetwork.region


--источник
,trafficSource.campaign
,trafficSource.campaignCode
,trafficSource.source
,trafficSource.medium
,channelGrouping


--события
,hits.type
,hits.eventInfo.eventCategory
,hits.eventInfo.eventAction
,hits.eventInfo.eventLabel
,hits.eventInfo.eventValue
,hits.referer
,hits.page.pagePath
,customDimensions.index
,customDimensions.value

--эксперимент
/*,hits.experiment.experimentId
,hits.experiment.experimentVariant*/


--промо
/*,promotion.promoCreative
,promotion.promoId
,promotion.promoName
,promotion.promoPosition
,promotionActionInfo.promoIsView
,promotionActionInfo.promoIsClick*/

FROM `project_name.dataset_name.ga_sessions_*` a
,UNNEST(hits) AS hits
,UNNEST(hits.customDimensions) AS customDimensions
LEFT JOIN meow b ON a.visitId=b.visitId
/*,UNNEST(hits.promotion) as promotion
  ,UNNEST(hits.promotionActionInfo) as promotionActionInfo*/


WHERE 1=1
AND _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
