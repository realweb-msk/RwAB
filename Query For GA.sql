SELECT
--инфа о сессии/клиенте
fullVisitorId
,visitId
,totals.newVisits       --1 when new
,date


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


--промо
/*,promotion.promoCreative
,promotion.promoId
,promotion.promoName
,promotion.promoPosition
,promotionActionInfo.promoIsView
,promotionActionInfo.promoIsClick*/

FROM 'project_name:dataset_name.ga_sessions_*'
,UNNEST(hits) AS hits
,UNNEST(hits.customDimensions) AS customDimensions
/*,UNNEST(hits.promotion) as promotion
  ,UNNEST(hits.promotionActionInfo) as promotionActionInfo*/


WHERE 1=1
AND _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
