SELECT

--инфа о сессии/клиенте
clientId
,sessionId
newVisits     --1 when new
,date


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


--промо
/*,hits.promotion.promoCreative
,hits.promotion.promoId
,hits.promotion.promoName
,hits.promotion.promoPosition
,hits.promotionActionInfo*/

FROM 'project_name:dataset_name.owoxbi_sessions_*' a
,UNNEST(hits) AS hits
,UNNEST(hits.customDimensions) as customDimensions
WHERE 1=1
AND _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'

