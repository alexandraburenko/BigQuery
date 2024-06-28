
/*Calculation of Conversions
Conversion Metrics:
user_sessions_count: The number of unique sessions.
visit_to_cart: Conversion rate from session start to adding a product to the cart.
visit_to_checkout: Conversion rate from session start to beginning checkout.
Visit_to_purchase: Conversion rate from session start to purchase.
*/
with cte as (
select
timestamp_micros(event_timestamp) as event_timestamp,
 CONCAT(user_pseudo_id,cast((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as string)) as user_session_id,
  event_name,
  geo.country as country,
  device.category as device,
  traffic_source.medium as medium,
  traffic_source.source as source,
  traffic_source.name as campaign
 FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
where _table_suffix between '20210101'and '20211231'
AND event_name in ('session_start','view_item','add_to_cart', 'begin_checkout','add_shipping_info','add_payment_info','purchase')
)
select
  date(event_timestamp) as event_date,
  source,
  medium,
  campaign,
  COUNT( distinct user_session_id) AS user_sessions_count,
  round(count(distinct case when event_name ='add_to_cart' then user_session_id end)/
  cast(COUNT( distinct user_session_id) as numeric)*100,2) as  visit_to_cart,
  round(count(distinct case when event_name ='begin_checkout' then user_session_id end)/
  cast(COUNT( distinct user_session_id) as numeric)*100, 2 ) as visit_to_checkout,
  round(count(distinct case when event_name ='purchase' then user_session_id end) /
  cast(COUNT( distinct user_session_id) as numeric)*100, 2)  as Visit_to_purchase
From cte e
group by 1,2,3,4

;

/*Comparison of Conversion Between Different Landing Pages
 Conversion Metrics:
page_path: The landing page path.
sessions_count: The number of unique sessions for each landing page.
purchase_count: The number of purchases for each landing page.
Visit_to_purchase: Conversion rate from session start to purchase for each landing page.
*/

with session_start_path as (
select
  concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string)) as user_session_id,
  regexp_extract ((select value.string_value FROM UNNEST(event_params) where key = 'page_location'), r'(?:\w+\:\/\/)?[^\/]+\/([^\?#]*)') as page_path
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
where _table_suffix between '20200101'and '20201231'
and event_name ='session_start'
),
purchase as (
select
  CONCAT(user_pseudo_id,cast((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as string)) as user_session_id
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
where _table_suffix between '20200101'and '20201231'
and event_name ='purchase'
)

select 
  page_path,
  COUNT( distinct s.user_session_id) as sessions_count,
  count(distinct p.user_session_id) as purchase_count,
  round(count(distinct p.user_session_id) /cast(COUNT( distinct s.user_session_id) as numeric)*100,2)  as Visit_to_purchase
from session_start_path s 
left join purchase p on s.user_session_id=p.user_session_id
group by 1

;

/*Checking the Correlation Between User Engagement and Purchases
Correlation Coefficients:
engagement_time_to_purchase_cor: Correlation between total engagement time and purchase.
session_engaged_to_purchased_cor: Correlation between session engagement and purchase.
*/
with session as (
select
  concat(user_pseudo_id, cast((select value.int_value from unnest(event_params) where key = 'ga_session_id') as string)) as user_session_id,
  sum(coalesce((SELECT value.int_value from unnest(event_params) where key =  'engagement_time_msec'), 0)) as ttl_engagement_time_msec,
   CASE WHEN(coalesce(safe_cast((SELECT value.string_value from unnest(event_params) WHERE key =  'session_engaged')as integer),
   (SELECT value.int_value from unnest(event_params) WHERE key =  'session_engaged'), 0))> 0 THEN 1 ELSE 0 END as is_session_engaged
from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
group by 1,3
),
purchase as (
select
  CONCAT(user_pseudo_id,cast((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as string)) as user_session_id
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` 
where event_name ='purchase'
)
 
 select
  corr(s.ttl_engagement_time_msec, case when p.user_session_id is not null then 1 else 0 end) as engagement_time_to_purchase_cor,
  CORR(s.is_session_engaged, case when p.user_session_id is not null then 1 else 0 end) as session_engaged_to_purchased_cor
  from session s
  left join purchase p on s.user_session_id=p.user_session_id


/*Correlation Analysis Results
Correlation Between Total Engagement Time and Purchase (engagement_time_to_purchase_cor):

Correlation Coefficient: 0.24985132594815429

Description: This value represents a moderate positive correlation between the total engagement time during a session 
and the likelihood of a purchase occurring in that session.

 A correlation coefficient of approximately 0.25 suggests that there is a moderate tendency for users who spend more time engaging with your site 
 to complete a purchase. While this relationship is not strong, it indicates that increasing user engagement time could have a beneficial impact 
 on purchase rates. 
 
 Since there is a moderate positive correlation between engagement time and purchases, focusing on strategies 
 to increase the time users spend on your site could be beneficial. This might include improving content quality, enhancing user experience, 
 and offering engaging features that encourage longer visits.
 
Correlation Between Session Engagement and Purchase (session_engaged_to_purchased_cor):

Correlation Coefficient: 0.0078780430449121863

Description: This value indicates an extremely weak positive correlation between whether a session is classified as engaged 
and the likelihood of a purchase occurring in that session.

A correlation coefficient of approximately 0.008 suggests that the binary classification of a session as "engaged" has almost no impact
 on the likelihood of a purchase. This implies that the criteria used to define an "engaged" session might not be strongly linked to purchase behavior.

  
  
  
  
  
  
  
