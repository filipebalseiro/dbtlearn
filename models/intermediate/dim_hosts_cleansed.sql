{{ config(materialized="view") }}
with stg_hosts as (select * from {{ ref("stg_hosts") }})
select
    host_id,
    nvl(host_name, 'Anonymous') as host_name,
    is_superhost,
    created_at,
    updated_at
from stg_hosts
