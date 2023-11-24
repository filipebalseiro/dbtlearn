{{ config(materialized="incremental", on_schema_change="fail") }}
with stg_reviews as (select * from {{ ref("stg_reviews") }})
select
    {{ dbt_utils.generate_surrogate_key(['listing_id', 'review_date','reviewer_name', 'review_text', 'review_sentiment']) }} AS review_id,
    *
from stg_reviews
where
    review_text is not null

    -- incremental model config
    {% if is_incremental() %}
        and review_date > (select max(review_date) from {{ this }})
    {% endif %}
