{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['wiki', 'minute']
    )
}}

with events as (

    select *
    from {{ ref('stg_wikimedia__recent_changes') }}
    where change_type in ('edit', 'new')

    {% if is_incremental() %}
    and event_time >= (select dateadd('hour', -1, max(minute)) from {{ this }})
    {% endif %}

)

select
    wiki,
    date_trunc('minute', event_time)                        as minute,
    count(*)                                                as edits,
    count_if(is_bot)                                        as bot_edits,
    count_if(not is_bot)                                    as human_edits,
    count_if(change_type = 'new')                           as pages_created,
    count(distinct user_name)                               as editors,
    coalesce(sum(iff(bytes_changed > 0, bytes_changed, 0)), 0)  as bytes_added,
    coalesce(sum(iff(bytes_changed < 0, -bytes_changed, 0)), 0) as bytes_removed
from events
group by 1, 2
