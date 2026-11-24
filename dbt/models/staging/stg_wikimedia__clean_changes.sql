{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['wiki', 'rc_id'],
        on_schema_change='append_new_columns'
    )
}}

with source as (

    select
        record_content as event,
        to_timestamp_ltz(record_metadata:SnowflakeConnectorPushTime::number, 3) as loaded_at
    from {{ source('raw', 'clean_changes') }}

    {% if is_incremental() %}
    where to_timestamp_ltz(record_metadata:SnowflakeConnectorPushTime::number, 3)
        > (select dateadd('minute', -10, max(loaded_at)) from {{ this }})
    {% endif %}

),

typed as (

    select
        event:event_id::number                  as rc_id,
        event:event_time::timestamp_ltz         as event_time,
        event:wiki::string                      as wiki,
        event:server_name::string               as server_name,
        try_to_number(event:namespace::string)  as namespace,
        event:title::string                     as title,
        event:change_type::string               as change_type,
        event:user_name::string                 as user_name,
        event:is_bot::boolean                   as is_bot,
        loaded_at
    from source

)

select *
from typed
where rc_id is not null
qualify row_number() over (partition by wiki, rc_id order by loaded_at) = 1