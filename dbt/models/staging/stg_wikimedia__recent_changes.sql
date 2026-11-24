{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='event_id',
        on_schema_change='append_new_columns'
    )
}}

with source as (

    select
        record_content as event,
        record_metadata as kafka,
        to_timestamp_ltz(record_metadata:SnowflakeConnectorPushTime::number, 3) as loaded_at
    from {{ source('raw', 'recent_changes') }}

    {% if is_incremental() %}
    where to_timestamp_ltz(record_metadata:SnowflakeConnectorPushTime::number, 3)
        > (select dateadd('minute', -10, max(loaded_at)) from {{ this }})
    {% endif %}

),

typed as (

    select
        event:meta:id::string                       as event_id,
        event:id::number                            as rc_id,
        to_timestamp_ltz(event:timestamp::number)   as event_time,
        event:wiki::string                          as wiki,
        event:server_name::string                   as server_name,
        event:namespace::number                     as namespace,
        event:title::string                         as title,
        event:title_url::string                     as title_url,
        event:type::string                          as change_type,
        event:user::string                          as user_name,
        event:bot::boolean                          as is_bot,
        coalesce(event:minor::boolean, false)       as is_minor,
        event:comment::string                       as comment,
        event:length:old::number                    as length_old,
        event:length:new::number                    as length_new,
        -- NULL for events without a length, such as log entries
        event:length:new::number - coalesce(event:length:old::number, 0) as bytes_changed,
        event:revision:old::number                  as revision_old,
        event:revision:new::number                  as revision_new,
        kafka:partition::number                     as kafka_partition,
        kafka:offset::number                        as kafka_offset,
        loaded_at
    from source

)

select *
from typed
where event_id is not null
-- The producer can send an event twice after a reconnect. Keep the first copy.
qualify row_number() over (partition by event_id order by loaded_at, kafka_offset) = 1