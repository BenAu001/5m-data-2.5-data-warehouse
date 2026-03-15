with stations as (
    select
        station_id,
        station_name,
        status,
        location,
        address,
        alternate_name,
        city_asset_number,
        property_type,
        number_of_docks,
        power_type,
        footprint_length,
        footprint_width,
        notes,
        council_district,
        image,
        modified_date
    from {{ ref('station_snapshot') }}
    where dbt_valid_to is null
),

trip_starts as (
    select
        start_station_id                  as station_id,
        sum(duration_minutes * 60)        as total_duration,
        count(start_station_name)         as total_starts
    from {{ source('austin_bikeshare', 'bikeshare_trips') }}
    group by start_station_id
),

trip_ends as (
    select
        end_station_id                    as station_id,
        count(end_station_name)           as total_ends
    from {{ source('austin_bikeshare', 'bikeshare_trips') }}
    group by end_station_id
)

select
    s.station_id,
    s.station_name,
    s.status,
    s.location,
    s.address,
    s.alternate_name,
    s.city_asset_number,
    s.property_type,
    s.number_of_docks,
    s.power_type,
    s.footprint_length,
    s.footprint_width,
    s.notes,
    s.council_district,
    s.image,
    s.modified_date,
    coalesce(ts.total_duration, 0)        as total_duration,
    coalesce(ts.total_starts, 0)          as total_starts,
    coalesce(te.total_ends, 0)            as total_ends
from stations s
left join trip_starts ts on s.station_id = ts.station_id
left join trip_ends te on s.station_id = te.station_id