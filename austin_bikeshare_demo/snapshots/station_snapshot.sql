{% snapshot station_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='station_id',
    strategy='timestamp',
    updated_at='modified_date'
  )
}}

select
    cast(station_id as string) as station_id,
    name as station_name,
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
    cast(modified_date as timestamp) as modified_date
from {{ source('austin_bikeshare', 'bikeshare_stations') }}

{% endsnapshot %}