SELECT DISTINCT
    bike_id,
    bike_type
FROM {{ source('austin_bikeshare', 'bikeshare_trips') }}