with source as (
    select * from {{ source('trips_data_all', 'fhv_2019') }}),

import as (
    select *
    FROM source
    WHERE dispatching_base_num IS NOT NULL)

SELECT *
FROM import
