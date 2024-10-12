{{config(materialized='table')}}



WITH source_data AS (
    SELECT * FROM {{ source('public','cleandata') }}  
)

SELECT
    "Channel Title",
    "Channel Username",
    "ID",
    "Message",
    "Date",
    "Media Path"
FROM source_data