{{ config(
    materialized='external', 
    location='output/netflix_top_shows.csv', 
    format='csv'
 )}}

-- SQL GOES HERE

SELECT
    "As of" date_label, Rank, Title, Type
    FROM 'https://s3.amazonaws.com/uvads-systems/data/netflix_daily_top_10.parquet'
    WHERE Rank = 1
    ORDER BY date_label DESC