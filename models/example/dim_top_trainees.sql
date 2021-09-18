{{ config (
    materialized="view"
)}}

with top_trainees_4_BUSINESS_UNDERSTANDING as (

    select
        FULLNAME as traineeName
     from competency.trainees.competency_score
         where BUSINESS_UNDERSTANDING > 4

)
SELECT * FROM top_trainees_4_BUSINESS_UNDERSTANDING
