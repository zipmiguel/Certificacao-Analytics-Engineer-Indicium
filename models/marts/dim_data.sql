with 
    date_spine as (
        {{ dbt_utils.date_spine(
            start_date="cast('2010-01-01' as date)"
            , end_date="cast('2015-12-31' as date)"
            , datepart="day"
        ) }}
    )

    , final as (
        select
            {{ dbt_utils.generate_surrogate_key(['date_day']) }} as sk_data
            , date_day as data
            , extract(year from date_day) as ano
            , extract(month from date_day) as mes
            , extract(day from date_day) as dia
            , extract(quarter from date_day) as trimestre
            , trim(to_char(date_day, 'Day')) as dia_semana
            , to_char(date_day, 'YYYY-MM') as ano_mes
        from date_spine
    )
select *
from final