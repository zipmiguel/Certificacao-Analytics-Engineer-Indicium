with 
    source as (
        select * 
        from {{ source('erp', 'human_resources_employee') }}
    )
    , renamed as (
        select
            cast(businessentityid as int) as pk_funcionario
            , cast(jobtitle as varchar) as cargo
            , case
                when gender = 'M' then 'Masculino'
                when gender = 'F' then 'Feminino'
                else 'Outro' 
            end as genero
            , cast(hiredate as date) as data_contratacao
            --nationalidnumber,
            --loginid,            
            --birthdate,
            --maritalstatus,            
            --salariedflag,
            --vacationhours,
            --sickleavehours,
            --currentflag,
            --rowguid,
            --modifieddate,
            --organizationnode
        from source
    )
select * 
from renamed