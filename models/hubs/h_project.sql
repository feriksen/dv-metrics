
select  {{ dbt_utils.surrogate_key('src.project_name') }} as project_hsh,
        src.project_name,
        min(src.load_dte) as load_dte,
        '{{ invocation_id }}' as invocation_id
from    (
        select  project_name,
                min(load_dte) as load_dte
        from    {{ref('invocation_log')}}
        group by
                project_name
        union
        select  package_name,
                min(load_dte) as load_dte
        from    {{ref('invocation_log')}}
        group by
                package_name
        ) src
{% if is_incremental() %}

        left join {{ this }} dest on dest.project_name = src.project_name
where   dest.project_hsh is null

{% endif %}

group by
        src.project_name
