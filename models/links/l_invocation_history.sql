{#
this is a non-historized link.

We add inner joins to the hubs here for no other reason then to show the depencies
As we are running on a single invocation (log tables are recreated each run), this should be acceptable.

#}



select  src.invocation_history_hsh,
        src.invocation_hsh,
        src.model_hsh,
        src.project_hsh,
        src.package_hsh,
        src.initial_row_count,
        src.invocation_row_count,
        src.final_row_count,
        src.load_dte,
        '{{ invocation_id }}' as invocation_id
from    (
        select  {{ dbt_utils.surrogate_key('src.invocation_id','src.model_schema', 'src.model_name','src.project_name','src.package_name') }} as invocation_history_hsh,
                {{ dbt_utils.surrogate_key('src.invocation_id') }} as invocation_hsh,
                {{ dbt_utils.surrogate_key('src.model_schema', 'src.model_name') }} as model_hsh,
                {{ dbt_utils.surrogate_key('src.project_name') }} as project_hsh,
                {{ dbt_utils.surrogate_key('src.package_name') }} as package_hsh,
                src.initial_row_count,
                src.invocation_row_count,
                src.final_row_count,
                src.load_dte
        from    {{ref('invocation_log')}} src
        ) src
        left join {{ref('h_invocation')}} h1 on src.invocation_hsh = h1.invocation_hsh
        left join {{ref('h_model')}} h2 on src.model_hsh = h2.model_hsh
        left join {{ref('h_project')}} h3 on src.project_hsh = h3.project_hsh
        left join {{ref('h_project')}} h4 on src.package_hsh = h4.project_hsh
