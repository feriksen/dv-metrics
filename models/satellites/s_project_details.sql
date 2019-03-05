
{#
we ignore the WITH key word, as the base model is always ephemeral, and thus comes in as a CTE
#}

,
cte_project as
(
select  project_name,
        project_version,
        min(load_dte) as load_dte
from    {{ref('invocation_log')}}
group by
        project_name,
        project_version
union
select  package_name,
        package_version,
        min(load_dte) as load_dte
from    {{ref('invocation_log')}}
group by
        package_name,
        package_version
),
cte_source as
(
select  {{ dbt_utils.surrogate_key('project_name') }} as project_hsh,
        project_version,
        {{ dbt_utils.surrogate_key('project_version') }} as diff_hsh,
        load_dte
from    cte_project src

{% if is_incremental() %}

where   not exists (
                    select  1
                    from    {{ this }} trg
                    where   trg.project_hsh = {{ dbt_utils.surrogate_key('project_name') }}
                    and     trg.load_dte = src.load_dte
                    )


{% endif %}
)
,cte_condense as
(
select  project_hsh,
        project_version,
        diff_hsh,
        load_dte,
        row_number() over (partition by project_hsh order by load_dte)
        -
        row_number() over (partition by
                                        project_hsh,
                                        diff_hsh
                                        order by load_dte) as ts
from    cte_source src
)
{% if is_incremental() %}
,cte_target as
(
select  trg.project_hsh,
        trg.diff_hsh
from    {{ this }} trg
        inner join (
                    select  current_rows.project_hsh,
                            max(load_dte) as load_dte
                    from    {{this}} current_rows
                    group by
                            current_rows.project_hsh
                  ) cr on trg.project_hsh = cr.project_hsh and trg.load_dte = cr.load_dte
)
{% endif %}
select  src.project_hsh,
        src.project_version,
        src.diff_hsh,
        min(src.load_dte) as load_dte,
        '{{ invocation_id }}' as invocation_id
from    cte_condense src

{% if is_incremental() %}

        left join cte_target trg on src.project_hsh = trg.project_hsh and src.diff_hsh = trg.diff_hsh

where   trg.project_hsh is null

{% endif %}

group by
        src.project_hsh,
        src.project_version,
        src.diff_hsh,
        src.ts
