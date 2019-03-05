{#
we ignore the WITH key word, as the base model is always ephemeral, and thus comes in as a CTE
#}

,
cte_source as
(
select  {{ dbt_utils.surrogate_key('invocation_id') }} as invocation_hsh,
        flag_strict_mode,
        flag_non_destructive,
        flag_full_refresh,
        dbt_version,
        {{ dbt_utils.surrogate_key('flag_strict_mode','flag_non_destructive','flag_full_refresh','dbt_version') }} as diff_hsh,
        --md5(FirstName || '|' || LastName ) as HashDiff,
        load_dte
from    {{ref('invocation_log')}} src

{% if is_incremental() %}

where   not exists (
                    select  1
                    from    {{ this }} trg
                    where   trg.invocation_hsh = {{ dbt_utils.surrogate_key('invocation_id') }}
                    and     trg.load_dte = src.load_dte
                    )


{% endif %}
)
,cte_condense as
(
select  invocation_hsh,
        flag_strict_mode,
        flag_non_destructive,
        flag_full_refresh,
        dbt_version,
        diff_hsh,
        load_dte,
        row_number() over (partition by invocation_hsh order by load_dte)
        -
        row_number() over (partition by
                                        invocation_hsh,
                                        diff_hsh
                                        order by load_dte) as ts
from    cte_source src
)
{% if is_incremental() %}
,cte_target as
(
select  trg.invocation_hsh,
        trg.diff_hsh
from    {{ this }} trg
        inner join (
                    select  current_rows.invocation_hsh,
                            max(load_dte) as load_dte
                    from    {{this}} current_rows
                    group by
                            current_rows.invocation_hsh
                  ) cr on trg.invocation_hsh = cr.invocation_hsh and trg.load_dte = cr.load_dte
)
{% endif %}
select  src.invocation_hsh,
        src.flag_strict_mode,
        src.flag_non_destructive,
        src.flag_full_refresh,
        src.dbt_version,
        src.diff_hsh,
        min(src.load_dte) as load_dte,
        '{{ invocation_id }}' as invocation_id
from    cte_condense src

{% if is_incremental() %}

        left join cte_target trg on src.invocation_hsh = trg.invocation_hsh and src.diff_hsh = trg.diff_hsh

where   trg.invocation_hsh is null

{% endif %}

group by
        src.invocation_hsh,
        src.flag_strict_mode,
        src.flag_non_destructive,
        src.flag_full_refresh,
        src.dbt_version,
        src.diff_hsh,
        src.ts
