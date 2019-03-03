select  {{ dbt_utils.surrogate_key('src.invocation_id') }} as invocation_hsh,
        src.invocation_id,
        min(src.load_dte) as load_dte
from    {{ref('invocation_log')}} src

{% if is_incremental() %}

        left join {{ this }} dest on dest.invocation_id = src.invocation_id
where   dest.invocation_hsh is null

{% endif %}

group by
        src.invocation_id
