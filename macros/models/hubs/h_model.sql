select  {{ dbt_utils.surrogate_key('src.model_schema', 'src.model_name') }} as model_hsh,
        src.model_schema,
        src.model_name,
        min(src.event_timestamp) as load_dte,
        '{{ invocation_id }}' as invocation_id
from    {{ref('invocation_log')}} src

{% if is_incremental() %}

        left join {{ this }} dest on dest.model_schema = src.model_schema and dest.model_name = src.model_name
where   dest.model_hsh is null

{% endif %}

group by
        src.model_schema
        , src.model_name
