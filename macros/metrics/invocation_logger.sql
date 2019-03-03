{#
shamelessly stolen from dbt-event-logging
#}
{% macro get_invocation_log_relation() %}
    {%- set invocation_log_table =
        api.Relation.create(
            identifier='stage_dvm_invocation_log',
            schema=target.schema~'_metrics',
            type='table'
        ) -%}
    {{ return(invocation_log_table) }}
{% endmacro %}

{#
for MS SQL, we could:
  select sum([rows])
  from sys.partitions where index_id = min(index_id) to be much(!) more efficient
#}
{% macro persist_invocation_log_event(project_name, project_version, schema_name, model_name) %}

        insert into {{ dv_metrics.get_invocation_log_relation() }} (
            invocation_id,
            dbt_version,
            project_name,
            project_version,
            package_name,
            package_version,
            event_timestamp,
            model_schema,
            model_name,
            flag_strict_mode,
            flag_non_destructive,
            flag_full_refresh,
            initial_row_count,
            invocation_row_count,
            final_row_count
            )
        select
            '{{ invocation_id }}',
            '0.0.0',
            '{{ project_name }}',
            '{{project_version}}',
            'package name',
            '0.0.0',
            {{dbt_utils.current_timestamp_in_utc()}},
            '{{ schema_name }}',
            '{{ model_name }}',
            {% if flags.STRICT_MODE  %}'Y'{% else %}'N'{% endif %},
            {% if flags.NON_DESTRUCTIVE  %}'Y'{% else %}'N'{% endif %},
            {% if flags.FULL_REFRESH  %}'Y'{% else %}'N'{% endif %},
            sum(case when invocation_id = '{{ invocation_id }}' then 0 else 1 end),
            sum(case when invocation_id = '{{ invocation_id }}' then 1 else 0 end),
            count(*)
            from  "{{ schema }}"."{{ model_name }}"

{% endmacro %}

{% macro drop_invocation_log_table() %}
  drop table if exists {{ dv_metrics.get_invocation_log_relation() }}
{% endmacro %}

{% macro create_invocation_log_table() %}

    create table if not exists {{ dv_metrics.get_invocation_log_relation() }}
    (
      invocation_id varchar(36),
      dbt_version varchar(12),
      project_name varchar(128),
      project_version varchar(12),
      package_name varchar(128),
      package_version varchar(12),
      event_timestamp timestamp,
      model_schema varchar(128),
      model_name varchar(128),
      flag_strict_mode char(1),
      flag_non_destructive char(1),
      flag_full_refresh char(1),
      initial_row_count int,
      invocation_row_count int,
      final_row_count int
    )

{% endmacro %}
