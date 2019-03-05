{#
shamelessly stolen from dbt-event-logging
#}
{% macro get_column_log_relation() %}
    {%- set column_log_table =
        api.Relation.create(
            identifier='stage_dbt_metrics',
            schema=target.schema~'_metrics',
            type='table'
        ) -%}
    {{ return(column_log_table) }}
{% endmacro %}



{#
for MS SQL, we could:
  select sum([rows])
  from sys.partitions where index_id = min(index_id) to be much(!) more efficient
#}
{% macro persist_column_log_event(event_name, schema, relation) %}

        insert into {{ dv_metrics.get_column_log_relation() }} (
            event_name,
            event_schema,
            event_model,
            invocation_id,
            project_version,
            initial_row_count,
            run_row_count,
            final_row_count
            )
        select
            '{{ event_name }}',
            '{{ schema }}',
            '{{ relation }}',
            '{{ project }}',
            'TODO: Add Project, package and DBT versions',
            sum(case when invocation_id = '{{ invocation_id }}' then 0 else 1 end),
            sum(case when invocation_id = '{{ invocation_id }}' then 1 else 0 end),
            count(*)
            from  "{{ schema }}"."{{ relation }}"

{% endmacro %}

{% macro drop_column_log_table() %}
  drop table if exists {{ dv_metrics.get_column_log_relation() }}
{% endmacro %}

{% macro create_column_log_table() %}

    create table if not exists {{ dv_metrics.get_column_log_relation() }}
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
      column_name varchar(128),
      data_type_name varchar(128),
      empty_row_count int,
      unique_value_count int
    )

{% endmacro %}

{% macro log_column_log_event() %}
    {%- set event_name = 'model run' -%}

    {% if flags.NON_DESTRUCTIVE %}
        {%- set event_name = event_name~':NON_DESTRUCTIVE' -%}
    {% endif %}

    {% if flags.FULL_REFRESH %}
        {%- set event_name = event_name~':FULL_REFRESH' -%}
    {% endif %}

    {{ dv_metrics.persist_column_log_event(event_name, this.schema, this.name) }}
{% endmacro %}
