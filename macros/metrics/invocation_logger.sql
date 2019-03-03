{#
shamelessly stolen from dbt-event-logging
#}
{% macro get_invocation_log_relation() %}
    {%- set invocation_log_table =
        api.Relation.create(
            identifier='dvm_invocation_log',
            schema='stage',
            type='table'
        ) -%}
    {{ return(invocation_log_table) }}
{% endmacro %}

{% macro get_invocation_log_schema() %}
    {% set invocation_log_table = dbt_dv_utils.get_invocation_log_relation() %}
    {{ return(invocation_log_table.include(schema=True, identifier=False)) }}
{% endmacro %}

{#
for MS SQL, we could:
  select sum([rows])
  from sys.partitions where index_id = min(index_id) to be much(!) more efficient
#}
{% macro persist_invocation_log_event(event_name, schema, relation) %}

        insert into {{ dbt_dv_utils.get_invocation_log_relation() }} (
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

{% macro create_invocation_log_schema() %}
    create schema if not exists {{ dbt_dv_utils.get_invocation_log_schema() }}
{% endmacro %}


{% macro create_invocation_log_table() %}

    create table if not exists {{ dbt_dv_utils.get_invocation_log_relation() }}
    (
       event_name       varchar(512),
       event_schema     varchar(512),
       event_model      varchar(512),
       invocation_id    varchar(512),
       project_version  varchar(512),
       initial_row_count int,
       run_row_count  int,
       final_row_count int
    )

{% endmacro %}

{% macro log_invocation_log_event() %}
    {%- set event_name = 'model run' -%}

    {% if flags.NON_DESTRUCTIVE %}
        {%- set event_name = event_name~':NON_DESTRUCTIVE' -%}
    {% endif %}

    {% if flags.FULL_REFRESH %}
        {%- set event_name = event_name~':FULL_REFRESH' -%}
    {% endif %}

    {{ dbt_dv_utils.persist_invocation_log_event(event_name, this.schema, this.name) }}
{% endmacro %}
