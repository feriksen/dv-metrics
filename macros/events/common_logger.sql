{% macro create_dv_metrics_schema() %}
    create schema if not exists {{ target.schema }}_metrics
{% endmacro %}

{% macro log_metric_event() %}
    {%- set schema_name = this.schema -%}
    {%- set model_name = this.name -%}
    {%- set project_name = 'project name' -%}
    {%- set project_version = '0.0.0' -%}

    {# We dont log internal metrics - TBD: should we? if so, need to add invocation_id as load_src.. #}
    {% if schema_name != '{{ target.schema }}_metrics' %}
        {{ dv_metrics.persist_invocation_log_event(project_name, project_version, schema_name, model_name) }}
    {% endif %}
{% endmacro %}
