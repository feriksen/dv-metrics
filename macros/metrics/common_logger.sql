{% macro create_dv_metrics_schema() %}
    create schema if not exists stage;
    create schema if not exists dvm;
{% endmacro %}
