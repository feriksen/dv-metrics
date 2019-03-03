{% macro create_dv_metrics_schema() %}
    create schema if not exists target.schema~'_metrics'
{% endmacro %}
