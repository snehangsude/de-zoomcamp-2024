{% macro set_limit() %}
    {% if var('test', default=True) %}
        
        limit 30

    {% endif %}
{% endmacro%}