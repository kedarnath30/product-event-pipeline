{% macro funnel_steps() %}
  {{ return(["Estimate Started", "Estimate Completed", "Account Created", "Application Submitted",
             "Documents Uploaded", "Offer Viewed", "Offer Accepted"]) }}
{% endmacro %}
