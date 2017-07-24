
# include: "*.view.lkml"         # include all views in this project
# include: "*.dashboard.lookml"  # include all dashboards in this project

include: "/data_sources/finance.model"

view: financial_data {
  derived_table: {
    sql_trigger_value: select count(*) ;;
    explore_source: financial_indicators {
      column: indicator_date {}
      column: next_indicator_date {}
      column: description { field: indicators_metadata.description }
      column: total_value {}
      column: indicator_growth_yoy { field: indicator_yoy_facts.indicator_growth_yoy }
      filters: {
        field: financial_indicators.category
        value: "Prices and Inflation"
      }
    }
  }
  dimension: indicator_date {
#     convert_tz: no
  }
  dimension: next_indicator_date {}
  dimension: primary_key {
    sql: concat(cast((${indicator_date}) as string), ${description}) ;;
    primary_key: yes
  }
  dimension: description {}
  dimension: total_value {}
  dimension: indicator_growth_yoy {}
}
