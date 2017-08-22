
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
      filters: {
        field: indicators_metadata.description
        value: "Consumer Price Index for All Urban Consumers: All Items"
      }
    }
  }
#   dimension: indicator_date {
#   }

  dimension_group: indicator {
    type: time
    timeframes: [date, month, year]
    sql: cast(${TABLE}.indicator_date as date) ;;
    convert_tz: no
  }
  dimension_group: next_indicator {
    type: time
    timeframes: [date, month, year]
    sql:cast(${TABLE}.next_indicator_date as date) ;;
    convert_tz: no
  }
  dimension: primary_key {
    sql: concat(cast((${TABLE}.indicator_date) as string), ${description}) ;;
    primary_key: yes
  }
  dimension: description {}
  dimension: total_value {}
  dimension: indicator_growth_yoy {}
}
