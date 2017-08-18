include: "/data_sources/weather_bq.model"

explore: weather_facts {}

view: weather_facts {
  derived_table: {
    explore_source: gsod {
      column: zipcode { field: county_zipcode_mapping.zipcode }
      column: total_precipitation {}
      filters: {
        field: stations.state
        value: "NY"
      }
    }
  }
  dimension: zipcode {}
  dimension: total_precipitation {}
}

view: weather_facts_month {
  derived_table: {
    sql_trigger_value: select count(*) ;;
    explore_source: gsod {
#       column: zip_code { field: users.zip_code }
      column: date_month {}
      column: total_precipitation {}
      filters: {
        field: gsod.year
        value: "2016"
      }
    }
  }
  dimension: zip_code {}
  dimension: date_month {}
  dimension: total_precipitation {}
  dimension: precipitation_tier {
    type: tier
    tiers: [0, 0.5, 1, 2, 4, 6]
    sql: ${total_precipitation} ;;
  }
}


#
#
# view: add_a_unique_name_1500854821 {
#   derived_table: {
#     explore_source: gsod {
#       column: zipcode { field: county_zipcode_mapping.zipcode }
#       column: total_precipitation {}
#       filters: {
#         field: stations.state
#         value: "NY"
#       }
#     }
#   }
#   dimension: zipcode {}
#   dimension: total_precipitation {}
# }
