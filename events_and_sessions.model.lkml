connection: "bigquery_publicdata_standard_sql"

include: "custom_functions.view"
include: "products.view"

explore: events {
  view_name: events
  join: products {
    view_label: "Products Visited"
    sql_on: ${events.visited_product_id}=${products.id} ;;
    relationship: one_to_many
  }
}

view: events {
  sql_table_name: thelook_web_analytics.events ;;

  dimension: id {primary_key:yes  type:number}
  dimension: browser {}
  dimension: city {}
  dimension: country {}
  dimension_group: created {type:time  sql: ${TABLE}.created_at ;;}
  dimension: event_type {}
  dimension: ip_address {}
  dimension: latitude {type:number}
  dimension: longitude {type:number}
  dimension: os {}
  dimension: sequence_number {type:number}
  dimension: session_id {}
  dimension: state {}
  dimension: traffic_source {}
  dimension: uri {}
  dimension: user_id {type:number  sql: CAST(REGEXP_EXTRACT(${TABLE}.user_id, r'\d+') AS INT64) ;;}
  dimension: zip {}
  dimension: visited_product_id {type:number sql: CAST(REGEXP_EXTRACT(${uri}, r'/product/(\d+)') AS INT64) ;; }

  measure: count {type:count
    drill_fields:[id, created_time, ip_address, users.id, uri, traffic_source ]}
}

#-----------------------------------------------------------------
#  Add some measures and dimension for sessionization.
#-----------------------------------------------------------------

explore: events_for_sessionization {
  sql_preamble:  --stuff ;;
  from: events_for_sessionization
  extends: [events, custom_functions]
}

view: events_for_sessionization {
  extends: [events]
  #
  #  Simple aggregates
  #
  measure: minimum_time {sql: MIN(${created_raw}) ;;}
  measure: max_time {sql: MAX(${created_raw}) ;;}

  # Hyper-trees
  measure: ip_addresses {
    sql: pairs_count_distinct(ARRAY_AGG(STRUCT(${ip_address} as key,CAST(${id} AS STRING) as value))) ;;}

  measure: events_fired {
    sql: pairs_count_distinct(ARRAY_AGG(STRUCT(${event_type} as key,CAST(${id} AS STRING) as value))) ;;}

  # parse the product_id out of the urls visited and how many times they were visited
  measure: products_visited {
    sql: pairs_count_distinct(ARRAY_AGG(STRUCT(CAST(${visited_product_id} as STRING) as key, CAST(${id} AS STRING) as value))) ;;}

}

#---------------------------------------------------------------------
#  Explore session data
#---------------------------------------------------------------------


explore: sessions {
  extends: [custom_functions]
  join: events_fired {
    sql: LEFT JOIN UNNEST(${sessions.events_fired}) as events_fired ;;
    relationship: one_to_many
  }
  join: products_visited {
    sql: LEFT JOIN UNNEST(${sessions.products_visited}) as products_visited ;;
    relationship: one_to_many
  }
  join: products {
    view_label: "Products Visited"
    sql_on: ${products_visited.product_id}=${products.id} ;;
    relationship: one_to_many
  }
}
view: sessions {
  derived_table: {
    #persist_for: "2 hours"
    explore_source: events_for_sessionization {
      column: id { field: events.session_id }
      column: events_fired { field: events.events_fired }
      column: session_time { field: events.minimum_time }
      column: session_end_time {field: events.max_time}
      column: ip_addresses {field: events.ip_addresses }
      column: user_id {field: events.user_id }
      column: products_visited {field: events.products_visited}
      derived_column: session_sequence {
        sql: ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY session_time) ;;
      }
    }
  }
  dimension: id {primary_key:yes}
  dimension: events_fired {hidden:yes}
  dimension: event_types {sql: pairs_to_string(${events_fired},'decimal_0') ;;}
  dimension_group: session {type:time  sql: ${TABLE}.session_time ;;}
  dimension: session_end_time {hidden:yes}
  dimension: ip_addresses {hidden:yes}
  dimension: user_id {}
  dimension: products_visited {hidden:yes}
  dimension: session_sequence {type:number}
  dimension: session_length {type:number
    sql: TIMESTAMP_DIFF(${session_end_time},${session_raw}, SECOND) ;;}
  dimension: session_length_tiered {type:tier  tiers: [0,60,120]  sql: ${session_length} ;;}
  dimension: has_cancel {type:yesno
    sql: (SELECT COUNT(*) FROM UNNEST(${events_fired}) ef WHERE ef.key='Cancel') > 0;;}

  measure: count_sessions {type:count  drill_fields:[session*]}
  measure: average_session_length {type:average  sql:${session_length};;}
  set: session{ fields:[session_time, id, user_id, event_types]}
}

view: events_fired {
  dimension: id { primary_key:yes hidden:yes sql: CONCAT(CAST(${sessions.id} as STRING), ${event_type}) ;; }
  dimension: event_type {sql: ${TABLE}.key;; }
  measure: times_fired {type:sum  sql: ${TABLE}.value ;;}
}

view: products_visited {
  dimension: id { primary_key:yes hidden:yes sql: CONCAT(CAST(${sessions.id} as STRING), ${product_id}) ;; }
  dimension: product_id {type:number  sql: CAST(${TABLE}.key AS INT64);; }
  measure: times_visited {type:sum  sql: ${TABLE}.value ;;}
}
