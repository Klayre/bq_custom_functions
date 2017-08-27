include: "events_and_sessions.model.lkml"

view: sessions {
  derived_table: {
    persist_for: "2 hours"
    explore_source: events_for_sessionization {
      column: id { field: events.session_id }
      column: events_fired { field: events.events_fired }
      column: session_time { field: events.minimum_time }
      column: session_end_time {field: events.max_time}
      column: ip_addresses {field: events.ip_addresses }
      column: user_id {field: events.user_id }
      column: products_visited {field: events.products_visited}
      column: categories_visited {field: events.categories_visited }
      column: event_sequence {field: events.event_sequence}
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
  dimension: categories_visited {hidden:yes}
  dimension: category_list {sql: pairs_to_string(${categories_visited}) ;; }
  dimension: session_sequence {type:number}
  dimension: session_length {type:number
    sql: TIMESTAMP_DIFF(${session_end_time},${session_raw}, SECOND) ;;}
  dimension: session_length_tiered {type:tier  tiers: [0,60,120]  sql: ${session_length} ;;}
  dimension: has_cancel {type:yesno
    sql: (SELECT COUNT(*) FROM UNNEST(${events_fired}) ef WHERE ef.key='Cancel') > 0;;}
  dimension: event_sequence {}

  measure: count_sessions {type:count  drill_fields:[session*]}
  measure: average_session_length {type:average  sql:${session_length};;}
  measure: user_count {type:count_distinct  sql: ${user_id} ;; drill_fields:[user_id, count_sessions]}
  set: session{ fields:[session_time, id, user_id, event_sequence]}
}

view: events_fired {
  dimension: id { primary_key:yes hidden:yes sql: CONCAT(CAST(${sessions.id} as STRING), ${event_type}) ;; }
  dimension: event_type {sql: ${TABLE}.key;; }
  measure: times_fired {type:sum  sql: ${TABLE}.value ;;}
}

view: categories_visited {
  dimension: id { primary_key:yes hidden:yes sql: CONCAT(CAST(${sessions.id} as STRING), ${visited_category}) ;; }
  dimension: visited_category {sql: ${TABLE}.key;; }
  measure: times_visited {type:sum  sql: ${TABLE}.value ;;}
}

view: products_visited {
  dimension: id { primary_key:yes hidden:yes sql: CONCAT(CAST(${sessions.id} as STRING), CAST(${product_id} AS STRING)) ;; }
  dimension: product_id {type:number  sql: CAST(${TABLE}.key AS INT64);; }
  measure: times_visited {type:sum  sql: ${TABLE}.value ;;}
}
