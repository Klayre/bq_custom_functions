include: "events.view"

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

  measure: categories_visited {
    sql: pairs_count_distinct(ARRAY_AGG(STRUCT(${visited_category} as key,CAST(${id} AS STRING) as value))) ;;}

  # parse the product_id out of the urls visited and how many times they were visited
  measure: products_visited {
    sql: pairs_count_distinct(ARRAY_AGG(STRUCT(CAST(${visited_product_id} as STRING) as key, CAST(${id} AS STRING) as value))) ;;}

  measure: event_sequence {
    sql:  time_sequence(ARRAY_AGG(STRUCT(${created_raw} as ts, event_type as str))) ;;}
}
