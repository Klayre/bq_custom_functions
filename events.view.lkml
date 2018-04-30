view: events {
  sql_table_name: thelook_web_analytics.events ;;

  dimension: id {primary_key:yes  type:number}
  dimension: browser {}
  dimension: city {}
  dimension: country {}
  dimension_group: created {type:time  sql: ${TABLE}.created_at ;;
    #allow_fill:no
    }
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
  dimension: visited_category {sql: REGEXP_EXTRACT(${uri}, r'/category/([^/]*)')  ;; }

  measure: event_count {type:count
    label: "Count of Events"
    drill_fields:[id, created_time, ip_address, users.id, uri, traffic_source ]}
  measure: user_count {type:count_distinct  sql: ${user_id} ;; drill_fields:[user_id, event_count]}
}
