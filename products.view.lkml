view: products {
  sql_table_name: thelook_web_analytics.products ;;

  dimension: id {primary_key: yes  type:number}
  dimension: brand {}
  dimension: category {}
  dimension: cost {type: number}
  dimension: department {}
  dimension: name {}
  dimension: retail_price {}
  dimension: sku {}
  measure: count {type:count  drill_fields: [id, name]}
}
