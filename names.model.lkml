connection: "bigquery_publicdata_standard_sql"
include: "custom_functions.view.lkml"


explore: names {
  hidden: yes
  extends: [custom_functions]
  persist_for: "24 hours"
}

view: names {
  sql_table_name: `fh-bigquery.popular_names.usa_1910_2013`
    ;;

  # Fields in the table
  dimension: name {}
  dimension: state {}
  dimension: gender {}
  dimension: year {type: number}
  dimension: population {type:number  sql: CAST(${TABLE}.number AS FLOAT64) ;; }

  # computed dimensions
  dimension: decade {type:number  sql: CAST(FLOOR(${year}/10)*10 AS INT64) ;;}
  dimension: first_letter {sql: SUBSTR(${name},1,1) ;;}

  # aggregate calculastions
  measure: total_population {type:sum  sql: ${population} ;;
    drill_fields:[state, gender, year, name, population]}

  # Top-N Measures

  measure: top_5_names {
    sql: pairs_sum_top_n(ARRAY_AGG(STRUCT(${name} as key, ${population} as value)), 5) ;;}
  measure: top_3_years {
    sql: pairs_sum_top_n(ARRAY_AGG(STRUCT(CAST(${year} as STRING) as key, ${population} as value)), 3) ;;}
  measure: top_5_states {
    sql: pairs_sum_top_n(ARRAY_AGG(STRUCT(${state} as key, ${population} as value)), 5) ;;}

 # Graphs

  measure: decade_graph {
    sql: time_graph(ARRAY_AGG(STRUCT(CAST(${decade} AS STRING) as key, ${population} as value)),10) ;;
    html:
     <img src="https://chart.googleapis.com/chart?chs=200x50&cht=ls&chco=0077CC&chf=bg,s,FFFFFF00&chxt=x&chxr=0,1910,2010,20&chd=t:{{value}}">
    ;;}

  measure: year_graph {
    sql: time_graph(ARRAY_AGG(STRUCT(CAST(${year} AS STRING) as key, ${population} as value)),1) ;;
    html:
    <img src="https://chart.googleapis.com/chart?chs=200x50&cht=ls&chco=0077CC&chxt=x&chxr=0,1910,2013,20&chf=bg,s,FFFFFF00&chd=t:{{value}}">
    ;;}

  measure: gender_balance_graph {
    sql:  pairs_sum_graph(ARRAY_AGG(STRUCT(${gender} as key, ${population} as value)))  ;;
    html:
    <img src="https://chart.googleapis.com/chart?chs=200x50&cht=p3&chf=bg,s,FFFFFF00&{{value}}">
    ;;}

  }
