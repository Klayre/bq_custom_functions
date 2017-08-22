connection: "bigquery_publicdata_standard_sql"

include: "custom_functions.view.lkml"         # include all views in this project
include: "*.dashboard.lookml"  # include all dashboards in this project

explore: sf_salary {
  extends: [cf_empty, math_functions_median]
}

view: sf_salary {
  sql_table_name: `lookerdata.sfsalary.salaries` ;;

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: number
    sql: ${TABLE}.Id ;;
  }

  dimension: employee_name {
    type: string
    sql: ${TABLE}.EmployeeName ;;
  }

  dimension: job_title {
    type: string
    sql: ${TABLE}.JobTitle ;;
  }

  dimension: base_pay {
    type: string
    sql: CASE WHEN
          REGEXP_CONTAINS(${TABLE}.BasePay, r'^[\d\.]+$')
          THEN CAST(${TABLE}.BasePay AS FLOAT64)
         END ;;
  }

  measure: average_base_pay {
    type: average
    sql: ${base_pay} ;;
  }

  measure: median_base_pay {
    type: number
    sql: MEDIAN(ARRAY_AGG(${base_pay})) ;;
  }

  dimension: year {
    type: string
    sql: ${TABLE}.Year ;;
  }

  dimension: notes {
    type: string
    sql: ${TABLE}.Notes ;;
  }

  dimension: agency {
    type: string
    sql: ${TABLE}.Agency ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.Status ;;
  }

  set: detail {
    fields: [
      id,
      employee_name,
      job_title,
      base_pay,
      year,
      notes,
      agency,
      status
    ]
  }
}
