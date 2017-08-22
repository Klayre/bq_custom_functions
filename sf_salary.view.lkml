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
    sql: ${TABLE}.BasePay ;;
  }

  dimension: overtime_pay {
    type: string
    sql: ${TABLE}.OvertimePay ;;
  }

  dimension: other_pay {
    type: string
    sql: ${TABLE}.OtherPay ;;
  }

  dimension: benefits {
    type: string
    sql: ${TABLE}.Benefits ;;
  }

  dimension: total_pay {
    type: string
    sql: ${TABLE}.TotalPay ;;
  }

  dimension: total_pay_benefits {
    type: string
    sql: ${TABLE}.TotalPayBenefits ;;
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
      overtime_pay,
      other_pay,
      benefits,
      total_pay,
      total_pay_benefits,
      year,
      notes,
      agency,
      status
    ]
  }
}
