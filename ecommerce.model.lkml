connection: "bigquery_publicdata_standard_sql"

# include all the views
include: "*.view"

# include all the dashboards
include: "*.dashboard"

include: "*.explore"

explore: users {
  join: weather_facts {
    relationship: many_to_one
    sql_on: ${users.zip} = ${weather_facts.zipcode} ;;
  }
}

explore: order_items {
  join: users {
    sql_on: ${users.id} = ${order_items.user_id} ;;
    type: left_outer
    relationship: many_to_one
  }
  join: inventory_items {
    type: left_outer
    sql_on: ${order_items.inventory_item_id} = ${inventory_items.id} ;;
    relationship: many_to_one
  }

  join: products {
    type: left_outer
    sql_on: ${inventory_items.product_id} = ${products.id} ;;
    relationship: many_to_one
  }
  join: weather_facts_month {
    relationship: many_to_one
    sql_on: ${users.zip} = ${weather_facts_month.zip_code}
    and ${order_items.created_month} = ${weather_facts_month.date_month};;
  }
  join: financial_data {
    sql_on:
      ${order_items.created_year} >= cast(timestamp(${financial_data.indicator_date}) as date)
      and ${financial_data.description} = 'Consumer Price Index for All Urban Consumers: All Items';;
    type: left_outer
    relationship: many_to_one

  }
}

#
#       ${order_items.created_date} >= cast(timestamp(${financial_data.indicator_date}) as date)
#       and ${order_items.created_date} < cast(timestamp(${financial_data.next_indicator_date}) as date)
explore: financial_data {}
