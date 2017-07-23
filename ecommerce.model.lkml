connection: "bigquery_publicdata_standard_sql"

# include all the views
include: "*.view"

# include all the dashboards
include: "*.dashboard"

include: "*.explore"

explore: users {
  join: order_items {
    sql: ${users.id} = ${order_items.user_id} ;;
    type: left_outer
    relationship: one_to_many
  }
}
