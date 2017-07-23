connection: "bigquery_publicdata_standard_sql"

include: "*view*"         # include all views in this project
# include: "*.dashboard.lookml"  # include all dashboards in this project

# # Select the views that should be a part of this model,
# # and define the joins that connect them together.


explore: financial_data {}
