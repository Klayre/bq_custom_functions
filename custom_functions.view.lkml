explore: custom_functions {
  extension: required
  sql_preamble:
    CREATE TEMP FUNCTION COUNT_DISTINCT_ARRAY(s ARRAY<STRING>)
    RETURNS INT64 AS ((
      SELECT COUNT(DISTINCT x) FROM UNNEST(s) as x
    ));

    CREATE TEMP FUNCTION STRING_AGG_DISTINCT(s ARRAY<STRING>)
    RETURNS STRING AS ((
      SELECT STRING_AGG(x,', ')
      FROM (
        SELECT x
        FROM UNNEST(s) as x
        WHERE x <> ''
        GROUP BY 1 ORDER BY 1

      )
    ));

    CREATE TEMP FUNCTION GET_URL_PARAM(query STRING, p STRING)
    RETURNS STRING
    LANGUAGE js AS """
      ret = null
      try{
        if(query) {
          params = query.split("&").forEach(function(part){
            item  = part.split('=')
            if(item[0] == p ) {
              ret = decodeURIComponent(item[1])
            }
          });
        }
      }
      catch(err){}
      return ret
    """;

    CREATE TEMP FUNCTION GET_URL_KEYS(query STRING)
    RETURNS ARRAY<STRING>
    LANGUAGE js AS """
      ret = []
      try{
        if(query) {
          params = query.split("&").forEach(function(part){
            ret.push( part.split('=')[0] )
          });
        }
      }
      catch(err){}
      return ret
    """;

    CREATE TEMP FUNCTION GET_VIS_PARAM(query STRING, p STRING)
    RETURNS STRING
    LANGUAGE js AS """
      ret = null
      try {
        if(query) {
          params = query.split("&").forEach(function(part){
            item  = part.split('=')
            if(item[0] == 'vis' ) {
              ret = JSON.parse(decodeURIComponent(item[1]))[p]
            }
          });
        }
      }
      catch(err){}
      return ret
    """;

     -- take a dimension, number pair and aggregate as a sum
      CREATE TEMP FUNCTION pairs_sum(a ARRAY<STRUCT<str STRING, num FLOAT64>>)
      RETURNS ARRAY<STRUCT<str STRING, num FLOAT64>> AS ((
        SELECT
           ARRAY_AGG(STRUCT(str,total_num as num))
        FROM (
          SELECT
            str
            , SUM(num) as total_num
          FROM UNNEST(a)
          GROUP BY 1
          ORDER BY 2 DESC
        )
      ));

      -- take a set of string, number pairs and convert the number to percentage of max or total
      -- pass 'total' or 'max' as type to change behaviour
      CREATE TEMP FUNCTION pairs_convert_percentage(a ARRAY<STRUCT<str STRING, num FLOAT64>>,type STRING)
      RETURNS ARRAY<STRUCT<str STRING, num FLOAT64>> AS ((
        SELECT
          ARRAY_AGG(STRUCT(str,new_num as num))
        FROM (
          SELECT
            str
            , 100.0*num/total
             as new_num
          FROM UNNEST(a)
          CROSS JOIN (
            SELECT
              CASE
               WHEN type='total' THEN SUM(b.num)
               WHEN type='max' THEN MAX(b.num)
              END
              as total FROM UNNEST(a) as b
          ) as t
          ORDER BY 2 DESC
        )
      ));

      -- formats a STR N into String(number)
      CREATE TEMP FUNCTION format_result(str STRING, num FLOAT64, format_str STRING)
      RETURNS STRING AS ((
        SELECT
           CONCAT(str, '(',
            CASE
              WHEN format_str = 'decimal_0'
                THEN FORMAT("%0.0f", num)
              WHEN format_str = 'percent_0'
                THEN FORMAT("%0.2f%%", num)
            END,
            ')' )
      ));

      -- convert pairs into a string ('Other' is always last)
      CREATE TEMP FUNCTION pairs_to_string(a ARRAY<STRUCT<str STRING, num FLOAT64>>, format_str STRING)
      RETURNS STRING AS ((
        SELECT
          STRING_AGG(str2,", ")
        FROM (
          SELECT (
            format_result(str,num,format_str)) as str2
            ,rn
          FROM (
            SELECT
              ROW_NUMBER() OVER (ORDER BY CASE WHEN str='Other' THEN -1 ELSE num END DESC) as rn
              , *
            FROM
              UNNEST(a)
          )
          ORDER BY rn
        )
      ));

      -- convert a array to a shortened array with an 'Other'.  Keep the ordering by Num and make other last
      --  by using a row number.
      CREATE TEMP FUNCTION pairs_top_n(a ARRAY<STRUCT<str STRING, num FLOAT64>>, n INT64, use_other BOOL)
      RETURNS ARRAY<STRUCT<str STRING, num FLOAT64>> AS ((
        SELECT
          ARRAY(
            SELECT
              STRUCT(str2 as str ,num2 as num)
            FROM (
              SELECT
                CASE WHEN rn <= n THEN str ELSE 'Other' END as str2
                , CASE WHEN rn <= n THEN n ELSE n + 1 END as n2
                , SUM(num) as num2
              FROM (
                SELECT
                  ROW_NUMBER() OVER() as rn
                  , *
                FROM UNNEST(a)
                ORDER BY num DESC
              )
              GROUP BY 1,2
              ORDER BY 2
            ) as t
            WHERE str2 <> 'Other' or use_other
            ORDER BY n2
          )
      ));


      -- convert pairs to a json string
      CREATE TEMP FUNCTION pairs_to_json(a ARRAY<STRUCT<str STRING, num FLOAT64>>)
      RETURNS STRING
      LANGUAGE js AS """
        return JSON.stringify(a);
      """;

      -- take pairs, sum them and convert to a string
      CREATE TEMP FUNCTION pairs_sum_str(a ARRAY<STRUCT<str STRING, num FLOAT64>>)
      RETURNS STRING AS ((
         pairs_to_string( pairs_sum(a), 'decimal_0' )
      ));

      -- take pairs them sum and convert to a json blob
      CREATE TEMP FUNCTION pairs_sum_graph(a ARRAY<STRUCT<str STRING, num FLOAT64>>)
      RETURNS STRING AS ((
        SELECT
           CONCAT('chl=',STRING_AGG(str,'|'),'&chd=t:',STRING_AGG(FORMAT("%0.0f",num),','))
        FROM (SELECT * FROM UNNEST(pairs_convert_percentage(pairs_sum(a),'total')) ORDER BY str)
      ));

      -- take pairs sum, topn then and convert to a string
      CREATE TEMP FUNCTION pairs_sum_top_n(a ARRAY<STRUCT<str STRING, num FLOAT64>>, n INT64)
      RETURNS STRING AS ((
        pairs_to_string( pairs_top_n(pairs_convert_percentage(pairs_sum(a),'total'), n, true), 'percent_0' )
      ));

      -- Build a decade graph
      CREATE TEMP FUNCTION time_graph(a ARRAY<STRUCT<str STRING, num FLOAT64>>, t INT64)
      RETURNS STRING AS ((
        SELECT
           STRING_AGG(COALESCE(FORMAT("%0.0f",num),"0"))
        FROM (
          SELECT
            *
          FROM
            UNNEST(GENERATE_ARRAY(1910,2013,t)) AS year
            -- zero fill the decades with no data.
            LEFT JOIN UNNEST(pairs_convert_percentage(pairs_sum(a),'max')) as d
              ON d.str=CAST(year as STRING)
          ORDER BY year
        )
      ));

      CREATE TEMP FUNCTION pairs_count_distinct(a ARRAY<STRUCT<str STRING, key STRING>>)
      RETURNS ARRAY<STRUCT<str STRING, num FLOAT64>> AS ((
        SELECT
          ARRAY_AGG(STRUCT(str, num))
        FROM (
           SELECT
              str, CAST(COUNT(*) as FLOAT64) as num
              FROM (
                SELECT
                  a.str, a.key
                FROM UNNEST(a) a
                GROUP BY 1,2
              )
              GROUP BY 1
           )
      ));



      CREATE TEMP FUNCTION list_top_n( a ARRAY<STRUCT<str STRING, key STRING>>, n INT64)
      RETURNS STRING AS ((
        pairs_to_string(
          pairs_top_n(
            pairs_count_distinct(a)
            , n
            , false
          ),'decimal_0'
         )
      ));
  ;;
}
