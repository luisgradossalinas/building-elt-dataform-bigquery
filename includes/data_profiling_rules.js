function default_select( table_name, data_type,column_name, profile_rule){
    return `
            SELECT
              "${table_name}" AS table_name,
              "${data_type}" AS data_type,
              "${column_name}" AS column_name,
              "${profile_rule.name}" AS rule_name,
              CURRENT_TIMESTAMP() AS executed_on,`;
}

function resolve_profile_rule(table_name, data_type, column_name, profile_rule){
    switch(profile_rule.name) {
        case "NOT_NULL":
          return not_null_rule(table_name, data_type, column_name, profile_rule);
        case "NULL":
          return null_rule(table_name, data_type, column_name, profile_rule);
        case "DUPLICATE_COUNT":
          return duplicate_rule(table_name,data_type,column_name,profile_rule);
        case "UNIQUE_COUNT":
            return unique_rule(table_name,data_type,column_name,profile_rule);
        case "MAX":
            return max_rule(table_name,data_type,column_name,profile_rule);
        case "MIN":
            return min_rule(table_name,data_type,column_name,profile_rule);
        case "SUM":
            return sum_rule(table_name,data_type,column_name,profile_rule);
        case "STANDARD_DEVIATION":
            return standard_deviation_rule(table_name,data_type,column_name,profile_rule);
        case "VARIANCE":
            return variance_rule(table_name,data_type,column_name,profile_rule);
        case "MAX_LENGTH":
            return max_length_rule(table_name,data_type,column_name,profile_rule);
        case "MIN_LENGTH":
            return min_length_rule(table_name,data_type,column_name,profile_rule);
        case "AVERAGE_LENGTH":
            return average_length_rule(table_name,data_type,column_name,profile_rule);

    }
    return null;
}

/////////////////////////////////////////// COMMON RULES /////////////////////////////////////////
//Duplicate
function duplicate_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
              COUNT(1) AS result
            FROM (
                SELECT
                  ${column_name},
                  COUNT(1) AS occurrences
                FROM ${table_name}
                GROUP BY ${column_name}
                HAVING occurrences > 1
            )
            WHERE ${column_name} IS NOT NULL`;
}

//MAX
function max_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
            MAX(${column_name})  AS result
            FROM ${table_name} `;
}

//MIN
function min_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
            MIN(${column_name})  AS result
            FROM ${table_name}`;
}

//NOT NULL
function not_null_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
              COUNT(1) AS result
             FROM ${table_name}
             WHERE ${column_name} IS NOT NULL`;
  }

//NULL
function null_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
              COUNT(1) AS result
            FROM ${table_name}
            WHERE ${column_name} IS NULL`;
}

/////////////////////////////////////////// NUMERIC RULES ////////////////////////////////////////
//SUM
function sum_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
                SUM(CAST(${column_name} AS NUMERIC))  AS result
                FROM ${table_name}`;
}

//UNIQUE
function unique_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
             COUNT(DISTINCT ${column_name})  AS result
             FROM ${table_name}`
}

//STANDARD DEVIATION
function standard_deviation_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
             STDDEV(DISTINCT ${column_name})  AS result
             FROM ${table_name}`
}


//VARIANCE
function variance_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
             VARIANCE(DISTINCT ${column_name})  AS result
             FROM ${table_name}`
}


/////////////////////////////////////////// NON-CATEGORICAL RULES ////////////////////////////////////////
//MAX_LENGTH
function max_length_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
                MAX(char_length( ${column_name}))   AS result
                FROM ${table_name}`;
}

//MIN_LENGTH
function min_length_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
                MIN(char_length( ${column_name}))   AS result
                FROM ${table_name}`;
}

//AVERAGE_LENGTH
function average_length_rule(table_name,data_type,column_name,profile_rule){
    return ` ${default_select(table_name,data_type,column_name,profile_rule)}
                AVG(Distinct char_length( ${column_name}))   AS result
                FROM ${table_name}`;
}

module.exports = {
    resolve_profile_rule,
    default_select
}
  