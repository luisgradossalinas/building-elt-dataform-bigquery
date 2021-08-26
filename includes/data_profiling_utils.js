const rules = require("includes/data_profiling_rules.js")

// Default Data Profiling Rules
const default_rules = [
  {
    "data_type": "Categorical",
    "rules":[
        { "name": "NULL"},
        { "name": "NOT_NULL"},
        { "name": "UNIQUE_COUNT"},
        { "name": "DUPLICATE_COUNT"},
        { "name": "MAX_LENGTH"},
        { "name": "MIN_LENGTH"},
        { "name": "AVERAGE_LENGTH"},
  ]}, {
    "data_type": "Numeric",
    "rules":[
        { "name": "NULL"},
        { "name": "NOT_NULL"},
        { "name": "UNIQUE_COUNT"},
        { "name": "DUPLICATE_COUNT"},
        { "name": "SUM"},
        { "name": "MAX"},
        { "name": "MIN"},
        { "name": "STANDARD_DEVIATION"},
        { "name": "VARIANCE"},
   ]}, {
    "data_type": "Date",
    "rules":[
        { "name": "NULL"},
        { "name": "NOT_NULL"},
        { "name": "UNIQUE_COUNT"},
        { "name": "DUPLICATE_COUNT"}
    ]}
]

const data_profiling_table_name = `${dataform.projectConfig.defaultDatabase}.${dataform.projectConfig.vars.dprofile_dataset_name}.data_profiling`

function generate_profiling_results_table(data_profiles){
    create_profiling_table_if_not_exists()
    data_profiles.forEach(table => {
        const column_rules = [];
        let table_name = table.table_name;
        let columns = table.columns;
        columns.forEach ( column => {
          let column_name = column.column_name;
          let data_type = column.data_type;
          let profile_rules = column.rules != null && column.rules != undefined ? column.rules : get_default_rules(data_type);

          profile_rules.forEach( rule => {
            column_rules.push({"table_name":table_name,"data_type":data_type,"column_name":column_name,"rule":rule});
          });
       })
    applyRules(table_name.split('.')[2], column_rules);
    })
  }

function applyRules(table_name, column_rules){
  operate(`${table_name}_data_profiling`)
      .tags(["data_profiling"])
      .queries(generate_select(column_rules))
}

function get_default_rules (data_type){
  let data_rule = default_rules.find(rule => rule.data_type === data_type);
 return data_rule != null ? data_rule.rules : []
}

function generate_select(column_rules){
  let selects = [];
  column_rules.forEach(profile_rule => {
      selects.push(rules.resolve_profile_rule(profile_rule.table_name, profile_rule.data_type, profile_rule.column_name, profile_rule.rule));
  });
  return `
    INSERT INTO ${data_profiling_table_name}
    ${selects.join('\nUNION ALL\n')}
  `;
}

function create_profiling_table_if_not_exists(){
    // Run the following to create the data profiling table:
    //   dataform run --tags create_data_profiling_table
    operate('create_data_profiling_table_if_not_exists')
      .tags(["create_data_profiling_table"])
      .queries(`
        CREATE TABLE IF NOT EXISTS ${data_profiling_table_name}
        (
          table_name STRING,
          data_type STRING,
          column_name STRING,
          rule_name STRING,
          executed_on TIMESTAMP,
          result FLOAT64
        ) OPTIONS(
          description="Table that holds data profiling results",
          labels=[("dataform_data_profiling", "data_profiling")]
        )
      `);
}

module.exports = {
    generate_profiling_results_table,
}