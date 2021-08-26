const {generate_profiling_results_table} = data_profiling_utils;

let data_profiles = [
    {
        "table_name" : `${dataform.projectConfig.defaultDatabase}.${dataform.projectConfig.vars.bronze_dataset_name}.badges`,

        "columns" : [
            {
                "column_name" : "name",
                "data_type" : "Categorical"
            },
            {
                "column_name" : "class",
                "data_type" : "Numeric"
            }
        ]
    }   
];

generate_profiling_results_table(data_profiles);