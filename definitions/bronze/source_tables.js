[
    'badges',
    'posts_answers',
    'posts_questions',
    'users'
].forEach(source => declare({
      schema: `${dataform.projectConfig.vars.bronze_dataset_name}`,
      name: source,
      description: "Tables Bronze",
}));
