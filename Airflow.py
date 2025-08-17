# Exécuter la commande ci-dessous dans le terminal pour lister tous les DAGs existants.
airflow dags list

# Exécuter la commande ci-dessous pour lister toutes les tâches du DAG nommé example_bash_operator.
airflow tasks list example_bash_operator

# Exécuter une commande pour lister toutes les tâches du DAG nommé tutorial.
airflow tasks list tutorial

# Exécuter la commande ci-dessous pour activer (unpause) le DAG nommé tutorial.
airflow dags unpause tutorial

# Exécuter la commande pour mettre en pause le DAG (pause).
airflow dags pause tutorial

# Exécuter une commande pour activer (unpause) le DAG nommé example_branch_operator.
airflow dags unpause example_branch_operator

# Lister les tâches du DAG nommé example_branch_labels.
airflow tasks list example_branch_labels

# Activer (unpause) le DAG example_branch_labels.
airflow dags unpause example_branch_labels

# Mettre en pause le DAG example_branch_labels.
airflow dags pause example_branch_labels
