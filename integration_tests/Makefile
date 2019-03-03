
test-postgres:
	dbt seed --target postgres --full-refresh
	dbt run --target postgres --full-refresh --exclude test_insert_by_period
	dbt test --target postgres --exclude test_insert_by_period

test-all: test-postgres 
	echo "Completed successfully"
