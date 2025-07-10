from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Arlen',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='project2_cdc_pipeline_full',
    default_args=default_args,
    description='ETL: Source ➔ Kafka ➔ Destination using short-burst Producer & Consumer',
    schedule_interval=None,
    catchup=False,
) as dag:

    # Step 1: Create Source Tables with Trigger
    create_source_tables = PostgresOperator(
        task_id='create_source_tables',
        postgres_conn_id='postgres_source',
        sql="""
        CREATE TABLE IF NOT EXISTS employees (
            emp_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            dob DATE,
            city VARCHAR(100),
            salary INT
        );

        CREATE TABLE IF NOT EXISTS emp_cdc (
            emp_id INT,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            dob DATE,
            city VARCHAR(100),
            action VARCHAR(100),
            salary INT,
            last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE OR REPLACE FUNCTION employee_cdc_trigger_func()
        RETURNS TRIGGER AS $$
        BEGIN
            IF (TG_OP = 'INSERT') THEN
                INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action, last_updated_at)
                VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'insert', CURRENT_TIMESTAMP);
                RETURN NEW;

            ELSIF (TG_OP = 'UPDATE') THEN
                INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action, last_updated_at)
                VALUES (NEW.emp_id, NEW.first_name, NEW.last_name, NEW.dob, NEW.city, NEW.salary, 'update', CURRENT_TIMESTAMP);
                RETURN NEW;

            ELSIF (TG_OP = 'DELETE') THEN
                INSERT INTO emp_cdc(emp_id, first_name, last_name, dob, city, salary, action, last_updated_at)
                VALUES (OLD.emp_id, OLD.first_name, OLD.last_name, OLD.dob, OLD.city, OLD.salary, 'delete', CURRENT_TIMESTAMP);
                RETURN OLD;
            END IF;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS employee_cdc_trigger ON employees;

        CREATE TRIGGER employee_cdc_trigger
        AFTER INSERT OR UPDATE OR DELETE
        ON employees
        FOR EACH ROW
        EXECUTE FUNCTION employee_cdc_trigger_func();
        """
    )

    # Step 1b: Create Destination Table
    create_dest_table = PostgresOperator(
        task_id='create_dest_table',
        postgres_conn_id='postgres_dest',
        sql="""
        CREATE TABLE IF NOT EXISTS employees (
            emp_id INT PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            dob DATE,
            city VARCHAR(100),
            salary INT,
            action VARCHAR(100),
            last_updated_at TIMESTAMP
        );
        """
    )

    # Step 2: Insert Test Data into Source
    insert_test_data = PostgresOperator(
        task_id='insert_test_data',
        postgres_conn_id='postgres_source',
        sql="""
        -- Valid Inserts
        INSERT INTO employees (first_name, last_name, dob, city, salary)
        VALUES ('Alice', 'Johnson', '2012-04-10', 'San Francisco', 80000);

        INSERT INTO employees (first_name, last_name, dob, city, salary)
        VALUES ('Emma', 'Wood', '2015-07-01', 'Boston', 72000);

        -- Invalid Inserts (Should go to DLQ)
        INSERT INTO employees (first_name, last_name, dob, city, salary)
        VALUES ('Too', 'Low', '2000-01-01', 'LA', 50);

        INSERT INTO employees (first_name, last_name, dob, city, salary)
        VALUES ('Too', 'Old', '1900-01-01', 'San Jose', 50000);

        INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
        VALUES (-100, 'Wrong', 'ID', '2000-01-01', 'Chicago', 45000);

        -- Valid Update
        UPDATE employees
        SET city = 'San Francisco', salary = 75000
        WHERE emp_id = 1;

        -- Valid Delete
        DELETE FROM employees
        WHERE emp_id = 1;
        """
    )

    # Step 3: Run Producer (Short Burst)
    run_producer = BashOperator(
        task_id='run_producer',
        bash_command='python /opt/airflow/project2/producer.py',
    )

    # Step 4: Run Consumer (Short Burst)
    run_consumer = BashOperator(
        task_id='run_consumer',
        bash_command='python /opt/airflow/project2/consumer.py',
    )

    # Step 5: Check Destination Table
    check_destination = PostgresOperator(
        task_id='check_destination',
        postgres_conn_id='postgres_dest',
        sql='SELECT * FROM employees;',
    )

    # Define Execution Flow
    [create_source_tables, create_dest_table] >> insert_test_data >> run_producer >> run_consumer >> check_destination
