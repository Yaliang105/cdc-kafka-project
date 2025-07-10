import psycopg2
import json
from confluent_kafka import Producer
from employee import Employee

employee_topic_name = "bf_employee_cdc"

class cdcProducer(Producer):
    def __init__(self, host="kafka", port="9092"):
        producer_config = {
            'bootstrap.servers': f"{host}:{port}",
            'acks': 'all'
        }
        super().__init__(producer_config)
        self.last_processed_timestamp = '2000-01-01 00:00:00'

    def fetch_cdc(self):
        rows = []
        try:
            conn = psycopg2.connect(
                host="db_source",
                database="employee_source",
                user="postgres",
                port='5432',
                password="postgres"
            )
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute("""
                SELECT emp_id, first_name, last_name, dob, city, action, salary, last_updated_at
                FROM emp_cdc
                WHERE last_updated_at > %s
                ORDER BY last_updated_at ASC;
            """, (self.last_processed_timestamp,))

            rows = cur.fetchall()
            cur.close()
            conn.close()

        except Exception as err:
            print(f"❌ Error fetching CDC: {err}")

        return rows

def main():
    producer = cdcProducer()
    cdc_rows = producer.fetch_cdc()

    for row in cdc_rows:
        employee = Employee(
            action_id=0,
            emp_id=row[0],
            emp_FN=row[1],
            emp_LN=row[2],
            emp_dob=str(row[3]),
            emp_city=row[4],
            emp_salary=row[6],
            action=row[5]
        )

        message_dict = json.loads(employee.to_json())
        message_dict['last_updated_at'] = str(row[7])

        message = json.dumps(message_dict)

        producer.produce(
            topic=employee_topic_name,
            key=str(employee.emp_id),
            value=message
        )

        print(f"✅ Sent to Kafka: {message}")
        producer.last_processed_timestamp = str(row[7])

    producer.flush()

if __name__ == '__main__':
    main()
