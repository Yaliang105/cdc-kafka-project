import json
import psycopg2
from confluent_kafka import Consumer, Producer
from employee import Employee
from producer import employee_topic_name

# ✅ DLQ Producer (global)
dlq_producer = Producer({'bootstrap.servers': 'kafka:9092'})
dlq_topic = 'bf_employee_cdc_dlq'

class ShortBurstCDCConsumer(Consumer):
    def __init__(self, host="kafka", port="9092", group_id="cdc-group"):
        consumer_config = {
            'bootstrap.servers': f"{host}:{port}",
            'group.id': group_id,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest'
        }
        super().__init__(consumer_config)

    def run_once(self, topics, process_func, max_messages=50, timeout_sec=5):
        self.subscribe(topics)
        print(f"✅ Subscribed to {topics}")
        messages_processed = 0

        while messages_processed < max_messages:
            msg = self.poll(timeout_sec)
            if msg is None:
                break
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            process_func(msg)
            messages_processed += 1

        self.close()
        print(f"✅ Consumer finished after processing {messages_processed} messages.")


def process_message(msg):
    try:
        emp_data = json.loads(msg.value())
        employee = Employee(**emp_data)
        action = str(employee.action).lower()
        last_updated_at = emp_data.get('last_updated_at', None)

        dob_year = int(str(employee.emp_dob)[:4]) if employee.emp_dob else 0
        invalid = dob_year <= 2007 or employee.emp_salary <= 100 or employee.emp_id < 0

        if invalid:
            dlq_producer.produce(
                topic=dlq_topic,
                key=str(employee.emp_id),
                value=json.dumps(emp_data)
            )
            dlq_producer.flush()
            print(f"⚠️ Sent invalid data to DLQ for emp_id {employee.emp_id}")
            return

        conn = psycopg2.connect(
            host="db_dst",
            database="employee_dest",
            user="postgres",
            password="postgres",
            port="5432"
        )
        conn.autocommit = True
        cur = conn.cursor()

        if action == 'insert':
            cur.execute("""
                INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary, action, last_updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO NOTHING;
            """, (
                employee.emp_id, employee.emp_FN, employee.emp_LN, employee.emp_dob,
                employee.emp_city, employee.emp_salary, employee.action, last_updated_at
            ))

        elif action == 'update':
            cur.execute("""
                UPDATE employees
                SET first_name = %s, last_name = %s, dob = %s, city = %s, salary = %s, action = %s, last_updated_at = %s
                WHERE emp_id = %s;
            """, (
                employee.emp_FN, employee.emp_LN, employee.emp_dob, employee.emp_city,
                employee.emp_salary, employee.action, last_updated_at, employee.emp_id
            ))

        elif action == 'delete':
            cur.execute("DELETE FROM employees WHERE emp_id = %s;", (employee.emp_id,))

        print(f"✅ {action.upper()} processed for emp_id {employee.emp_id}")

    except Exception as e:
        print(f"❌ Processing error: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass


if __name__ == '__main__':
    consumer = ShortBurstCDCConsumer()
    consumer.run_once([employee_topic_name], process_message, max_messages=100, timeout_sec=5)
