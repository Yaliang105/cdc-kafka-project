import json
import psycopg2
from confluent_kafka import Consumer
from employee import Employee
from producer import employee_topic_name  # Make sure this matches the producer

class CDCConsumer(Consumer):
    def __init__(self, host="localhost", port="29092", group_id="cdc-group"):
        consumer_config = {
            'bootstrap.servers': f"{host}:{port}",
            'group.id': group_id,
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest'
        }
        super().__init__(consumer_config)
        self.keep_running = True

    def start(self, topics, process_func):
        try:
            self.subscribe(topics)
            print(f"✅ Subscribed to topics: {topics}")
            while self.keep_running:
                msg = self.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"❌ Consumer error: {msg.error()}")
                    continue
                process_func(msg)
        except KeyboardInterrupt:
            print("🛑 Consumer stopped by user.")
        finally:
            self.close()


def process_message(msg):
    try:
        emp_data = json.loads(msg.value())
        employee = Employee(**emp_data)
        action = str(employee.action).lower()
        # Extract timestamp from message
        last_updated_at = emp_data.get('last_updated_at', None)

        conn = psycopg2.connect(
            host="localhost",
            database="employee_dest",
            user="postgres",
            password="postgres",
            port="5433"
        )
        conn.autocommit = True
        cur = conn.cursor()

        if action == 'insert':
            cur.execute("""
                INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary, action, last_updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO NOTHING;
            """, (
                employee.emp_id,
                employee.emp_FN,
                employee.emp_LN,
                employee.emp_dob,
                employee.emp_city,
                employee.emp_salary,
                employee.action,
                last_updated_at
            ))

        elif action.lower() == 'update':
            cur.execute("""
                UPDATE employees
                SET first_name = %s,
                    last_name = %s,
                    dob = %s,
                    city = %s,
                    salary = %s,
                    action = %s,
                    last_updated_at = %s
                WHERE emp_id = %s;
            """, (
                employee.emp_FN,
                employee.emp_LN,
                employee.emp_dob,
                employee.emp_city,
                employee.emp_salary,
                employee.action,
                last_updated_at,
                employee.emp_id
            ))

        elif action == 'delete':
            cur.execute("""
                DELETE FROM employees
                WHERE emp_id = %s;
            """, (employee.emp_id,))
        else:
            print(f"⚠️ Unknown action: {action} for emp_id {employee.emp_id}")
            return

        print(f"✅ {action.upper()} applied for emp_id {employee.emp_id} at {last_updated_at}")

    except Exception as e:
        print(f"❌ Processing error: {e}")
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    try:
        consumer = CDCConsumer()
        consumer.start([employee_topic_name], process_message)
    except Exception as e:
        print(f"❌ Failed to start consumer: {e}")

