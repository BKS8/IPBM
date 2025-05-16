import logging
import mysql.connector
import random
import datetime
import pytz
import azure.functions as func

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 * * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)
def timer_trigger1(myTimer: func.TimerRequest) -> None:
    connection = None
    cursor = None 
    try:
        if myTimer.past_due:
            logging.info('The timer is past due!')

        logging.info('Python timer trigger function executed.')

        utc_now = datetime.datetime.utcnow()
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_now = utc_now.astimezone(ist_tz)
        
        order_hour = ist_now.hour
        order_date = ist_now.date()

        logging.info(f"Current IST time: {ist_now}. Order hour: {order_hour}, Order date: {order_date}")

        if order_hour < 10 or order_hour > 23:
            logging.info(f"Order hour {order_hour} is outside the valid range. Skipping execution.")
            return

        hour_bucket = {
            10: range(0, 4),
            11: range(4, 11),
            12: range(14, 21),
            13: range(13, 20),
            14: range(6, 13),
            15: range(5, 12),
            16: range(8, 15),
            17: range(10, 17),
            18: range(11, 18),
            19: range(8, 15),
            20: range(6, 13),
            21: range(3, 10),
            22: range(0, 7),
            23: range(0, 4),
        }

        logging.info('Connecting to the database...')
        connection = mysql.connector.connect(
            host="pizza-sales.mysql.database.azure.com",
            user="************",
            password="**********",
            database="pizza_details"
        )
        cursor = connection.cursor()
        logging.info('Database connection established.')

        # Helper functions
        def get_random_order_time_today():
            start_time = datetime.datetime.combine(order_date, datetime.time(order_hour, 0))
            end_time = datetime.datetime.combine(order_date, datetime.time(order_hour + 1, 0))
            random_time = start_time + datetime.timedelta(
                seconds=random.randint(0, int((end_time - start_time).total_seconds()))
            )
            return random_time.strftime('%H:%M:%S')

        def get_random_pizza_id():
            logging.info("Fetching a random pizza ID...")
            max_retries = 89
            retries = 0
            while retries < max_retries:
                cursor.execute("SELECT pizza_id FROM products WHERE instock > 0")
                pizzas = cursor.fetchall()
                if pizzas:
                    return random.choice(pizzas)[0]
                retries += 1
            logging.warning("Out of stock for all pizzas.")
            return None

        def get_random_customer_id():
            logging.info("Fetching a random customer ID...")
            customer_choice = random.choices(['existing', 'new'], weights=[70, 30], k=1)[0]
            if customer_choice == 'existing':
                cursor.execute("SELECT customer_id FROM customers ORDER BY RAND() LIMIT 1")
            else:
                cursor.execute("SELECT customer_id FROM new_customers ORDER BY RAND() LIMIT 1")
            result = cursor.fetchone()
            return result[0] if result else None

        def get_random_quantity():
            return random.choices([1, 2, 3], weights=[98.09, 1.86, 0.05], k=1)[0]

        remaining_records = random.choice(hour_bucket[order_hour])
        logging.info(f"Remaining records to process: {remaining_records}")
        cursor.execute("SELECT MAX(order_id) FROM sales")
        last_order_id = cursor.fetchone()[0] or 0
        sales_records = []

        while remaining_records > 0:
            last_order_id += 1
            order_id = last_order_id
            customer_id = get_random_customer_id()
            if not customer_id:
                continue

            num_records_for_order = min(
                random.choices([1, 2, 3, 4, 5], weights=[4.0, 3.0, 1.5, 1.0, 0.5], k=1)[0],
                remaining_records
            )

            for _ in range(num_records_for_order):
                pizza_id = get_random_pizza_id()
                if pizza_id is None:
                    continue
                quantity = get_random_quantity()
                cursor.execute("SELECT unit_price FROM products WHERE pizza_id = %s", (pizza_id,))
                unit_price = cursor.fetchone()[0]
                total_price = unit_price * quantity
                order_time = get_random_order_time_today()
                sales_records.append((order_id, customer_id, pizza_id, quantity, order_date, order_time, total_price))
                cursor.execute("UPDATE products SET instock = instock - %s WHERE pizza_id = %s", (quantity, pizza_id))

            remaining_records -= num_records_for_order

        try:
            cursor.executemany("""
                INSERT INTO sales (order_id, customer_id, pizza_id, quantity, order_date, order_time, total_price)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, sales_records)
            connection.commit()
        except mysql.connector.Error as err:
            logging.error(f"Error inserting records: {err}")
            connection.rollback()

        cursor.execute("SELECT customer_id FROM sales WHERE customer_id NOT IN (SELECT customer_id FROM customers)")
        new_customers = cursor.fetchall()

        for customer in new_customers:
            cursor.execute("""
                SELECT customer_id, gender, name, age, phone, email
                FROM new_customers
                WHERE customer_id = %s
            """, (customer[0],))
            new_customer_data = cursor.fetchone()

            cursor.execute("SELECT COUNT(*) FROM customers WHERE customer_id = %s", (customer[0],))
            if cursor.fetchone()[0] == 0:
                cursor.execute("""
                    INSERT INTO customers (customer_id, gender, first_name, age, phone, email)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, new_customer_data)
                connection.commit()

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    except Exception as e:
        logging.error("An unexpected error occurred", exc_info=True)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    logging.info('Function completed.')
