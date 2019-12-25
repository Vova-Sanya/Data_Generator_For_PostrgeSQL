import os
import time
import uuid
import random
import psycopg2
import threading
import concurrent.futures
from dotenv import load_dotenv
from datetime import datetime, timedelta
from psycopg2.extras import execute_batch

MAX_USERS = 500000
MAX_CATEGORIES = 5000
MAX_MESSAGES = 10000000

T_START = datetime(2010, 1, 1, 00, 00, 00)
T_END = T_START + timedelta(days=365 * 10)

load_dotenv(dotenv_path='.env')


def timeit(method):
    def timed(*args, **kwargs):
        time_start = time.time()
        result = method(*args, **kwargs)
        time_end = time.time()
        print(method.__name__, ' for ', get_work_time(time_start, time_end))
        return result

    return timed


def get_work_time(time_start, time_end):
    total_time = time_end - time_start
    total_time_int = int(total_time)
    total_time_float = total_time - total_time_int
    min_availability = False
    if total_time_int > 59:
        total_time_minutes = total_time_int // 60
        total_time_int -= total_time_minutes * 60
        min_availability = True
    if min_availability:
        time_string = "%s минут, %s секунд, %s миллисекунд" % (total_time_minutes, total_time_int, total_time_float)
    else:
        time_string = "%s секунд, %s миллисекунд" % (total_time_int, total_time_float)
    return time_string


@timeit
def author_uuid_creation():
    author_uuid = [str(uuid.uuid4()) for i in range(MAX_USERS)]
    return author_uuid


@timeit
def category_uuid_creation():
    category_uuid = [str(uuid.uuid4()) for i in range(MAX_CATEGORIES)]
    return category_uuid


@timeit
def message_uuid_creation():
    message_uuid = [str(uuid.uuid4()) for i in range(MAX_MESSAGES)]
    return message_uuid


@timeit
def containing_users_list(author_uuid):
    for i in range(MAX_USERS):
        yield author_uuid[i], 'User_' + str(i + 1)


@timeit
def containing_categories_list(category_uuid):
    for i in range(MAX_CATEGORIES):
        yield category_uuid[i], 'Category_' + str(i + 1), category_uuid[i]


@timeit
def containing_messages_list(message_uuid, category_uuid, author_uuid):
    for i in range(MAX_MESSAGES):
        yield message_uuid[i], 'Text_' + str(i + 1), random.choice(category_uuid), T_START + (
                T_END - T_START) * random.random(), random.choice(author_uuid)


@timeit
def data_base_connecting():
    con = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT"))
    cur = con.cursor()
    return con, cur


@timeit
def inserting_users_data(cur, author_uuid):
    cur.execute("PREPARE us AS INSERT INTO users VALUES($1, $2)")
    containing_function = containing_users_list(author_uuid)
    execute_batch(cur, "EXECUTE us (%s, %s)", iter(containing_function))


@timeit
def inserting_categories_data(cur, category_uuid):
    cur.execute("PREPARE cat AS INSERT INTO categories VALUES($1, $2, $3)")
    containing_function = containing_categories_list(category_uuid)
    execute_batch(cur, "EXECUTE cat (%s, %s, %s)", iter(containing_function))


@timeit
def inserting_messages_data(cur, message_uuid, category_uuid, author_uuid):
    cur.execute("PREPARE mes AS INSERT INTO messages VALUES($1, $2, $3, $4, $5)")
    containing_function = containing_messages_list(message_uuid, category_uuid, author_uuid)
    execute_batch(cur, "EXECUTE mes (%s, %s, %s, %s, %s)", iter(containing_function))


@timeit
def commit_and_close(con, cur):
    con.commit()
    cur.close()
    con.close()


@timeit
def main():
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        thread_1 = executor.submit(author_uuid_creation)
        thread_2 = executor.submit(category_uuid_creation)
        thread_3 = executor.submit(message_uuid_creation)
        author_uuid = thread_1.result()
        category_uuid = thread_2.result()
        message_uuid = thread_3.result()

    con, cur = data_base_connecting()

    thread_1 = threading.Thread(target=inserting_users_data, args=(cur, author_uuid))
    thread_2 = threading.Thread(target=inserting_categories_data, args=(cur, category_uuid))
    thread_3 = threading.Thread(target=inserting_messages_data, args=(cur, message_uuid, category_uuid, author_uuid))
    thread_1.start()
    thread_2.start()
    thread_1.join()
    thread_2.join()
    thread_3.start()
    thread_3.join()

    commit_and_close(con, cur)


main()
