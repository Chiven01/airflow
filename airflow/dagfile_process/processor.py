# -*- coding: utf-8 -*-

import os
import pika
import pickle
import signal
import os

from celery import Celery
from airflow import configuration as conf
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowTaskTimeout


class PikaTimeout():
    """
    To be used in a ``with`` block and timeout its content.
    """

    def __init__(self, log, channel, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.log = log
        self.channel = channel
        self.error_message = error_message + ', PID: ' + str(os.getpid())

    def handle_timeout(self, signum, frame):
        self.log.error("Process timed out, PID: %s", str(os.getpid()))
        self.channel.stop_consuming()
        raise AirflowTaskTimeout(self.error_message)

    def __enter__(self):
        try:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)

    def __exit__(self, type, value, traceback):
        try:
            signal.alarm(0)
        except ValueError as e:
            self.log.warning("timeout can't be used in the current context")
            self.log.exception(e)


QUEUE = 'python_conn_test'

def get_connection():
    HOST = '10.142.106.205'
    PORT = '5672'
    USER = 'airflow'
    PASS = 'airflow'

    #need to process ConnectionBlockedTimeout
    conn = pika.BlockingConnection(
           pika.ConnectionParameters(HOST,
                                     PORT,
                                     credentials=pika.PlainCredentials(USER, PASS),
                                     blocked_connection_timeout=2))
    return conn

def productor(body):
    conn = get_connection()
    channel = conn.channel()
    channel.queue_declare(queue=QUEUE)
    channel.basic_publish(exchange='',
                          routing_key=QUEUE,
                          body=body)
    conn.close()


def consumer(log):
    results = []
    conn = get_connection()
    channel = conn.channel()
    channel.queue_declare(queue=QUEUE)

    def callback(ch, method, properties, body):
        log.debug("Get one message, size is %d.", len(body))
        results.append(body)

    channel.basic_consume(QUEUE,
                          callback,
                          auto_ack=True)
    try:
        with PikaTimeout(log, channel, seconds=1):
            channel.start_consuming()
    except AirflowTaskTimeout as e:
        log.debug("Spend 1 seconds receiving %d simpledag_str", len(results))
    except Exception as e:
        log.debug("Unexpected exception occur.")
        log.error(e)

    if not conn.is_closed:
        conn.close()
    return results

section = "dagfileprocessor_celery"
broker_url = conf.get(section, 'BROKER_URL')
broker_transport_options = conf.getsection('celery_broker_transport_options')
celery_config = {
    'accept_content': ['json', 'pickle'],
    'event_serializer': 'json',
    'worker_prefetch_multiplier': 1,
    'task_acks_late': True,
    'task_default_queue': conf.get(section, 'DEFAULT_QUEUE'),
    'task_default_exchange': conf.get(section, 'DEFAULT_QUEUE'),
    'broker_url': broker_url,
    'broker_transport_options': broker_transport_options,
    'result_backend': conf.get(section, 'RESULT_BACKEND'),
    'worker_concurrency': conf.getint(section, 'WORKER_CONCURRENCY'),
    'result_expires': conf.getint(section, 'RESULT_EXPIRES'),
}
app = Celery(
    conf.get(section, 'CELERY_APP_NAME'), config_source=celery_config)

@app.task
def file_processor(do_pickle, dag_ids, file_tag, dag_contents):
    log = LoggingMixin().log

    if dag_contents and isinstance(dag_contents, dict):
        dag_folder = conf.get('core', 'DAGS_FOLDER')
        if not os.path.exists(dag_folder):
            os.makedirs(dag_folder)

        for name,content in dag_contents.items():
            file_path = os.path.join(dag_folder, name)
            with open(file_path, 'w', encoding='utf8') as dagfile:
                dagfile.writelines(content)
            log.debug("dag_contents have been saved in %s", file_path)
    else:
        log.error("args.dag_contents is not dict")
        return

    from airflow import jobs, settings
    settings.configure_orm()
    scheduler_job = jobs.SchedulerJob(dag_ids=dag_ids, log=log)
    simple_dags = scheduler_job.process_file(file_path, do_pickle)
    log.info("File %s's processing has finished , send back simpledags now.", file_path)

    results = {}
    results[file_tag] = simple_dags

    try:
        body = pickle.dumps(results)
        productor(body)
        log.debug("Successed when send samepledags to rabbitmq, file %s." , file_path)
    except:
        log.debug("Failed when send samepledags to rabbitmq, file %s." , file_path)



