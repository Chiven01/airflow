# -*- coding: utf-8 -*-

import os
import pika
import json

from celery import Celery
from airflow import configuration as conf
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow import jobs, settings

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

@app.task
def file_processor(do_pickle, dag_ids, dag_contents):
    log = LoggingMixin().log

    if dag_contents and isinstance(dag_contents, dict):
        dag_folder = conf.get('dagfileprocessor', 'DAGFILES_FOLDER')
        if not os.path.exists(dag_folder):
            os.makedirs(dag_folder)

        for k,v in dag_contents.items():
            file_path = os.path.join(dag_folder, k)
            with open(file_path, 'w', encoding='utf8') as dagfile:
                dagfile.writelines(v)
            log.debug("dag_contents have been saved in %s", file_path)
    else:
        log.error("args.dag_contents is not dict")
        return

    settings.configure_orm()
    scheduler_job = jobs.SchedulerJob(dag_ids=dag_ids, log=log)
    results = scheduler_job.process_file(file_path, do_pickle)
    log.info("File %s's processing has finished , send back simpledags now.", file_path)

    try:
        for result in results:
            body = json.dumps(result.__dict__)
            productor(body)
        log.debug("Successed when send samepledags to rabbitmq.")
    except:
        log.debug("Failed when send samepledags to rabbitmq.")



