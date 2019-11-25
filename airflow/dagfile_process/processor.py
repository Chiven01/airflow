# -*- coding: utf-8 -*-

import os
import pickle

from celery import Celery
from airflow import configuration as conf
from airflow.utils.log.logging_mixin import LoggingMixin

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
def file_processor(do_pickle, dag_ids, file_changed, dag_contents):
    #todo(chiven): Do not use 'return' to exit, beacause sometimes it will be ambiguous
    log = LoggingMixin().log

    if dag_contents and isinstance(dag_contents, dict):
        dag_folder = conf.get('core', 'DAGS_FOLDER')
        if not os.path.exists(dag_folder):
            os.makedirs(dag_folder)

        dagbag_folder = conf.get('core', 'DAGBAGS_FOLDER')
        if not os.path.exists(dagbag_folder):
            os.makedirs(dagbag_folder)

        for name,content in dag_contents.items():
            file_path = os.path.join(dag_folder, name)
            with open(file_path, 'w', encoding='utf8') as dagfile:
                dagfile.writelines(content)
            log.debug("Dag_contents have been saved in %s", file_path)

            mod_name, _ = os.path.splitext(name)
            dagbag_path = os.path.join(dagbag_folder, ''.join([mod_name, '.dagbag']))
            log.debug("Getting dagbag file %s", dagbag_path)
    else:
        log.error("args.dag_contents is not dict")
        return

    from airflow import jobs, settings
    settings.configure_orm()
    scheduler_job = jobs.SchedulerJob(dag_ids=dag_ids, log=log)
    dags = scheduler_job.process_file(file_path, dagbag_path, file_changed, do_pickle)
    log.info("File %s's processing has finished.", file_path)

    if len(dags) > 0:
        # Handle cases where a DAG run state is set (perhaps manually) to
        # a non-running state. Handle task instances that belong to
        # DAG runs in those states

        # If a task instance is up for retry but the corresponding DAG run
        # isn't running, mark the task instance as FAILED so we don't try
        # to re-run it.
        from airflow.utils.state import State
        scheduler_job._change_state_for_tis_without_dagrun(dags,
                                                  [State.UP_FOR_RETRY],
                                                  State.FAILED)

        # If a task instance is scheduled or queued or up for reschedule,
        # but the corresponding DAG run isn't running, set the state to
        # NONE so we don't try to re-run it.
        scheduler_job._change_state_for_tis_without_dagrun(dags,
                                                   [State.QUEUED,
                                                   State.SCHEDULED,
                                                   State.UP_FOR_RESCHEDULE],
                                                  State.NONE)




