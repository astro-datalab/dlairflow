# Licensed under a BSD-style 3-clause license - see LICENSE.md.
# -*- coding: utf-8 -*-
"""Test tasks defined in dlairflow.meta.
"""
import pytest
import os
import shutil
import json
import logging
import pendulum
import tempfile
from importlib import import_module
from importlib.resources import files
has_felis = True
try:
    from felis import Schema
except ImportError:
    has_felis = False


def setup_airflow_home():
    """Create a temporary ``AIRFLOW_HOME``.
    """
    airflow_home = tempfile.mkdtemp()
    os.makedirs(os.path.join(airflow_home, 'dags'), exist_ok=True)
    bundle = [{"name": "test",
               "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
               "kwargs": {"path": str(os.path.realpath(__file__))}}]
    os.environ['AIRFLOW_HOME'] = airflow_home
    os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'
    os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps(bundle)


def cleanup_airflow_home():
    """Remove the temporary ``AIRFLOW_HOME``.
    """
    airflow_home = os.environ['AIRFLOW_HOME']
    del os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"]
    del os.environ['AIRFLOW__CORE__UNIT_TEST_MODE']
    del os.environ['AIRFLOW_HOME']
    shutil.rmtree(airflow_home)


def create():
    """Create a DAG that will contain the tasks to be tested.
    """
    setup_airflow_home()
    from airflow.utils.db import upgradedb
    try:
        from airflow.sdk import DAG
    except ImportError:
        from airflow import DAG
    upgradedb()
    p = import_module('..meta', package='dlairflow.test')
    validate_schema_file_task = p.__dict__['validate_schema_file_task']
    with DAG(dag_id="test_validate_schema_file_task_dag",
             schedule=None, catchup=False,
             start_date=pendulum.datetime(2026, 1, 1, tz="UTC")) as dag:
        validate_schema_file_task(files('dlairflow.test') / 't' / 'test_validate_data_files.yml',
                                  check_description=False,
                                  check_redundant_datatypes=False,
                                  check_tap_table_indexes=False,
                                  check_tap_principal=False)
    return dag


dag = create()


def test_validate_schema_file_task(caplog):  # noqa: F811
    """Test the task wrapper for validate_schema_file.
    """
    if not has_felis:
        pytest.skip("Felis is not installed in the environment.")
    caplog.set_level(logging.INFO)
    from airflow.sdk import TaskInstanceState
    # assert os.environ['AIRFLOW_HOME'] == str(temporary_airflow_home)
    # assert os.path.exists(os.path.join(str(temporary_airflow_home), 'airflow.db'))
    # from airflow.sdk import DAG, TaskInstanceState
    p = import_module('..meta', package='dlairflow.test')
    validate_schema_file_task = p.__dict__['validate_schema_file_task']
    assert hasattr(validate_schema_file_task, 'function')
    assert hasattr(validate_schema_file_task, 'kwargs')
    assert validate_schema_file_task.kwargs['task_id'] == 'validate_schema_file_task'
    assert len(dag.tasks) == 1
    dagrun = dag.test()
    ti = dagrun.get_task_instance(task_id='validate_schema_file_task')
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull()
    assert isinstance(result, dict)  # Serialized version of Schema object.
    # print(result)
    schema = Schema.model_validate_json(json.dumps(result['__data__']))
    assert isinstance(schema, Schema)
    cleanup_airflow_home()
