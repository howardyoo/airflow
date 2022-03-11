#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
SequentialExecutor

.. seealso::
    For more information on how the SequentialExecutor works, take a look at the guide:
    :ref:`executor:SequentialExecutor`
"""
import subprocess
from typing import Any, Optional
import time
from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.state import State

from airflow.stats import Stats, Trace
from airflow.models.dag import DagRun
from airflow import settings

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

class SequentialExecutor(BaseExecutor):
    """
    This executor will only run one task instance at a time, can be used
    for debugging. It is also the only executor that can be used with sqlite
    since sqlite doesn't support multiple connections.

    Since we want airflow to work out of the box, it defaults to this
    SequentialExecutor alongside sqlite as you first install it.
    """

    def __init__(self):
        super().__init__()
        self.commands_to_run = []

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ) -> None:
        self.validate_command(command)
        self.commands_to_run.append((key, command))

    def sync(self) -> None:
        session = settings.Session()
        for key, command in self.commands_to_run:
            self.log.info("Executing command: %s", command)
            dagruns = DagRun.find(dag_id=key.dag_id, run_id=key.run_id, session=session)
            if len(dagruns) == 1:
                dagrun = dagruns[0]
                ti = dagrun.get_task_instance(task_id=key.task_id, session=session)
                span = Trace.get_span_from_json(ti.span_json)
                # we have to modify the 'actual start time' 

                span_name = f"{ti.task_id}-{command[1]}-{command[2]}"
                execution_span = Trace.start_span_from_taskinstance(service_name=self.__class__.__name__, span_name=span_name, ti=ti)
                execution_span.set_attributes({
                    "command": f"{command[1]} {command[2]}",
                    "dag_id": command[3],
                    "task_id": command[4],
                    "run_id": command[5]
                }) # we could add more details to here.

                try:
                    subprocess.check_call(command, close_fds=True)
                    self.change_state(key, State.SUCCESS)
                    span.set_attribute("state", State.SUCCESS)
                except subprocess.CalledProcessError as e:
                    self.change_state(key, State.FAILED)
                    self.log.error("Failed to execute task %s.", str(e))
                    span.set_attribute("state", State.FAILED)
                    span.set_attribute("error", "true")
                    span.add_event(name="subprocess.CalledProcessError", attributes={"message": str(e)})

                print(f"sequential-executor sync -> {execution_span.to_json()}")
                execution_span.end()
                span.end()

        self.commands_to_run = []

    def end(self):
        """End the executor."""
        self.heartbeat()

    def terminate(self):
        """Terminate the executor is not doing anything."""
