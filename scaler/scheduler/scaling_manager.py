import asyncio
import json
import logging
import urllib.request

from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import StateTask, StateWorker
from scaler.scheduler.mixins import ScalingManager
from scaler.utility.identifiers import WorkerID
from scaler.utility.mixins import Reporter


class VanillaScalingManager(ScalingManager, Reporter):
    def __init__(self, adapter_webhook_url: str, lower_task_ratio: int = 1,
                 upper_task_ratio: int = 10):
        self.worker_task_counts = {}
        self.worker_processes = {}
        self.total_task_count = 0

        self.adapter_webhook_url = adapter_webhook_url
        self.lower_task_ratio = lower_task_ratio
        self.upper_task_ratio = upper_task_ratio

    def get_status(self):
        return {"worker_task_counts": self.worker_task_counts}

    async def on_state_worker(self, state_worker: StateWorker):
        if state_worker.message == b"connected":
            self.worker_task_counts[state_worker.worker_id] = 0

        elif state_worker.message == b"disconnected":
            task_count = self.worker_task_counts.pop(state_worker.worker_id)
            self.total_task_count -= task_count

    async def on_state_task(self, state_task: StateTask):
        if state_task.status == TaskStatus.Inactive:
            if len(self.worker_task_counts) == 0:
                await self.start_worker()

            return

        if state_task.worker not in self.worker_task_counts:
            return

        if state_task.status == TaskStatus.Running:
            self.worker_task_counts[state_task.task_id] += 1
            self.total_task_count += 1

        elif state_task.status in (TaskStatus.Success, TaskStatus.Failed,
                                   TaskStatus.Canceled):
            self.worker_task_counts[state_task.task_id] -= 1
            self.total_task_count -= 1

        else:
            return

        if len(self.worker_task_counts) == 0:
            return

        task_ratio = self.total_task_count / len(self.worker_task_counts)

        if task_ratio > self.upper_task_ratio:
            await self.start_worker()
            logging.info(
                "Start new worker as task ratio is below the lower threshold.")

        elif task_ratio < self.lower_task_ratio:
            if self.total_task_count > 0 and len(self.worker_task_counts) <= 1:
                return

            worker_id = min(self.worker_task_counts,
                            key=self.worker_task_counts.get)
            await self.shutdown_worker(worker_id)
            logging.info(
                "Shutdown worker %s as task ratio is above the threshold.",
                worker_id)

    async def start_worker(self) -> WorkerID:
        response_data = await self._make_request({"action": "start_worker"})
        worker_id = response_data.get("worker_id")
        self.worker_task_counts[worker_id] = 0
        return worker_id

    async def shutdown_worker(self, worker_id: WorkerID):
        await self._make_request(
            {"action": "shutdown_worker", "worker_id": worker_id})
        if worker_id in self.worker_task_counts:
            self.worker_task_counts.pop(worker_id)

    async def _make_request(self, payload):
        def do_request():
            data = json.dumps(payload).encode("utf-8")
            headers = {"Content-Type": "application/json"}

            req = urllib.request.Request(self.adapter_webhook_url, data=data,
                                         headers=headers, method="POST")

            with urllib.request.urlopen(req) as response:
                if response.getcode() == 200:
                    return json.loads(response.read().decode("utf-8"))
                return {}

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, do_request)
