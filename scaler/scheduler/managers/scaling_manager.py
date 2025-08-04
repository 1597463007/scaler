import logging
import uuid

import aiohttp

from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import StateTask, StateWorker
from scaler.scheduler.managers.mixins import ScalingManager
from scaler.utility.identifiers import WorkerID
from scaler.utility.mixins import Reporter


class NullScalingManager(ScalingManager, Reporter):
    def get_status(self):
        return {"worker_task_counts": 0, "workers_pending_startup": [], "workers_pending_shutdown": []}


class VanillaScalingManager(ScalingManager, Reporter):
    def __init__(self, adapter_webhook_url: str, lower_task_ratio: int = 1, upper_task_ratio: int = 10):
        self.worker_task_counts = {}
        self.total_task_count = 0

        self.workers_pending_startup = set()
        self.workers_pending_shutdown = set()

        self.adapter_webhook_url = adapter_webhook_url
        self.lower_task_ratio = lower_task_ratio
        self.upper_task_ratio = upper_task_ratio

    def get_status(self):
        return {
            "worker_task_counts": self.worker_task_counts,
            "workers_pending_startup": [worker_id.decode() for worker_id in self.workers_pending_startup],
            "workers_pending_shutdown": [worker_id.decode() for worker_id in self.workers_pending_shutdown],
        }

    async def on_state_worker(self, state_worker: StateWorker):
        if state_worker.message == b"connected" and state_worker.worker_id in self.workers_pending_startup:
            self.worker_task_counts[state_worker.worker_id] = 0
            self.workers_pending_startup.remove(state_worker.worker_id)

        elif state_worker.message == b"disconnected" and state_worker.worker_id in self.workers_pending_shutdown:
            task_count = self.worker_task_counts.pop(state_worker.worker_id)
            self.total_task_count -= task_count
            self.workers_pending_shutdown.remove(state_worker.worker_id)

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

        elif state_task.status in (TaskStatus.Success, TaskStatus.Failed, TaskStatus.Canceled):
            self.worker_task_counts[state_task.task_id] -= 1
            self.total_task_count -= 1

        else:
            return

        if len(self.worker_task_counts) == 0:
            return

        task_ratio = self.total_task_count / len(self.worker_task_counts)

        if task_ratio > self.upper_task_ratio:
            await self.start_worker()
            logging.info("Start new worker as task ratio is below the lower threshold.")

        elif task_ratio < self.lower_task_ratio:
            if self.total_task_count > 0 and len(self.worker_task_counts) <= 1:
                return

            worker_id = min(self.worker_task_counts, key=self.worker_task_counts.get)
            await self.shutdown_worker(worker_id)
            logging.info("Shutdown worker %s as task ratio is above the threshold.", worker_id)

    async def start_worker(self) -> WorkerID:
        worker_id_str = f"worker-{uuid.uuid4().hex}"
        await self._make_request({"action": "start_worker", "worker_id": worker_id_str})
        worker_id = WorkerID(worker_id_str.encode())
        self.workers_pending_startup.add(worker_id)
        return worker_id

    async def shutdown_worker(self, worker_id: WorkerID):
        await self._make_request({"action": "shutdown_worker", "worker_id": worker_id.decode()})
        self.workers_pending_shutdown.add(worker_id)

    async def _make_request(self, payload):
        async with aiohttp.ClientSession() as session:
            async with session.post(self.adapter_webhook_url, json=payload) as response:
                if response.status == 200:
                    return await response.json()
                return {}
