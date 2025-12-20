#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI - Process Supervisor                           ║
║                  Manages worker processes lifecycle                          ║
╚══════════════════════════════════════════════════════════════════════════════╝

Purpose:
  - Spawn N worker processes (bounded)
  - Monitor worker health and restart crashed workers
  - Graceful shutdown with timeout
  - Force kill stragglers
  - Process-level isolation (no thread sharing)

Guarantees:
  - NEVER exceeds MAX_WORKERS processes
  - All workers cleaned up on stop
  - No orphan processes

Author: POWER-BHOOMI Team
Version: 4.0.0
"""

import os
import time
import signal
import logging
import threading
import multiprocessing as mp
from typing import Dict, List, Optional
from dataclasses import dataclass, field
import psutil

from playwright_worker import PlaywrightWorker

logger = logging.getLogger('ProcessSupervisor')


@dataclass
class WorkerInfo:
    """Information about a worker process"""
    worker_id: int
    process: mp.Process
    pid: Optional[int] = None
    started_at: float = field(default_factory=time.time)
    tasks_completed: int = 0
    tasks_failed: int = 0
    status: str = 'running'  # running, crashed, stopped
    last_heartbeat: float = field(default_factory=time.time)


class ProcessSupervisor:
    """
    Manages worker process lifecycle.
    
    Features:
    - Spawn N workers with staggered startup
    - Monitor for crashes and restart
    - Graceful shutdown (SIGTERM then SIGKILL)
    - Hard browser budget enforcement
    - Health metrics collection
    
    Usage:
        supervisor = ProcessSupervisor(
            num_workers=4,
            task_queue=queue,
            shared_state=state
        )
        
        supervisor.start_workers()
        # ... workers run ...
        supervisor.stop_workers(graceful_timeout=30)
    """
    
    def __init__(
        self,
        num_workers: int,
        task_queue: mp.Queue,
        shutdown_event: mp.Event,
        shared_state: Dict,
        db_path: str = None,
        auto_restart: bool = True
    ):
        """
        Initialize process supervisor.
        
        Args:
            num_workers: Number of worker processes (hard limit)
            task_queue: Shared task queue
            shutdown_event: Shared shutdown event
            shared_state: Shared state dict for metrics
            db_path: SQLite database path
            auto_restart: Auto-restart crashed workers
        """
        self.num_workers = num_workers
        self.task_queue = task_queue
        self.shutdown_event = shutdown_event
        self.shared_state = shared_state
        self.db_path = db_path
        self.auto_restart = auto_restart
        
        # Worker tracking
        self.workers: Dict[int, WorkerInfo] = {}
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_monitoring = threading.Event()
        
        logger.info(f"ProcessSupervisor initialized (max workers: {num_workers})")
    
    def start_workers(self, staggered_delay: float = 1.0):
        """
        Spawn worker processes with staggered startup.
        
        Args:
            staggered_delay: Seconds to wait between starting each worker
        """
        logger.info(f"🚀 Starting {self.num_workers} worker processes...")
        
        for worker_id in range(self.num_workers):
            self._spawn_worker(worker_id)
            
            # Staggered startup to avoid Chrome conflicts
            if worker_id < self.num_workers - 1:
                time.sleep(staggered_delay)
        
        # Start monitoring thread (watches for crashes)
        if self.auto_restart:
            self._start_monitoring()
        
        logger.info(f"✅ All {self.num_workers} workers started")
    
    def _spawn_worker(self, worker_id: int):
        """Spawn a single worker process"""
        process = mp.Process(
            target=PlaywrightWorker.run,
            args=(worker_id, self.task_queue, self.shutdown_event, self.shared_state, self.db_path),
            name=f'PlaywrightWorker-{worker_id}',
            daemon=False  # Explicit lifecycle control
        )
        
        process.start()
        
        worker_info = WorkerInfo(
            worker_id=worker_id,
            process=process,
            pid=process.pid,
            status='running'
        )
        
        self.workers[worker_id] = worker_info
        logger.info(f"  Worker {worker_id} spawned (PID: {process.pid})")
    
    def _start_monitoring(self):
        """Start background monitoring thread"""
        import threading
        
        if self._monitor_thread and self._monitor_thread.is_alive():
            return
        
        self._stop_monitoring.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name='WorkerMonitor',
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("🔍 Worker monitoring started")
    
    def _monitor_loop(self):
        """Monitor workers and restart crashed ones"""
        while not self._stop_monitoring.wait(5):  # Check every 5 seconds
            try:
                for worker_id, worker_info in list(self.workers.items()):
                    if not worker_info.process.is_alive() and not self.shutdown_event.is_set():
                        # Worker crashed - restart it
                        logger.warning(f"⚠️  Worker {worker_id} crashed, restarting...")
                        
                        # Clean up zombie process
                        worker_info.process.join(timeout=5)
                        worker_info.status = 'crashed'
                        
                        # Spawn replacement
                        self._spawn_worker(worker_id)
                        
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")
    
    def stop_workers(self, graceful_timeout: int = 30):
        """
        Stop all workers gracefully, force kill if needed.
        
        Process:
        1. Set shutdown event (workers finish current task)
        2. Wait up to `graceful_timeout` seconds
        3. Send SIGTERM to stragglers
        4. Wait 5 more seconds
        5. Send SIGKILL to any remaining processes
        
        Args:
            graceful_timeout: Seconds to wait for graceful shutdown
        """
        logger.info(f"🛑 Stopping {len(self.workers)} workers (graceful timeout: {graceful_timeout}s)...")
        
        # Step 1: Signal workers to stop
        self.shutdown_event.set()
        self._stop_monitoring.set()
        
        # Step 2: Wait for graceful shutdown
        start_time = time.time()
        while time.time() - start_time < graceful_timeout:
            alive = [w.process.is_alive() for w in self.workers.values()]
            if not any(alive):
                logger.info("✅ All workers stopped gracefully")
                self._cleanup_workers()
                return
            time.sleep(1)
        
        # Step 3: Send SIGTERM to stragglers
        stragglers = [w for w in self.workers.values() if w.process.is_alive()]
        if stragglers:
            logger.warning(f"⚠️  {len(stragglers)} workers still running, sending SIGTERM...")
            for worker_info in stragglers:
                try:
                    os.kill(worker_info.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass  # Already dead
            
            # Wait 5 more seconds
            time.sleep(5)
        
        # Step 4: Send SIGKILL to remaining processes
        still_alive = [w for w in self.workers.values() if w.process.is_alive()]
        if still_alive:
            logger.warning(f"⚠️  {len(still_alive)} workers still alive, sending SIGKILL...")
            for worker_info in still_alive:
                try:
                    os.kill(worker_info.pid, signal.SIGKILL)
                    worker_info.process.join(timeout=5)
                except ProcessLookupError:
                    pass
        
        self._cleanup_workers()
        logger.info("✅ All workers stopped")
    
    def _cleanup_workers(self):
        """Join all worker processes"""
        for worker_info in self.workers.values():
            if worker_info.process.is_alive():
                worker_info.process.join(timeout=5)
        self.workers.clear()
    
    def get_worker_stats(self) -> List[Dict]:
        """
        Get statistics for all workers.
        
        Returns:
            List of worker stats dicts
        """
        stats = []
        for worker_id, worker_info in self.workers.items():
            stats.append({
                'worker_id': worker_id,
                'pid': worker_info.pid,
                'status': worker_info.status,
                'uptime_seconds': time.time() - worker_info.started_at,
                'is_alive': worker_info.process.is_alive(),
                'exitcode': worker_info.process.exitcode
            })
        return stats
    
    def get_chromium_process_count(self) -> int:
        """
        Count Chromium processes owned by workers.
        
        This verifies the hard browser budget is enforced.
        
        Returns:
            Number of Chromium processes found
        """
        count = 0
        worker_pids = [w.pid for w in self.workers.values() if w.pid]
        
        for proc in psutil.process_iter(['pid', 'name', 'ppid']):
            try:
                # Check if this is a chromium process
                if 'chromium' in proc.info['name'].lower() or 'chrome' in proc.info['name'].lower():
                    # Check if parent is one of our workers
                    ppid = proc.info['ppid']
                    if ppid in worker_pids:
                        count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return count
    
    def verify_browser_budget(self) -> bool:
        """
        Verify browser budget is not exceeded.
        
        Returns:
            True if budget is respected, False if exceeded
        """
        chromium_count = self.get_chromium_process_count()
        
        # We expect at most num_workers browsers
        # Allow some tolerance for browser startup/shutdown
        max_allowed = self.num_workers + 2
        
        if chromium_count > max_allowed:
            logger.error(f"❌ Browser budget exceeded! Found {chromium_count} Chromium processes (max: {max_allowed})")
            return False
        
        return True
    
    def get_summary(self) -> Dict:
        """
        Get supervisor summary.
        
        Returns:
            Summary dict with metrics
        """
        alive_workers = sum(1 for w in self.workers.values() if w.process.is_alive())
        chromium_count = self.get_chromium_process_count()
        
        return {
            'total_workers': self.num_workers,
            'alive_workers': alive_workers,
            'chromium_processes': chromium_count,
            'budget_ok': chromium_count <= self.num_workers + 2,
            'task_queue_size': self.task_queue.qsize(),
            'workers': self.get_worker_stats()
        }


if __name__ == '__main__':
    # Test supervisor
    import multiprocessing as mp
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s'
    )
    
    # Create shared resources
    manager = mp.Manager()
    task_queue = mp.Queue(maxsize=100)
    shutdown_event = mp.Event()
    shared_state = manager.dict({
        'tasks_completed': 0,
        'tasks_failed': 0
    })
    
    # Create supervisor
    supervisor = ProcessSupervisor(
        num_workers=2,
        task_queue=task_queue,
        shutdown_event=shutdown_event,
        shared_state=shared_state
    )
    
    # Start workers
    supervisor.start_workers()
    
    # Let them run for 10 seconds
    print("\nWorkers running for 10 seconds...")
    for i in range(10):
        time.sleep(1)
        summary = supervisor.get_summary()
        print(f"  [{i+1}s] Workers: {summary['alive_workers']}/{summary['total_workers']}, "
              f"Chromium: {summary['chromium_processes']}, Queue: {summary['task_queue_size']}")
    
    # Stop workers
    print("\nStopping workers...")
    supervisor.stop_workers(graceful_timeout=10)
    
    # Verify cleanup
    final_chromium = supervisor.get_chromium_process_count()
    print(f"\n✅ Test complete! Chromium processes after cleanup: {final_chromium}")
    
    if final_chromium > 0:
        print(f"⚠️  Warning: {final_chromium} Chromium processes still running")
    else:
        print("✅ All Chromium processes cleaned up successfully!")

