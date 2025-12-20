#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI - Portal Health Monitor                         ║
║                  HTTP-based portal health checking                            ║
║                  (No browser overhead)                                        ║
╚══════════════════════════════════════════════════════════════════════════════╝

Purpose:
  - Proactive portal health monitoring without creating browsers
  - Circuit breaker pattern for graceful degradation
  - Exponential backoff on failures
  - Thread-safe, runs in background thread

Author: POWER-BHOOMI Team
Version: 4.0.0
"""

import requests
import time
import threading
import logging
from enum import Enum
from dataclasses import dataclass, asdict
from typing import Optional, Dict
import urllib3

# Disable SSL warnings (Bhoomi portal has cert issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger('PortalHealth')


class PortalStatus(Enum):
    """Portal health states"""
    HEALTHY = 'healthy'           # < 1s response, HTTP 200
    DEGRADED = 'degraded'         # 1-3s response, HTTP 200
    RATE_LIMITED = 'rate_limited' # HTTP 429 or repeated timeouts
    DOWN = 'down'                 # Connection refused, HTTP 5xx
    UNKNOWN = 'unknown'           # Not checked yet


@dataclass
class HealthMetrics:
    """Current health metrics"""
    status: PortalStatus
    response_time_ms: float
    consecutive_failures: int
    consecutive_successes: int
    last_check_time: float
    last_success_time: float
    total_checks: int
    failed_checks: int
    
    def to_dict(self) -> Dict:
        return {
            'status': self.status.value,
            'response_time_ms': round(self.response_time_ms, 2),
            'consecutive_failures': self.consecutive_failures,
            'consecutive_successes': self.consecutive_successes,
            'last_check_ago_seconds': round(time.time() - self.last_check_time, 1),
            'last_success_ago_seconds': round(time.time() - self.last_success_time, 1),
            'total_checks': self.total_checks,
            'failed_checks': self.failed_checks,
            'success_rate': round((self.total_checks - self.failed_checks) / max(self.total_checks, 1) * 100, 1)
        }


class PortalHealthMonitor:
    """
    HTTP-based portal health monitoring.
    
    Features:
    - Lightweight HEAD requests (no browser needed)
    - Circuit breaker pattern
    - Exponential backoff
    - Thread-safe operation
    
    Usage:
        monitor = PortalHealthMonitor()
        monitor.start()
        
        if monitor.should_allow_task():
            # Process task
            pass
        
        backoff = monitor.get_backoff_seconds()
        time.sleep(backoff)
        
        monitor.stop()
    """
    
    def __init__(self, portal_url: str = None, check_interval: float = 10.0):
        """
        Initialize portal health monitor.
        
        Args:
            portal_url: Portal URL to monitor (default: Bhoomi Service2)
            check_interval: Seconds between health checks (default: 10s)
        """
        self.portal_url = portal_url or 'https://landrecords.karnataka.gov.in/Service2/'
        self.check_interval = check_interval
        
        # Current metrics
        self._metrics = HealthMetrics(
            status=PortalStatus.UNKNOWN,
            response_time_ms=0,
            consecutive_failures=0,
            consecutive_successes=0,
            last_check_time=0,
            last_success_time=time.time(),
            total_checks=0,
            failed_checks=0
        )
        
        # Thread control
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        
        # State change callbacks
        self._state_change_callbacks = []
        
        logger.info(f"🏥 PortalHealthMonitor initialized (URL: {self.portal_url})")
    
    def start(self):
        """Start background health monitoring thread"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            logger.warning("Monitor already running")
            return
        
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name='PortalHealthMonitor',
            daemon=True
        )
        self._monitor_thread.start()
        logger.info("🏥 Portal health monitoring started")
    
    def stop(self):
        """Stop health monitoring thread"""
        if not self._monitor_thread:
            return
        
        self._stop_event.set()
        self._monitor_thread.join(timeout=10)
        logger.info("🏥 Portal health monitoring stopped")
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        while not self._stop_event.wait(self.check_interval):
            try:
                self._check_health()
            except Exception as e:
                logger.error(f"Health check error: {e}")
    
    def _check_health(self):
        """Perform HTTP HEAD request to check portal availability"""
        old_status = self._metrics.status
        
        try:
            start_time = time.time()
            
            # HEAD request (lighter than GET)
            response = requests.head(
                self.portal_url,
                timeout=5,
                allow_redirects=True,
                verify=False  # Bhoomi has cert issues
            )
            
            elapsed_ms = (time.time() - start_time) * 1000
            
            with self._lock:
                self._metrics.total_checks += 1
                self._metrics.last_check_time = time.time()
                self._metrics.response_time_ms = elapsed_ms
                
                # Determine status based on response
                if response.status_code == 200:
                    # Success - determine health level based on response time
                    if elapsed_ms < 1000:
                        new_status = PortalStatus.HEALTHY
                    elif elapsed_ms < 3000:
                        new_status = PortalStatus.DEGRADED
                    else:
                        new_status = PortalStatus.DEGRADED
                    
                    self._metrics.status = new_status
                    self._metrics.consecutive_failures = 0
                    self._metrics.consecutive_successes += 1
                    self._metrics.last_success_time = time.time()
                    
                elif response.status_code == 429:
                    # Rate limited
                    self._metrics.status = PortalStatus.RATE_LIMITED
                    self._metrics.consecutive_failures += 1
                    self._metrics.consecutive_successes = 0
                    self._metrics.failed_checks += 1
                    
                elif response.status_code >= 500:
                    # Server error
                    self._metrics.status = PortalStatus.DOWN
                    self._metrics.consecutive_failures += 1
                    self._metrics.consecutive_successes = 0
                    self._metrics.failed_checks += 1
                    
                else:
                    # Other errors (4xx, etc.)
                    self._metrics.status = PortalStatus.DEGRADED
                    self._metrics.consecutive_failures += 1
                    self._metrics.consecutive_successes = 0
                
        except (requests.Timeout, requests.ConnectionError) as e:
            # Network issues - portal might be down or network congested
            with self._lock:
                self._metrics.total_checks += 1
                self._metrics.last_check_time = time.time()
                self._metrics.status = PortalStatus.DOWN
                self._metrics.consecutive_failures += 1
                self._metrics.consecutive_successes = 0
                self._metrics.failed_checks += 1
                self._metrics.response_time_ms = 5000  # Timeout value
                
        except Exception as e:
            # Unknown error
            logger.error(f"Health check failed: {e}")
            with self._lock:
                self._metrics.total_checks += 1
                self._metrics.last_check_time = time.time()
                self._metrics.status = PortalStatus.UNKNOWN
                self._metrics.failed_checks += 1
        
        # Detect state change
        new_status = self._metrics.status
        if old_status != new_status:
            logger.info(f"🏥 Portal state changed: {old_status.value} → {new_status.value}")
            self._notify_state_change(old_status, new_status)
    
    def should_allow_task(self) -> bool:
        """
        Circuit breaker - should we allow task processing?
        
        Returns:
            True if tasks should be processed, False if should pause
        """
        with self._lock:
            status = self._metrics.status
            
            if status == PortalStatus.HEALTHY:
                return True
            elif status == PortalStatus.DEGRADED:
                # Allow 50% of tasks in degraded mode
                return (int(time.time() * 1000) % 2) == 0
            elif status in (PortalStatus.RATE_LIMITED, PortalStatus.DOWN):
                # Pause completely
                return False
            else:  # UNKNOWN
                # Be conservative - allow but with caution
                return True
    
    def get_backoff_seconds(self) -> float:
        """
        Get exponential backoff duration based on consecutive failures.
        
        Returns:
            Seconds to wait before next attempt (0 if no backoff needed)
        """
        with self._lock:
            failures = self._metrics.consecutive_failures
            
            if failures == 0:
                return 0
            
            # Exponential backoff: 2^failures seconds, capped at 5 minutes
            backoff = min(2 ** failures, 300)
            return backoff
    
    def get_status(self) -> PortalStatus:
        """Get current portal status"""
        with self._lock:
            return self._metrics.status
    
    def get_metrics(self) -> Dict:
        """Get current metrics as dictionary"""
        with self._lock:
            return self._metrics.to_dict()
    
    def register_state_change_callback(self, callback):
        """Register callback for state changes"""
        self._state_change_callbacks.append(callback)
    
    def _notify_state_change(self, old_status: PortalStatus, new_status: PortalStatus):
        """Notify callbacks of state change"""
        for callback in self._state_change_callbacks:
            try:
                callback(old_status, new_status)
            except Exception as e:
                logger.error(f"State change callback error: {e}")
    
    def force_check(self):
        """Force an immediate health check (blocking)"""
        self._check_health()
    
    def reset_metrics(self):
        """Reset metrics (useful for testing)"""
        with self._lock:
            self._metrics = HealthMetrics(
                status=PortalStatus.UNKNOWN,
                response_time_ms=0,
                consecutive_failures=0,
                consecutive_successes=0,
                last_check_time=0,
                last_success_time=time.time(),
                total_checks=0,
                failed_checks=0
            )


# Singleton instance for global access
_global_monitor: Optional[PortalHealthMonitor] = None
_global_lock = threading.Lock()


def get_portal_monitor() -> PortalHealthMonitor:
    """Get or create global portal monitor instance"""
    global _global_monitor
    if _global_monitor is None:
        with _global_lock:
            if _global_monitor is None:
                _global_monitor = PortalHealthMonitor()
    return _global_monitor


if __name__ == '__main__':
    # Test the monitor
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s'
    )
    
    monitor = PortalHealthMonitor()
    
    # Test manual check
    print("Testing portal health...")
    monitor.force_check()
    print(f"Status: {monitor.get_status()}")
    print(f"Metrics: {monitor.get_metrics()}")
    
    # Test background monitoring
    print("\nStarting background monitoring (30 seconds)...")
    monitor.start()
    
    for i in range(6):
        time.sleep(5)
        metrics = monitor.get_metrics()
        print(f"[{i*5}s] Status: {metrics['status']}, Response: {metrics['response_time_ms']}ms")
        print(f"      Allow task: {monitor.should_allow_task()}, Backoff: {monitor.get_backoff_seconds()}s")
    
    monitor.stop()
    print("\nTest complete!")





