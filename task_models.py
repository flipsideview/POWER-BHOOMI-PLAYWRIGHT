#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                  POWER-BHOOMI - Task Models                                   ║
║                  Dataclasses for task-based execution                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

Purpose:
  - Define task structure for idempotent, resumable work units
  - Task granularity: (village, survey_no) or (village, survey, surnoc, hissa)
  - Supports serialization for multiprocessing.Queue

Author: POWER-BHOOMI Team
Version: 4.0.0
"""

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any
from datetime import datetime
import json


@dataclass
class SearchTask:
    """
    Atomic unit of work for Bhoomi search.
    
    Granularity options:
    1. Village-level: Entire village (survey_no=None explores all)
    2. Survey-level: Specific survey number (surnoc=None explores all)
    3. Hissa-level: Specific hissa (most granular)
    
    Tasks are:
    - Idempotent: Re-running same task produces same DB records
    - Deterministic: Same input → same output
    - Self-contained: No shared state except DB
    - Resumable: Can be re-queued after failure
    """
    
    # Session context
    session_id: str
    
    # Hierarchy
    district_code: str
    district_name: str
    taluk_code: str
    taluk_name: str
    hobli_code: str
    hobli_name: str
    village_code: str
    village_name: str
    
    # Survey specifics
    survey_no: int
    surnoc: Optional[str] = None  # None = explore all surnocs for this survey
    hissa: Optional[str] = None   # None = explore all hissas for this surnoc
    period: Optional[str] = None  # None = explore all periods for this hissa
    
    # Search criteria
    owner_name: str = ""
    owner_variants: list = field(default_factory=list)
    
    # Task metadata
    task_id: str = field(default="")  # Unique task identifier
    retry_count: int = 0
    created_at: float = field(default_factory=lambda: datetime.now().timestamp())
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    
    # Status tracking
    status: str = "pending"  # pending, processing, completed, failed
    error_message: str = ""
    worker_id: Optional[int] = None
    
    def __post_init__(self):
        """Generate task_id if not provided"""
        if not self.task_id:
            # Format: session_village_survey_surnoc_hissa
            parts = [
                self.session_id[:8],
                self.village_code,
                f"S{self.survey_no}"
            ]
            if self.surnoc:
                parts.append(f"SN{self.surnoc}")
            if self.hissa:
                parts.append(f"H{self.hissa}")
            self.task_id = "_".join(parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SearchTask':
        """Create from dictionary"""
        return cls(**data)
    
    def to_json(self) -> str:
        """Serialize to JSON"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'SearchTask':
        """Deserialize from JSON"""
        return cls.from_dict(json.loads(json_str))
    
    def mark_started(self, worker_id: int):
        """Mark task as started"""
        self.status = "processing"
        self.worker_id = worker_id
        self.started_at = datetime.now().timestamp()
    
    def mark_completed(self):
        """Mark task as completed"""
        self.status = "completed"
        self.completed_at = datetime.now().timestamp()
    
    def mark_failed(self, error: str):
        """Mark task as failed"""
        self.status = "failed"
        self.error_message = error
        self.completed_at = datetime.now().timestamp()
    
    def can_retry(self, max_retries: int = 3) -> bool:
        """Check if task can be retried"""
        return self.retry_count < max_retries
    
    def increment_retry(self):
        """Increment retry count and reset status"""
        self.retry_count += 1
        self.status = "pending"
        self.error_message = ""
        self.started_at = None
        self.completed_at = None
    
    def get_summary(self) -> str:
        """Get human-readable task summary"""
        summary = f"{self.village_name} - Survey #{self.survey_no}"
        if self.surnoc:
            summary += f" / Surnoc {self.surnoc}"
        if self.hissa:
            summary += f" / Hissa {self.hissa}"
        return summary


@dataclass
class VillageTask:
    """
    High-level village task (contains multiple survey tasks).
    Used for progress tracking and distribution.
    """
    session_id: str
    village_code: str
    village_name: str
    hobli_code: str
    hobli_name: str
    taluk_code: str
    taluk_name: str
    district_code: str
    district_name: str
    
    max_survey: int = 200
    status: str = "pending"  # pending, in_progress, completed, failed
    
    surveys_completed: int = 0
    records_found: int = 0
    matches_found: int = 0
    
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error_message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VillageTask':
        """Create from dictionary"""
        return cls(**data)
    
    def generate_survey_tasks(self, owner_name: str, owner_variants: list = None) -> list:
        """
        Generate survey-level tasks for this village.
        
        Returns:
            List of SearchTask objects (one per survey number)
        """
        tasks = []
        for survey_no in range(1, self.max_survey + 1):
            task = SearchTask(
                session_id=self.session_id,
                district_code=self.district_code,
                district_name=self.district_name,
                taluk_code=self.taluk_code,
                taluk_name=self.taluk_name,
                hobli_code=self.hobli_code,
                hobli_name=self.hobli_name,
                village_code=self.village_code,
                village_name=self.village_name,
                survey_no=survey_no,
                owner_name=owner_name,
                owner_variants=owner_variants or []
            )
            tasks.append(task)
        return tasks


@dataclass
class TaskStatistics:
    """Statistics for task processing"""
    total_tasks: int = 0
    pending_tasks: int = 0
    processing_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    retried_tasks: int = 0
    
    total_records: int = 0
    total_matches: int = 0
    
    avg_task_duration_seconds: float = 0.0
    tasks_per_minute: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)
    
    def completion_percentage(self) -> float:
        """Calculate completion percentage"""
        if self.total_tasks == 0:
            return 0.0
        return (self.completed_tasks / self.total_tasks) * 100
    
    def failure_rate(self) -> float:
        """Calculate failure rate"""
        if self.total_tasks == 0:
            return 0.0
        return (self.failed_tasks / self.total_tasks) * 100
    
    def estimated_time_remaining_minutes(self) -> float:
        """Estimate time remaining in minutes"""
        if self.tasks_per_minute == 0:
            return 0.0
        remaining = self.pending_tasks + self.processing_tasks
        return remaining / self.tasks_per_minute


if __name__ == '__main__':
    # Test task creation and serialization
    
    # Create a search task
    task = SearchTask(
        session_id="test_20251220_001",
        district_code="01",
        district_name="Bangalore Urban",
        taluk_code="02",
        taluk_name="Bangalore North",
        hobli_code="03",
        hobli_name="Yelahanka",
        village_code="12345",
        village_name="Test Village",
        survey_no=42,
        owner_name="Test Owner"
    )
    
    print("Task created:")
    print(f"  ID: {task.task_id}")
    print(f"  Summary: {task.get_summary()}")
    
    # Test serialization
    json_str = task.to_json()
    print(f"\nSerialized to JSON ({len(json_str)} bytes)")
    
    # Test deserialization
    task2 = SearchTask.from_json(json_str)
    print(f"Deserialized: {task2.task_id}")
    
    # Test village task
    village = VillageTask(
        session_id="test_20251220_001",
        village_code="12345",
        village_name="Test Village",
        hobli_code="03",
        hobli_name="Yelahanka",
        taluk_code="02",
        taluk_name="Bangalore North",
        district_code="01",
        district_name="Bangalore Urban",
        max_survey=10
    )
    
    survey_tasks = village.generate_survey_tasks("Test Owner")
    print(f"\nGenerated {len(survey_tasks)} survey tasks from village task")
    print(f"  First: {survey_tasks[0].get_summary()}")
    print(f"  Last: {survey_tasks[-1].get_summary()}")
    
    print("\n✅ Task models test complete!")





