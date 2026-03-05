"""Backend API client for fetching metric calculations."""
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BackendAPIClient:
    """Client for interacting with carbon_metrics backend API."""
    
    def __init__(self, base_url: str = "http://127.0.0.1:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def calculate_metric(
        self,
        metric_name: str,
        time_start: datetime,
        time_end: datetime,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        equipment_id: Optional[str] = None,
        sub_equipment_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculate a single metric via backend API.
        
        Returns:
            API response with keys: value, unit, status, quality_score, etc.
        """
        params = {
            'metric_name': metric_name,
            'time_start': time_start.isoformat(),
            'time_end': time_end.isoformat()
        }
        
        if building_id:
            params['building_id'] = building_id
        if system_id:
            params['system_id'] = system_id
        if equipment_type:
            params['equipment_type'] = equipment_type
        if equipment_id:
            params['equipment_id'] = equipment_id
        if sub_equipment_id:
            params['sub_equipment_id'] = sub_equipment_id
        
        url = f"{self.base_url}/api/metrics/calculate"
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {metric_name}: {e}")
            raise
    
    def calculate_batch(
        self,
        metric_names: List[str],
        time_start: datetime,
        time_end: datetime,
        building_id: Optional[str] = None,
        system_id: Optional[str] = None,
        equipment_type: Optional[str] = None,
        equipment_id: Optional[str] = None,
        sub_equipment_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Calculate multiple metrics in one batch request.
        
        Returns:
            API response with 'results' list containing each metric's calculation.
        """
        payload = {
            'metric_names': metric_names,
            'time_start': time_start.isoformat(),
            'time_end': time_end.isoformat()
        }
        
        if building_id:
            payload['building_id'] = building_id
        if system_id:
            payload['system_id'] = system_id
        if equipment_type:
            payload['equipment_type'] = equipment_type
        if equipment_id:
            payload['equipment_id'] = equipment_id
        if sub_equipment_id:
            payload['sub_equipment_id'] = sub_equipment_id
        
        url = f"{self.base_url}/api/metrics/calculate_batch"
        
        try:
            response = self.session.post(url, json=payload, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Batch API request failed: {e}")
            raise
    
    def health_check(self) -> bool:
        """Check if backend API is reachable."""
        try:
            response = self.session.get(f"{self.base_url}/health", timeout=5)
            return response.status_code == 200
        except requests.exceptions.RequestException:
            return False
