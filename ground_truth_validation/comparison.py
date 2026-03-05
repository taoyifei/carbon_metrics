"""Comparison engine - compare GT vs Backend results."""
from typing import Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class ComparisonEngine:
    """Compare Ground Truth vs Backend API results."""
    
    def __init__(self, epsilon: float = 0.001):
        self.epsilon = epsilon  # 0.1% tolerance
    
    def compare(self, gt_result: Dict[str, Any], backend_result: Dict[str, Any]) -> Dict[str, Any]:
        """Compare two metric results.
        
        Returns:
            {
                "match": bool,
                "gt_value": float,
                "backend_value": float,
                "diff": float,
                "diff_pct": float,
                "status": str
            }
        """
        gt_val = gt_result.get("value")
        backend_val = backend_result.get("value")
        
        # Both no data
        if gt_val is None and backend_val is None:
            return {
                "match": True,
                "gt_value": None,
                "backend_value": None,
                "diff": None,
                "diff_pct": None,
                "status": "both_no_data"
            }
        
        # One has data, other doesn't
        if gt_val is None or backend_val is None:
            return {
                "match": False,
                "gt_value": gt_val,
                "backend_value": backend_val,
                "diff": None,
                "diff_pct": None,
                "status": "data_mismatch"
            }
        
        # Both have data - compare
        diff = abs(gt_val - backend_val)
        diff_pct = (diff / abs(gt_val)) if gt_val != 0 else 0
        match = diff_pct <= self.epsilon
        
        return {
            "match": match,
            "gt_value": round(gt_val, 4),
            "backend_value": round(backend_val, 4),
            "diff": round(diff, 4),
            "diff_pct": round(diff_pct * 100, 4),
            "status": "match" if match else "mismatch"
        }
