"""Report generator - output validation results to markdown."""
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path


class ReportGenerator:
    """Generate markdown validation report."""
    
    def generate(
        self,
        results: List[Dict[str, Any]],
        output_path: Path,
        calibration_month: str,
        building_id: str,
        system_id: str
    ):
        """Generate markdown report from comparison results."""
        lines = [
            "# Ground Truth Validation Report",
            "",
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Calibration Month:** {calibration_month}",
            f"**Building:** {building_id}",
            f"**System:** {system_id}",
            "",
            "## Summary",
            ""
        ]
        
        # Summary stats
        total = len(results)
        matched = sum(1 for r in results if r['comparison']['match'])
        mismatched = total - matched
        
        lines.extend([
            f"- **Total Metrics:** {total}",
            f"- **Matched:** {matched} ({matched/total*100:.1f}%)",
            f"- **Mismatched:** {mismatched} ({mismatched/total*100:.1f}%)",
            "",
            "## Detailed Results",
            "",
            "| Metric | GT Value | Backend Value | Diff | Diff % | Status |",
            "|--------|----------|---------------|------|--------|--------|"
        ])
        
        for r in results:
            metric = r['metric_name']
            comp = r['comparison']
            gt = comp['gt_value'] if comp['gt_value'] is not None else 'N/A'
            be = comp['backend_value'] if comp['backend_value'] is not None else 'N/A'
            diff = comp['diff'] if comp['diff'] is not None else 'N/A'
            diff_pct = f"{comp['diff_pct']}%" if comp['diff_pct'] is not None else 'N/A'
            status = "✅" if comp['match'] else "❌"
            
            lines.append(f"| {metric} | {gt} | {be} | {diff} | {diff_pct} | {status} |")
        
        # Write to file
        output_path.write_text('\n'.join(lines), encoding='utf-8')
