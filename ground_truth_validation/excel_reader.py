"""Excel data reader for Format A files."""
# pyright: reportMissingImports=false
import re
from pathlib import Path
from typing import Optional
from datetime import datetime
import logging

import polars as pl

logger = logging.getLogger(__name__)


class ExcelReader:
    """Read Format A Excel files (标签/采集时间/采集值/单位)."""
    
    def __init__(self, data_dir: Path):
        self.data_dir = Path(data_dir)
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
    
    def _parse_tag_metric(self, tag: str) -> Optional[str]:
        """Extract metric_name from tag using simplified mapping logic."""
        if not tag:
            return None
        
        compact = re.sub(r"\s+", "", str(tag).strip())
        
        # Frequency
        if "频率" in compact:
            return "frequency"
        
        # Load rate
        if "电流百分比" in compact or "负载率" in compact or "负荷率" in compact:
            return "load_rate"
        
        # Runtime
        if "运行时间" in compact or "模式时间" in compact:
            return "runtime"
        
        # Flow
        if "流量" in compact:
            if "冷冻水" in compact:
                return "chilled_flow"
            elif "冷却水" in compact:
                return "cooling_flow"
        
        # Temperature
        if "温度" in compact:
            if "冷冻水" in compact:
                if "供水" in compact:
                    return "chilled_supply_temp"
                elif "回水" in compact:
                    return "chilled_return_temp"
            elif "冷却水" in compact:
                if "上塔" in compact or "回水" in compact or "进塔" in compact:
                    return "cooling_return_temp"
                elif "下塔" in compact or "供水" in compact or "出塔" in compact:
                    return "cooling_supply_temp"
        
        return None
    
    def _parse_filename_metric(self, filename: str) -> Optional[str]:
        """Extract metric from filename (电量/功率)."""
        if "电量" in filename:
            return "energy"
        elif "功率" in filename:
            return "power"
        return None
    
    def read_excel_file(self, file_path: Path) -> pl.DataFrame:
        """Read a single Format A Excel file and return DataFrame.
        
        Returns DataFrame with columns: timestamp, tag, value, metric_name
        """
        # Skip non-data files (parameter tables, config files)
        skip_keywords = ['参数', '编号', '型号', '配置', '说明', '汇总']
        if any(keyword in file_path.name for keyword in skip_keywords):
            logger.debug(f"Skipping non-data file: {file_path.name}")
            return pl.DataFrame()
        try:
            # Read Excel with openpyxl engine
            df = pl.read_excel(file_path, engine="openpyxl")
            
            # Format A: 标签/采集时间/采集值/单位
            if df.shape[1] < 3:
                logger.warning(f"File {file_path.name} has insufficient columns, skipping")
                return pl.DataFrame()
            
            # Rename columns to standard names
            rename_map = {
                df.columns[0]: "tag",
                df.columns[1]: "timestamp",
                df.columns[2]: "value",
            }
            df = df.rename(rename_map)
            
            # Parse metric from filename
            filename_metric = self._parse_filename_metric(file_path.name)
            
            # Parse metric from tag for each row
            df = df.with_columns(
                pl.col("tag")
                .map_elements(self._parse_tag_metric, return_dtype=pl.String)
                .alias("metric_name")
            )
            
            # Use filename metric as fallback
            if filename_metric:
                df = df.with_columns(pl.col("metric_name").fill_null(filename_metric))
            
            # Convert timestamp
            df = df.with_columns(
                pl.col("timestamp")
                .cast(pl.Utf8)
                .str.strptime(pl.Datetime, strict=False)
                .alias("timestamp")
            )
            
            # Convert value to numeric
            df = df.with_columns(pl.col("value").cast(pl.Float64, strict=False).alias("value"))
            
            # Drop rows with missing critical fields
            df = df.drop_nulls(["timestamp", "value", "metric_name"])
            
            return df.select(["timestamp", "tag", "value", "metric_name"])
            
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return pl.DataFrame()
    
    def read_metric_data(
        self,
        metric_name: str,
        time_start: datetime,
        time_end: datetime,
        building_id: str = "G11",
        system_id: str = "1"
    ) -> pl.DataFrame:
        """Read all Excel files for a specific metric and time range.
        
        Returns DataFrame with columns: timestamp, tag, value, metric_name
        """
        all_data = []
        
        # Scan all Excel files in data directory
        for excel_file in self.data_dir.rglob("*.xlsx"):
            if excel_file.name.startswith('~$'):  # Skip temp files
                continue
            
            # Skip non-data files (parameter tables, config files)
            skip_keywords = ['参数', '编号', '型号', '配置', '说明', '汇总']
            if any(keyword in excel_file.name for keyword in skip_keywords):
                continue
            
            df = self.read_excel_file(excel_file)
            if df.is_empty():
                continue
            
            # Filter by metric
            df = df.filter(pl.col("metric_name") == metric_name)
            if df.is_empty():
                continue
            
            # Filter by time range
            df = df.filter(
                (pl.col("timestamp") >= time_start) & (pl.col("timestamp") <= time_end)
            )
            if df.is_empty():
                continue
            
            all_data.append(df)
        
        if not all_data:
            logger.warning(f"No data found for metric {metric_name} in range {time_start} to {time_end}")
            return pl.DataFrame()
        
        # Combine all data
        combined = pl.concat(all_data, how="vertical_relaxed")
        combined = combined.sort("timestamp")
        
        return combined
