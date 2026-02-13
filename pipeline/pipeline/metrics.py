"""
鎸囨爣璁＄畻妯″潡
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pymysql

from .db import LOGGER
from .models import MetricDefinition


def load_metric_definitions(conn: pymysql.Connection) -> List[MetricDefinition]:
    """浠?metric_definition 琛ㄥ姞杞芥寚鏍囧畾涔?"""
    sql = """
        SELECT metric_code, metric_name, category_code, formula,
               required_metrics, applicable_levels, time_granularity,
               agg_method, unit, baseline_value
        FROM metric_definition
        WHERE is_active = 1
        ORDER BY sort_order
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    definitions = []
    for row in rows:
        definitions.append(MetricDefinition(
            metric_code=row['metric_code'],
            metric_name=row['metric_name'],
            category_code=row['category_code'],
            formula=row['formula'],
            required_metrics=json.loads(row['required_metrics']) if row['required_metrics'] else [],
            applicable_levels=json.loads(row['applicable_levels']) if row['applicable_levels'] else [],
            time_granularity=json.loads(row['time_granularity']) if row['time_granularity'] else [],
            agg_method=row['agg_method'],
            unit=row['unit'],
            baseline_value=row['baseline_value']
        ))
    return definitions


def load_equipment_params(conn: pymysql.Connection) -> Dict[str, Dict[str, Any]]:
    """鍔犺浇璁惧棰濆畾鍙傛暟"""
    sql = """
        SELECT system_id, equipment_type, equipment_id,
               rated_power_kw, extended_params
        FROM equipment_registry
        WHERE is_active = 1
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    params_map = {}
    for row in rows:
        key = f"{row['system_id']}|{row['equipment_type']}|{row['equipment_id']}"
        ext = json.loads(row['extended_params']) if row['extended_params'] else {}
        params_map[key] = {
            'rated_power_kw': float(row['rated_power_kw']) if row['rated_power_kw'] else None,
            'rated_cop': ext.get('rated_cop'),
            'cooling_capacity_kw': ext.get('cooling_capacity_kw'),
            'head_m': ext.get('head_m'),
            'flow_rate_m3h': ext.get('flow_rate_m3h'),
        }
    return params_map


def query_agg_data(
    conn: pymysql.Connection,
    bucket_type: str,
    start_time: str,
    end_time: str,
    metric_names: List[str],
    system_id: Optional[str] = None,
    equipment_type: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """鏌ヨ鑱氬悎鏁版嵁"""
    table = "agg_hour" if bucket_type == "hour" else "agg_day"

    sql = f"""
        SELECT bucket_time, building_id, system_id, equipment_type,
               equipment_id, sub_equipment_id, metric_name,
               agg_avg, agg_min, agg_max, agg_sum, agg_delta,
               agg_first, agg_last, sample_count
        FROM {table}
        WHERE bucket_time >= %s AND bucket_time < %s
          AND metric_name IN ({','.join(['%s'] * len(metric_names))})
    """
    params: List[Any] = [start_time, end_time] + metric_names

    if system_id:
        sql += " AND system_id = %s"
        params.append(system_id)
    if equipment_type:
        sql += " AND equipment_type = %s"
        params.append(equipment_type)

    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        rows = cursor.fetchall()

    data_map: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        bucket_str = row['bucket_time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(row['bucket_time'], 'strftime') else str(row['bucket_time'])
        key = f"{bucket_str}|{row['system_id']}|{row['equipment_type']}|{row['equipment_id'] or ''}|{row['sub_equipment_id'] or ''}|{row['metric_name']}"
        data_map[key] = {
            'bucket_time': row['bucket_time'],
            'building_id': row['building_id'],
            'system_id': row['system_id'],
            'equipment_type': row['equipment_type'],
            'equipment_id': row['equipment_id'],
            'sub_equipment_id': row['sub_equipment_id'],
            'metric_name': row['metric_name'],
            'agg_avg': row['agg_avg'],
            'agg_min': row['agg_min'],
            'agg_max': row['agg_max'],
            'agg_sum': row['agg_sum'],
            'agg_delta': row['agg_delta'],
            'agg_first': row['agg_first'],
            'agg_last': row['agg_last'],
            'sample_count': row['sample_count'],
        }
    return data_map


def get_metric_value(
    data_map: Dict[str, Dict[str, Any]],
    bucket_time: str,
    system_id: str,
    equipment_type: str,
    equipment_id: Optional[str],
    sub_equipment_id: Optional[str],
    metric_name: str,
    value_field: str = 'agg_avg'
) -> Optional[float]:
    """浠庢暟鎹瓧鍏镐腑鑾峰彇鎸囧畾鎸囨爣鐨勫€?"""
    key = f"{bucket_time}|{system_id}|{equipment_type}|{equipment_id or ''}|{sub_equipment_id or ''}|{metric_name}"
    if key in data_map:
        return data_map[key].get(value_field)
    return None


def build_type_metric_index(
    data_map: Dict[str, Dict[str, Any]],
) -> Dict[Tuple[str, str, str, str], Dict[str, float]]:
    """Pre-aggregate values by (bucket_date, system_id, equipment_type, metric_name)."""
    index: Dict[Tuple[str, str, str, str], Dict[str, float]] = {}
    fields = (
        "agg_avg",
        "agg_min",
        "agg_max",
        "agg_sum",
        "agg_delta",
        "agg_first",
        "agg_last",
        "sample_count",
    )

    for data in data_map.values():
        bucket_date = str(data["bucket_time"]).split()[0]
        key = (
            bucket_date,
            data["system_id"],
            data["equipment_type"],
            data["metric_name"],
        )
        stats = index.setdefault(key, {})

        for field in fields:
            val = data.get(field)
            if val is None:
                continue
            sum_key = f"{field}__sum"
            count_key = f"{field}__count"
            stats[sum_key] = stats.get(sum_key, 0.0) + float(val)
            stats[count_key] = stats.get(count_key, 0.0) + 1.0

    return index


def sum_metric_by_type(
    data_map: Dict[str, Dict[str, Any]],
    bucket_time: str,
    system_id: str,
    equipment_type: str,
    metric_name: str,
    value_field: str = 'agg_delta',
    type_metric_index: Optional[Dict[Tuple[str, str, str, str], Dict[str, float]]] = None,
) -> Optional[float]:
    """姹囨€绘煇璁惧绫诲瀷涓嬫墍鏈夎澶囩殑鎸囨爣鍊?"""
    if type_metric_index is not None:
        lookup_key = (bucket_time.split()[0], system_id, equipment_type, metric_name)
        stats = type_metric_index.get(lookup_key)
        if not stats:
            return None
        count = stats.get(f"{value_field}__count", 0.0)
        if count <= 0:
            return None
        return float(stats.get(f"{value_field}__sum", 0.0))

    total = 0.0
    found = False
    for data in data_map.values():
        if (str(data['bucket_time']).startswith(bucket_time.split()[0]) and
            data['system_id'] == system_id and
            data['equipment_type'] == equipment_type and
            data['metric_name'] == metric_name):
            val = data.get(value_field)
            if val is not None:
                total += val
                found = True
    return total if found else None


def avg_metric_by_type(
    data_map: Dict[str, Dict[str, Any]],
    bucket_time: str,
    system_id: str,
    equipment_type: str,
    metric_name: str,
    value_field: str = 'agg_avg',
    type_metric_index: Optional[Dict[Tuple[str, str, str, str], Dict[str, float]]] = None,
) -> Optional[float]:
    """璁＄畻鏌愯澶囩被鍨嬩笅鎵€鏈夎澶囩殑鎸囨爣骞冲潎鍊?"""
    if type_metric_index is not None:
        lookup_key = (bucket_time.split()[0], system_id, equipment_type, metric_name)
        stats = type_metric_index.get(lookup_key)
        if not stats:
            return None
        count = stats.get(f"{value_field}__count", 0.0)
        if count <= 0:
            return None
        total = stats.get(f"{value_field}__sum", 0.0)
        return float(total / count)

    values = []
    for data in data_map.values():
        if (str(data['bucket_time']).startswith(bucket_time.split()[0]) and
            data['system_id'] == system_id and
            data['equipment_type'] == equipment_type and
            data['metric_name'] == metric_name):
            val = data.get(value_field)
            if val is not None:
                values.append(val)
    return sum(values) / len(values) if values else None


# ============================================================
# 鍏蜂綋鎸囨爣璁＄畻鍑芥暟
# ============================================================

def calc_chiller_cop(
    chilled_flow: Optional[float],
    chilled_supply_temp: Optional[float],
    chilled_return_temp: Optional[float],
    chiller_power: Optional[float]
) -> Optional[float]:
    """璁＄畻鍐锋満COP"""
    if None in (chilled_flow, chilled_supply_temp, chilled_return_temp, chiller_power):
        return None
    if chiller_power <= 0:
        return None
    delta_t = chilled_return_temp - chilled_supply_temp
    if delta_t <= 0:
        return None
    cooling_capacity = chilled_flow * delta_t * 4.186 / 3.6
    return cooling_capacity / chiller_power


def calc_cooling_capacity(
    chilled_flow: Optional[float],
    chilled_supply_temp: Optional[float],
    chilled_return_temp: Optional[float]
) -> Optional[float]:
    """璁＄畻鍒跺喎閲?"""
    if None in (chilled_flow, chilled_supply_temp, chilled_return_temp):
        return None
    delta_t = chilled_return_temp - chilled_supply_temp
    if delta_t <= 0:
        return None
    return chilled_flow * delta_t * 4.186 / 3.6


def calc_energy_ratio(part_energy: Optional[float], total_energy: Optional[float]) -> Optional[float]:
    """璁＄畻鑳借€楀崰姣?"""
    if None in (part_energy, total_energy) or total_energy <= 0:
        return None
    return (part_energy / total_energy) * 100


def calc_delta_t(return_temp: Optional[float], supply_temp: Optional[float]) -> Optional[float]:
    """璁＄畻娓╁樊"""
    if None in (return_temp, supply_temp):
        return None
    return return_temp - supply_temp


def calc_power_utilization(actual_power: Optional[float], rated_power: Optional[float]) -> Optional[float]:
    """璁＄畻鍔熺巼鍒╃敤鐜?"""
    if None in (actual_power, rated_power) or rated_power <= 0:
        return None
    return (actual_power / rated_power) * 100


def calc_deviation_pct(actual: Optional[float], baseline: Optional[float]) -> Optional[float]:
    """璁＄畻鍋忕鐜?"""
    if None in (actual, baseline) or baseline == 0:
        return None
    return ((actual - baseline) / baseline) * 100


# ============================================================
# 鎸囨爣璁＄畻涓诲嚱鏁?# ============================================================

def compute_metrics(
    conn: pymysql.Connection,
    bucket_type: str = 'hour',
    start_time: Optional[str] = None,
    end_time: Optional[str] = None
) -> int:
    """Compute system-level metrics and write into metric_result."""
    table_name = 'agg_hour' if bucket_type == 'hour' else 'agg_day'
    if not start_time or not end_time:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT MIN(bucket_time) AS min_t, MAX(bucket_time) AS max_t FROM {table_name}")
            row = cursor.fetchone()
            if row and row["min_t"] and row["max_t"]:
                if not start_time:
                    start_time = str(row["min_t"])
                if not end_time:
                    end_time = str(row["max_t"])
            else:
                LOGGER.warning(f"No data found in {table_name}, skipping metric computation")
                return 0

    LOGGER.info(f"Computing metrics: {bucket_type} from {start_time} to {end_time}")

    # Keep existing loading behavior for compatibility.
    load_metric_definitions(conn)
    load_equipment_params(conn)

    db_metric_names = [
        'energy', 'power', 'chilled_supply_temp', 'chilled_return_temp',
        'cooling_supply_temp', 'cooling_return_temp', 'chilled_flow',
        'cooling_flow', 'load_rate', 'frequency', 'run_status', 'runtime'
    ]

    data_map = query_agg_data(conn, bucket_type, start_time, end_time, db_metric_names)
    LOGGER.info(f"Loaded {len(data_map)} aggregated data points")
    type_metric_index = build_type_metric_index(data_map)
    LOGGER.info(f"Built type-metric index entries={len(type_metric_index)}")

    time_systems = set()
    for data in data_map.values():
        bucket_str = (
            data['bucket_time'].strftime('%Y-%m-%d %H:%M:%S')
            if hasattr(data['bucket_time'], 'strftime')
            else str(data['bucket_time'])
        )
        time_systems.add((bucket_str, data['building_id'], data['system_id']))

    results: List[Tuple[Any, ...]] = []
    trace_id = f"CALC_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    for bucket_time, building_id, system_id in sorted(time_systems):
        chiller_energy = sum_metric_by_type(
            data_map,
            bucket_time,
            system_id,
            'chiller',
            'energy',
            'agg_delta',
            type_metric_index=type_metric_index,
        )
        chilled_pump_energy = sum_metric_by_type(
            data_map,
            bucket_time,
            system_id,
            'chilled_pump',
            'energy',
            'agg_delta',
            type_metric_index=type_metric_index,
        )
        cooling_pump_energy = sum_metric_by_type(
            data_map,
            bucket_time,
            system_id,
            'cooling_pump',
            'energy',
            'agg_delta',
            type_metric_index=type_metric_index,
        )
        tower_energy = sum_metric_by_type(
            data_map,
            bucket_time,
            system_id,
            'cooling_tower',
            'energy',
            'agg_delta',
            type_metric_index=type_metric_index,
        )
        tower_fan_energy = sum_metric_by_type(
            data_map,
            bucket_time,
            system_id,
            'tower_fan',
            'energy',
            'agg_delta',
            type_metric_index=type_metric_index,
        )

        total_tower_energy = (tower_energy or 0) + (tower_fan_energy or 0)
        if tower_energy is None and tower_fan_energy is None:
            total_tower_energy = None

        energy_parts = [chiller_energy, chilled_pump_energy, cooling_pump_energy, total_tower_energy]
        valid_parts = [e for e in energy_parts if e is not None]
        total_energy = sum(valid_parts) if valid_parts else None

        if total_energy is not None and total_energy > 0:
            results.append((
                bucket_time, bucket_type, building_id, system_id, None, None, None,
                'energy_structure', 'total_energy', 'total_energy',
                total_energy, 'value', None, None, 'kWh',
                'chiller + chilled_pump + cooling_pump + tower energy',
                json.dumps({
                    'chiller': chiller_energy,
                    'chilled_pump': chilled_pump_energy,
                    'cooling_pump': cooling_pump_energy,
                    'tower': total_tower_energy,
                }),
                trace_id, 3, 3, None, ''
            ))

            if chiller_energy is not None:
                ratio = calc_energy_ratio(chiller_energy, total_energy)
                if ratio is not None:
                    results.append((
                        bucket_time, bucket_type, building_id, system_id, None, None, None,
                        'energy_structure', 'chiller_energy_ratio', 'chiller_energy_ratio',
                        ratio, 'ratio', None, None, '%',
                        'chiller_energy / total_energy',
                        json.dumps({'chiller_energy': chiller_energy, 'total_energy': total_energy}),
                        trace_id, 3, 3, None, ''
                    ))

            pump_energy = (chilled_pump_energy or 0) + (cooling_pump_energy or 0)
            if chilled_pump_energy is not None or cooling_pump_energy is not None:
                ratio = calc_energy_ratio(pump_energy, total_energy)
                if ratio is not None:
                    results.append((
                        bucket_time, bucket_type, building_id, system_id, None, None, None,
                        'energy_structure', 'pump_energy_ratio', 'pump_energy_ratio',
                        ratio, 'ratio', None, None, '%',
                        '(chilled_pump_energy + cooling_pump_energy) / total_energy',
                        json.dumps({'pump_energy': pump_energy, 'total_energy': total_energy}),
                        trace_id, 3, 3, None, ''
                    ))

            if total_tower_energy is not None:
                ratio = calc_energy_ratio(total_tower_energy, total_energy)
                if ratio is not None:
                    results.append((
                        bucket_time, bucket_type, building_id, system_id, None, None, None,
                        'energy_structure', 'tower_energy_ratio', 'tower_energy_ratio',
                        ratio, 'ratio', None, None, '%',
                        'tower_energy / total_energy',
                        json.dumps({'tower_energy': total_tower_energy, 'total_energy': total_energy}),
                        trace_id, 3, 3, None, ''
                    ))

        chilled_supply = get_metric_value(data_map, bucket_time, system_id, 'system', None, None, 'chilled_supply_temp')
        chilled_return = get_metric_value(data_map, bucket_time, system_id, 'system', None, None, 'chilled_return_temp')
        chilled_delta_t = calc_delta_t(chilled_return, chilled_supply)
        if chilled_delta_t is not None:
            results.append((
                bucket_time, bucket_type, building_id, system_id, None, None, None,
                'temperature', 'chilled_water_delta_t', 'chilled_water_delta_t',
                chilled_delta_t, 'value', 5.0, calc_deviation_pct(chilled_delta_t, 5.0), 'C',
                'return - supply', json.dumps({'return': chilled_return, 'supply': chilled_supply}),
                trace_id, 3, 3, None, ''
            ))

        chilled_flow = get_metric_value(data_map, bucket_time, system_id, 'system', None, None, 'chilled_flow')
        if chilled_flow and chilled_delta_t and chilled_delta_t > 0:
            cooling_capacity = calc_cooling_capacity(chilled_flow, chilled_supply, chilled_return)
            if cooling_capacity:
                results.append((
                    bucket_time, bucket_type, building_id, system_id, None, None, None,
                    'flow', 'cooling_capacity', 'cooling_capacity',
                    cooling_capacity, 'value', None, None, 'kW',
                    'flow * delta_t * 4.186 / 3.6',
                    json.dumps({'flow': chilled_flow, 'delta_t': chilled_delta_t}),
                    trace_id, 3, 3, None, ''
                ))

                chiller_power = sum_metric_by_type(
                    data_map,
                    bucket_time,
                    system_id,
                    'chiller',
                    'power',
                    'agg_avg',
                    type_metric_index=type_metric_index,
                )
                if chiller_power and chiller_power > 0:
                    system_cop = cooling_capacity / chiller_power
                    results.append((
                        bucket_time, bucket_type, building_id, system_id, None, None, None,
                        'chiller_efficiency', 'chiller_cop', 'chiller_cop',
                        system_cop, 'ratio', None, None, None,
                        'cooling_capacity / chiller_power',
                        json.dumps({'cooling_capacity': cooling_capacity, 'chiller_power': chiller_power}),
                        trace_id, 3, 3, None, ''
                    ))

    if results:
        insert_sql = """
            INSERT INTO metric_result
            (bucket_time, bucket_type, building_id, system_id, equipment_type, equipment_id, sub_equipment_id,
             metric_category, metric_name, metric_code, value, stat_type, baseline_value, deviation_pct, unit,
             formula, data_sources, sql_trace_id, granularity_requested, granularity_actual, fallback_reason, quality_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value = VALUES(value), computed_at = CURRENT_TIMESTAMP
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, results)
        conn.commit()
        LOGGER.info(f"Inserted/updated {len(results)} metric results")

    return len(results)


def compute_equipment_metrics(
    conn: pymysql.Connection,
    bucket_type: str = 'hour',
    start_time: Optional[str] = None,
    end_time: Optional[str] = None
) -> int:
    """Compute equipment-level metrics (L5/L6)."""
    table_name = 'agg_hour' if bucket_type == 'hour' else 'agg_day'
    if not start_time or not end_time:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT MIN(bucket_time) AS min_t, MAX(bucket_time) AS max_t FROM {table_name}")
            row = cursor.fetchone()
            if row and row["min_t"] and row["max_t"]:
                if not start_time:
                    start_time = str(row["min_t"])
                if not end_time:
                    end_time = str(row["max_t"])
            else:
                LOGGER.warning(f"No data found in {table_name}")
                return 0

    LOGGER.info(f"Computing equipment-level metrics: {bucket_type} from {start_time} to {end_time}")
    equipment_params = load_equipment_params(conn)

    sql = f"""
        SELECT bucket_time, building_id, system_id, equipment_type,
               equipment_id, sub_equipment_id, metric_name,
               agg_avg, agg_delta, agg_max, agg_min
        FROM {table_name}
        WHERE bucket_time >= %s AND bucket_time < %s
          AND equipment_id IS NOT NULL
          AND metric_name IN ('power', 'energy', 'load_rate', 'frequency', 'run_status')
    """

    with conn.cursor() as cursor:
        cursor.execute(sql, (start_time, end_time))
        rows = cursor.fetchall()

    equipment_data: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = f"{row['bucket_time']}|{row['system_id']}|{row['equipment_type']}|{row['equipment_id']}"
        if key not in equipment_data:
            equipment_data[key] = []
        equipment_data[key].append(row)

    results: List[Tuple[Any, ...]] = []
    trace_id = f"CALC_EQ_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    for key, data_list in equipment_data.items():
        parts = key.split('|')
        bucket_time = parts[0]
        system_id = parts[1]
        equipment_type = parts[2]
        equipment_id = parts[3]

        metrics = {d['metric_name']: d for d in data_list}
        building_id = data_list[0]['building_id']
        sub_equipment_id = data_list[0].get('sub_equipment_id')

        param_key = f"{system_id}|{equipment_type}|{equipment_id}"
        params = equipment_params.get(param_key, {})
        rated_power = params.get('rated_power_kw')

        if 'power' in metrics and rated_power and rated_power > 0:
            actual_power = metrics['power'].get('agg_avg')
            if actual_power is not None:
                utilization = calc_power_utilization(actual_power, rated_power)
                if utilization is not None:
                    results.append((
                        bucket_time, bucket_type, building_id, system_id,
                        equipment_type, equipment_id, sub_equipment_id,
                        'equipment', 'power_utilization', 'power_utilization',
                        utilization, 'ratio', rated_power, None, '%',
                        'actual_power / rated_power',
                        json.dumps({'actual': actual_power, 'rated': rated_power}),
                        trace_id, 5, 5, None, ''
                    ))

    if results:
        insert_sql = """
            INSERT INTO metric_result
            (bucket_time, bucket_type, building_id, system_id, equipment_type, equipment_id, sub_equipment_id,
             metric_category, metric_name, metric_code, value, stat_type, baseline_value, deviation_pct, unit,
             formula, data_sources, sql_trace_id, granularity_requested, granularity_actual, fallback_reason, quality_flags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE value = VALUES(value), computed_at = CURRENT_TIMESTAMP
        """
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, results)
        conn.commit()
        LOGGER.info(f"Inserted/updated {len(results)} equipment-level metric results")

    return len(results)
