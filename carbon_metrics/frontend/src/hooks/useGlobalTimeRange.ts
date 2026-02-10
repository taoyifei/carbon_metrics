import { useCallback, useMemo } from 'react';
import dayjs from 'dayjs';
import { useSearchParams } from 'react-router-dom';

export type TimeRangeValue = [string, string];

const TIME_FORMAT = 'YYYY-MM-DDTHH:mm:ss';

function buildDefaultRange(defaultDays: number): TimeRangeValue {
  return [
    dayjs().subtract(defaultDays, 'day').format(TIME_FORMAT),
    dayjs().format(TIME_FORMAT),
  ];
}

function isValidDateTime(value: string | null): value is string {
  return !!value && dayjs(value).isValid();
}

export function useGlobalTimeRange(defaultDays = 7): [TimeRangeValue, (range: TimeRangeValue) => void] {
  const [searchParams, setSearchParams] = useSearchParams();

  const defaultRange = useMemo(
    () => buildDefaultRange(defaultDays),
    [defaultDays],
  );

  const timeRange = useMemo<TimeRangeValue>(() => {
    const timeStart = searchParams.get('time_start');
    const timeEnd = searchParams.get('time_end');
    if (isValidDateTime(timeStart) && isValidDateTime(timeEnd)) {
      return [timeStart, timeEnd];
    }
    return defaultRange;
  }, [defaultRange, searchParams]);

  const setTimeRange = useCallback(
    (range: TimeRangeValue) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        next.set('time_start', range[0]);
        next.set('time_end', range[1]);
        return next;
      }, { replace: true });
    },
    [setSearchParams],
  );

  return [timeRange, setTimeRange];
}
