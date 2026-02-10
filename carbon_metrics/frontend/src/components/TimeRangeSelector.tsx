import { DatePicker, Space, Button } from 'antd';
import dayjs, { type Dayjs } from 'dayjs';

const { RangePicker } = DatePicker;

interface Props {
  value: [string, string];
  onChange: (range: [string, string]) => void;
}

const PRESETS: { label: string; value: [Dayjs, Dayjs] }[] = [
  {
    label: '最近1天',
    value: [dayjs().subtract(1, 'day'), dayjs()],
  },
  {
    label: '最近7天',
    value: [dayjs().subtract(7, 'day'), dayjs()],
  },
  {
    label: '最近30天',
    value: [dayjs().subtract(30, 'day'), dayjs()],
  },
  {
    label: '最近3个月',
    value: [dayjs().subtract(3, 'month'), dayjs()],
  },
];

export default function TimeRangeSelector({ value, onChange }: Props) {
  const handleChange = (dates: [Dayjs | null, Dayjs | null] | null) => {
    if (dates && dates[0] && dates[1]) {
      onChange([
        dates[0].format('YYYY-MM-DDTHH:mm:ss'),
        dates[1].format('YYYY-MM-DDTHH:mm:ss'),
      ]);
    }
  };

  const handlePreset = (preset: [Dayjs, Dayjs]) => {
    onChange([
      preset[0].format('YYYY-MM-DDTHH:mm:ss'),
      preset[1].format('YYYY-MM-DDTHH:mm:ss'),
    ]);
  };

  return (
    <Space wrap>
      <RangePicker
        showTime
        value={[dayjs(value[0]), dayjs(value[1])]}
        onChange={handleChange as never}
        format="YYYY-MM-DD HH:mm"
      />
      {PRESETS.map((p) => (
        <Button
          key={p.label}
          size="small"
          onClick={() => handlePreset(p.value)}
        >
          {p.label}
        </Button>
      ))}
    </Space>
  );
}
