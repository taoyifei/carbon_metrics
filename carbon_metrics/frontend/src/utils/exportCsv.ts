/**
 * 前端 CSV 导出工具 — 支持中文（BOM）、逗号/换行转义
 */

interface CsvColumn<T extends object> {
  title: string;
  dataIndex: keyof T;
  render?: (value: unknown, record: T) => string;
}

export function exportToCsv<T extends object>(
  filename: string,
  columns: CsvColumn<T>[],
  data: T[],
) {
  const escape = (v: unknown): string => {
    const s = v == null ? '' : String(v);
    if (s.includes(',') || s.includes('"') || s.includes('\n')) {
      return `"${s.replace(/"/g, '""')}"`;
    }
    return s;
  };

  const header = columns.map((c) => escape(c.title)).join(',');
  const rows = data.map((row) =>
    columns
      .map((c) => {
        const raw = row[c.dataIndex];
        const val = c.render ? c.render(raw, row) : raw;
        return escape(val);
      })
      .join(','),
  );

  const bom = '\uFEFF';
  const csv = bom + [header, ...rows].join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
