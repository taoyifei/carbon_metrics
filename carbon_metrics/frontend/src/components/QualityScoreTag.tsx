import { Tag } from 'antd';

interface Props {
  score: number;
}

export default function QualityScoreTag({ score }: Props) {
  let color: string;
  if (score >= 80) color = '#52c41a';
  else if (score >= 60) color = '#faad14';
  else color = '#ff4d4f';

  return <Tag color={color}>{score.toFixed(1)}</Tag>;
}
