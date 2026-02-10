import { Card, Skeleton } from 'antd';

export default function LoadingCard() {
  return (
    <Card>
      <Skeleton active paragraph={{ rows: 3 }} />
    </Card>
  );
}
