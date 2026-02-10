import { Alert } from 'antd';

interface Props {
  message: string;
}

export default function ErrorAlert({ message }: Props) {
  return (
    <Alert
      type="error"
      showIcon
      message="请求失败"
      description={message}
      style={{ marginBottom: 16 }}
    />
  );
}
