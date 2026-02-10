import { useState } from 'react';
import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import { Layout, Menu, theme } from 'antd';
import {
  HomeOutlined,
  BarChartOutlined,
  DatabaseOutlined,
} from '@ant-design/icons';

const { Sider, Header, Content } = Layout;

const menuItems = [
  {
    key: '/',
    icon: <HomeOutlined />,
    label: '总览',
  },
  {
    key: '/metrics',
    icon: <BarChartOutlined />,
    label: '指标分析',
  },
  {
    key: '/quality',
    icon: <DatabaseOutlined />,
    label: '数据质量',
  },
];

export default function AppLayout() {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { token } = theme.useToken();

  const handleMenuNavigate = (pathname: string) => {
    const currentParams = new URLSearchParams(location.search);
    const nextParams = new URLSearchParams();
    const timeStart = currentParams.get('time_start');
    const timeEnd = currentParams.get('time_end');

    if (timeStart) {
      nextParams.set('time_start', timeStart);
    }
    if (timeEnd) {
      nextParams.set('time_end', timeEnd);
    }

    const search = nextParams.toString();
    navigate({
      pathname,
      search: search ? `?${search}` : '',
    });
  };

  const selectedKey = location.pathname.startsWith('/quality')
    ? '/quality'
    : location.pathname.startsWith('/metrics')
      ? '/metrics'
      : location.pathname || '/';

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        style={{ background: token.colorBgContainer }}
      >
        <div
          style={{
            height: 48,
            margin: 12,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontWeight: 600,
            fontSize: collapsed ? 14 : 16,
            color: token.colorTextHeading,
            whiteSpace: 'nowrap',
            overflow: 'hidden',
          }}
        >
          {collapsed ? '指标' : '制冷系统指标平台'}
        </div>
        <Menu
          mode="inline"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={({ key }) => handleMenuNavigate(key)}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            padding: '0 24px',
            background: token.colorBgContainer,
            borderBottom: `1px solid ${token.colorBorderSecondary}`,
            display: 'flex',
            alignItems: 'center',
            fontSize: 16,
            fontWeight: 500,
          }}
        >
          制冷系统能耗数据平台
        </Header>
        <Content style={{ margin: 24 }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
