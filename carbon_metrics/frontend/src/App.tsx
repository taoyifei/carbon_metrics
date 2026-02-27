import { Routes, Route, useLocation } from 'react-router-dom';
import AppLayout from './layouts/AppLayout';
import Dashboard from './pages/Dashboard';
import QualityPage from './pages/Quality';
import MetricsPage from './pages/Metrics';
import EquipmentDetail from './pages/Equipment/EquipmentDetail';
import ErrorBoundary from './components/ErrorBoundary';

function App() {
  const location = useLocation();
  return (
    <ErrorBoundary resetKey={location.pathname}>
      <Routes>
        <Route element={<AppLayout />}>
          <Route path="/" element={<Dashboard />} />
          <Route path="/metrics" element={<MetricsPage />} />
          <Route path="/quality" element={<QualityPage />} />
          <Route path="/quality/equipment/:equipmentId" element={<EquipmentDetail />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}

export default App;
