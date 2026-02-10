import { Routes, Route } from 'react-router-dom';
import AppLayout from './layouts/AppLayout';
import Dashboard from './pages/Dashboard';
import QualityPage from './pages/Quality';
import MetricsPage from './pages/Metrics';
import EquipmentDetail from './pages/Equipment/EquipmentDetail';

function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/metrics" element={<MetricsPage />} />
        <Route path="/quality" element={<QualityPage />} />
        <Route path="/quality/equipment/:equipmentId" element={<EquipmentDetail />} />
      </Route>
    </Routes>
  );
}

export default App;
