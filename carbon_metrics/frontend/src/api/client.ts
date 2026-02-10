import axios from 'axios';

const apiClient = axios.create({
  baseURL: '/api',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

apiClient.interceptors.response.use(
  (response) => response.data,
  (error) => {
    const message =
      error.response?.data?.detail || error.message || '请求失败';
    return Promise.reject(new Error(message));
  },
);

export default apiClient;
