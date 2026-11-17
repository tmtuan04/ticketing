import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

// Gateway và payment không bật CORS, nên trình duyệt gọi cùng origin rồi để dev server chuyển tiếp:
//   /api/* -> gateway (3000), /pay/* -> payment (3001). Trong compose, nginx làm đúng việc này.
const GATEWAY = process.env.GATEWAY_URL ?? 'http://localhost:3000';
const PAYMENT = process.env.PAYMENT_URL ?? 'http://localhost:3001';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target: GATEWAY, changeOrigin: true, rewrite: (p) => p.replace(/^\/api/, '') },
      '/pay': { target: PAYMENT, changeOrigin: true, rewrite: (p) => p.replace(/^\/pay/, '') },
    },
  },
});
