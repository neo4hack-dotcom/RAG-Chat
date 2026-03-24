import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import path from 'path';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  // Keep the Vite dependency cache outside node_modules.
  // On some Windows setups, antivirus / indexing can lock node_modules/.vite
  // during the rename from deps_temp_* to deps, which breaks `npm run dev`.
  cacheDir: path.resolve(__dirname, '.vite-cache'),
  resolve: {
    alias: {
      '@': path.resolve(__dirname, '.'),
    },
  },
  server: {
    // HMR is disabled in some environments via DISABLE_HMR env var.
    hmr: process.env.DISABLE_HMR !== 'true',
    // Proxy /api requests to the Python backend during development.
    proxy: {
      '/api': {
        target: `http://localhost:${process.env.BACKEND_PORT || 8000}`,
        changeOrigin: true,
      },
    },
  },
});
