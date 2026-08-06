import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      "/api": "http://localhost:8080",
    },
  },
  build: {
    outDir: "../pkg/baton/explorer/frontend",
    emptyOutDir: true,
    rolldownOptions: {
      output: {
        codeSplitting: {
          groups: [
            {
              name: "react",
              test: /node_modules\/(?:react|react-dom|react-router)\//,
            },
            {
              name: "mui",
              test: /node_modules\/@mui\//,
            },
            {
              name: "reactflow",
              test: /node_modules\/(?:reactflow|@reactflow)\//,
            },
          ],
        },
      },
    },
  },
});
