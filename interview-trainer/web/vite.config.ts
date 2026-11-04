import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import react from '@vitejs/plugin-react';
import { defineConfig, type Plugin } from 'vite';
// Relative import on purpose: workspace packages are externalised when bundling this config.
import { contentFiles, loadCourse } from '../shared/src/load-content';

const contentDir = resolve(fileURLToPath(new URL('.', import.meta.url)), '../content');

/** Exposes the validated YAML course as `import course from 'virtual:course'`. */
function courseContent(): Plugin {
  const id = 'virtual:course';
  const resolved = '\0' + id;
  return {
    name: 'course-content',
    resolveId: (source) => (source === id ? resolved : null),
    load(source) {
      if (source !== resolved) return null;
      for (const file of contentFiles(contentDir)) this.addWatchFile(file);
      return `export default ${JSON.stringify(loadCourse(contentDir))};`;
    },
    configureServer(server) {
      server.watcher.add(contentDir);
      server.watcher.on('change', (file) => {
        if (!file.startsWith(contentDir)) return;
        const mod = server.moduleGraph.getModuleById(resolved);
        if (mod) server.moduleGraph.invalidateModule(mod);
        server.ws.send({ type: 'full-reload' });
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), courseContent()],
  server: { host: true, proxy: { '/api': 'http://localhost:8787' } },
});
