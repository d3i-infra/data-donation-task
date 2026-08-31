import { defineConfig, type Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

// Dev-only sink for FakeBridge's `POST /data-submission` (packages/feldspar/
// src/fake_bridge.ts). Without this, a bare `pnpm start` run 404s at the
// donate step: the Vite dev server has no such route by default. Playwright
// specs (tests/*.spec.ts) never hit this — each registers its own
// `page.route('/data-submission', ...)` stub before driving the flow, which
// intercepts the request before it reaches the dev server at all — so this
// exists only for manual/browser dev runs outside Playwright.
//
// `apply: 'serve'` is belt-and-suspenders with the fact that Vite only ever
// calls `configureServer` for the dev/preview server, never for `vite
// build` — this plugin cannot affect production output either way.
function devDonateSinkPlugin (): Plugin {
  return {
    name: 'dev-donate-sink',
    apply: 'serve',
    configureServer (server) {
      server.middlewares.use('/data-submission', (req, res, next) => {
        if (req.method !== 'POST') {
          next()
          return
        }
        let body = ''
        req.on('data', (chunk) => { body += chunk })
        req.on('end', () => {
          let key = '(unparseable body)'
          try {
            const parsed = JSON.parse(body)
            key = typeof parsed.key === 'string' ? parsed.key : '(no key)'
          } catch {
            // leave the fallback message — never crash the dev server on a
            // malformed body
          }
          console.log(`[dev-donate-sink] POST /data-submission key=${key} bytes=${body.length}`)
          res.statusCode = 200
          res.setHeader('Content-Type', 'application/json')
          res.end(JSON.stringify({ ok: true }))
        })
      })
    }
  }
}

// https://vitejs.dev/config/
export default defineConfig({
  base: './',
  plugins: [
    react({
      // Include JSX runtime automatically
      jsxRuntime: 'automatic',
      // Include feldspar source files for Fast Refresh
      include: [
        '**/*.tsx',
        '**/*.ts',
        '../feldspar/src/**/*.tsx',
        '../feldspar/src/**/*.ts'
      ]
    }),
    devDonateSinkPlugin()
  ],
  server: {
    port: 3000,
    open: true,
    host: true,
    // Watch feldspar source files for changes
    fs: {
      allow: ['..'] // Allow serving files from parent directories
    },
    watch: {
      // Watch feldspar source directory for changes
      ignored: ['!**/packages/feldspar/src/**']
    }
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    rollupOptions: {
      output: {
        manualChunks: (id) => {
          if (id.includes('react-dom') || id.includes('react')) return 'vendor'
          if (id.includes('@eyra/feldspar')) return 'feldspar'
        }
      }
    }
  },
  optimizeDeps: {
    // Exclude feldspar from pre-bundling so it gets processed by Vite directly
    exclude: ['@eyra/feldspar']
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src')
      // Remove the direct source alias for now to use built version
    }
  },
  publicDir: 'public',
  // Ensure compatibility with Python worker
  assetsInclude: ['**/*.whl', '**/*.tar.gz']
})
