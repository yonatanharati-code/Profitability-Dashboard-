/**
 * Bundles dist/ into one self-contained HTML file.
 *
 * The Artifact host serves a single file under a strict CSP and supplies its own
 * <head>, so this script:
 *   - inlines the CSS and the JS module (no external asset requests)
 *   - emits page content only: no doctype, <html>, <head> or <body>
 *   - puts <title> first, since only the first 8KB is scanned for it
 *   - injects the viewport meta at runtime, because we cannot write <head>
 *
 * Google Fonts is the one external host the CSP allows, so the @import in
 * index.css keeps working. Open-Meteo does not, so the weather block falls back
 * to its labelled September averages — which it is built to do.
 */
import { readFileSync, writeFileSync, readdirSync } from 'node:fs'
import { join, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const dist = join(root, 'dist')
const assets = readdirSync(join(dist, 'assets'))

const cssFile = assets.find((f) => f.endsWith('.css'))
const jsFile = assets.find((f) => f.endsWith('.js'))
if (!cssFile || !jsFile) throw new Error('Run `npm run build` first — dist/assets is incomplete.')

const css = readFileSync(join(dist, 'assets', cssFile), 'utf8')
let js = readFileSync(join(dist, 'assets', jsFile), 'utf8')

// A literal </script> anywhere in the bundle would close the inline tag early.
if (js.includes('</script')) {
  throw new Error('Bundle contains a literal </script>; inlining would break the page.')
}

// The service worker cannot resolve on the artifact host, and a failed
// registration is just console noise. Strip the registration block.
js = js.replace(/"serviceWorker"in navigator/g, 'false')

const out = `<title>Japan 2026</title>

<script>
  // The host supplies <head>, so the mobile viewport has to be set from here.
  document.head.insertAdjacentHTML(
    'beforeend',
    '<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">'
  )
</script>

<style>
${css}
</style>

<div id="root"></div>

<script type="module">
${js}
</script>
`

const target = join(root, 'dist', 'japan-2026.html')
writeFileSync(target, out)
console.log(`Wrote ${target} — ${(out.length / 1024).toFixed(0)} KB`)
console.log(`  css ${(css.length / 1024).toFixed(0)} KB, js ${(js.length / 1024).toFixed(0)} KB`)
