const PREFIX = 'jtc:v1:'

/**
 * Thin localStorage wrapper. Everything the app remembers lives here:
 * completed activities, skips, reordering, favourites, notes, checklist ticks.
 * No backend, and a private-mode failure degrades to in-memory rather than
 * throwing on every keystroke.
 */
const memory = new Map<string, string>()

function available(): boolean {
  try {
    const k = `${PREFIX}__probe`
    window.localStorage.setItem(k, '1')
    window.localStorage.removeItem(k)
    return true
  } catch {
    return false
  }
}

const canUse = typeof window !== 'undefined' && available()

export function load<T>(key: string, fallback: T): T {
  try {
    const raw = canUse ? window.localStorage.getItem(PREFIX + key) : memory.get(PREFIX + key)
    if (raw == null) return fallback
    return JSON.parse(raw) as T
  } catch {
    return fallback
  }
}

export function save<T>(key: string, value: T): void {
  try {
    const raw = JSON.stringify(value)
    if (canUse) window.localStorage.setItem(PREFIX + key, raw)
    else memory.set(PREFIX + key, raw)
  } catch {
    // Out of quota or serialisation failure — not worth breaking the UI over.
  }
}

export function clearAll(): void {
  try {
    if (canUse) {
      const doomed: string[] = []
      for (let i = 0; i < window.localStorage.length; i++) {
        const k = window.localStorage.key(i)
        if (k && k.startsWith(PREFIX)) doomed.push(k)
      }
      doomed.forEach((k) => window.localStorage.removeItem(k))
    }
    memory.clear()
  } catch {
    /* ignore */
  }
}
