import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from 'react'
import { Copy, Check, X, ExternalLink as ExternalIcon } from 'lucide-react'

/**
 * Opening external links, defensively.
 *
 * This app is mostly a set of doors out to somewhere else — Google Maps, an
 * official site, a ticket page. That makes it unusually sensitive to *where it
 * is hosted*: a page embedded in a sandboxed iframe without the `allow-popups`
 * permission has every target="_blank" navigation and every window.open call
 * silently dropped. The tap does nothing and no error is raised, which is the
 * worst possible failure for a button you are relying on in a station.
 *
 * So we do not trust the anchor alone:
 *   1. The <a href> stays real, so a normal host, a long-press, and
 *      "copy link address" all behave exactly as expected.
 *   2. On click we attempt window.open inside the user gesture. If it returns a
 *      window we are done.
 *   3. If it returns null the environment has blocked us, and we show the
 *      destination so it can still be reached by hand.
 */

interface ExternalCtx {
  open: (url: string) => boolean
}

const Ctx = createContext<ExternalCtx>({
  open: () => false,
})

export function ExternalLinkProvider({ children }: { children: ReactNode }) {
  const [blockedUrl, setBlockedUrl] = useState<string | null>(null)

  const open = useCallback((url: string) => {
    let win: Window | null = null
    try {
      // Do NOT pass 'noopener' in the feature string: window.open then returns
      // null by specification, and we would read every successful open as a
      // block. Sever the opener afterwards instead.
      win = window.open(url, '_blank')
      if (win) {
        try {
          win.opener = null
        } catch {
          // Cross-origin windows may refuse this. Best effort only.
        }
      }
    } catch {
      win = null
    }
    if (win) return true
    setBlockedUrl(url)
    return false
  }, [])

  const value = useMemo(() => ({ open }), [open])

  return (
    <Ctx.Provider value={value}>
      {children}
      {blockedUrl && <BlockedSheet url={blockedUrl} onClose={() => setBlockedUrl(null)} />}
    </Ctx.Provider>
  )
}

export function useOpenExternal() {
  return useContext(Ctx).open
}

/**
 * An external link. Use this instead of a bare <a target="_blank"> anywhere in
 * the app, so a hostile hosting environment degrades to "here is the address"
 * rather than to nothing at all.
 */
export function Ext({
  href,
  children,
  className = '',
  ariaLabel,
  title,
}: {
  href: string
  children: ReactNode
  className?: string
  ariaLabel?: string
  title?: string
}) {
  const open = useOpenExternal()
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={ariaLabel}
      title={title}
      className={className}
      onClick={(e) => {
        // Modified clicks are the browser's business, not ours.
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button !== 0) return
        e.preventDefault()
        open(href)
      }}
    >
      {children}
    </a>
  )
}

function BlockedSheet({ url, onClose }: { url: string; onClose: () => void }) {
  const [copied, setCopied] = useState(false)

  async function copy() {
    try {
      await navigator.clipboard.writeText(url)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1800)
    } catch {
      // Clipboard is itself restricted in some embedded contexts. The address is
      // selectable on screen, which is the guarantee that matters.
    }
  }

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label="Link blocked"
      className="fixed inset-0 z-50 flex items-end justify-center bg-sumi-900/40 px-4 pb-4 backdrop-blur-sm"
      onClick={onClose}
    >
      <div
        className="w-full max-w-md rounded-card bg-surface p-5 shadow-lift"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="eyebrow">Link blocked</p>
            <h2 className="mt-1 text-[16px] font-semibold leading-snug text-sumi-800">
              This viewer will not open external links
            </h2>
          </div>
          <button type="button" onClick={onClose} aria-label="Close" className="icon-btn -mr-2 -mt-2">
            <X size={17} />
          </button>
        </div>

        <p className="mt-2.5 text-[12.5px] leading-relaxed text-sumi-500">
          The page is embedded in a frame that blocks new windows, so the tap cannot reach Google
          Maps. Open the app from its own address and every link works normally — including opening
          the Maps app straight from your phone.
        </p>

        <p className="mt-3 eyebrow">Destination</p>
        <p className="mt-1.5 max-h-28 overflow-y-auto break-all rounded-xl border border-sumi-200 bg-sumi-50 px-3 py-2.5 text-[11.5px] leading-relaxed text-sumi-600 [-webkit-user-select:text] [user-select:text]">
          {url}
        </p>

        <div className="mt-3 grid grid-cols-2 gap-2">
          <button type="button" onClick={copy} className="btn-secondary">
            {copied ? <Check size={14} className="text-sage-500" /> : <Copy size={14} />}
            {copied ? 'Copied' : 'Copy address'}
          </button>
          <a
            href={url}
            target="_top"
            rel="noopener noreferrer"
            className="btn-primary"
            onClick={onClose}
          >
            Try anyway <ExternalIcon size={13} className="opacity-60" />
          </a>
        </div>
      </div>
    </div>
  )
}
