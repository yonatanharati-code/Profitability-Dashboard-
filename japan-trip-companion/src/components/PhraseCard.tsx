import { useState } from 'react'
import { Check, Copy } from 'lucide-react'
import type { Phrase } from '../data/phrases'

/**
 * A phrase you can hand to someone. The Japanese is the largest thing on the
 * card because showing it is the whole technique — reading it aloud is the
 * fallback, not the plan.
 */
export function PhraseCard({ phrase }: { phrase: Phrase }) {
  const [copied, setCopied] = useState(false)

  async function copy() {
    try {
      await navigator.clipboard.writeText(phrase.jp)
      setCopied(true)
      window.setTimeout(() => setCopied(false), 1600)
    } catch {
      // Clipboard is blocked in some embedded contexts; the text is on screen
      // anyway, which is what actually matters here.
    }
  }

  return (
    <article className="card px-4 py-3.5">
      <div className="flex items-start justify-between gap-3">
        <p className="min-w-0 flex-1 font-serif text-[19px] leading-[1.55] text-sumi-800">
          {phrase.jp}
        </p>
        <button
          type="button"
          onClick={copy}
          aria-label={`Copy "${phrase.en}" in Japanese`}
          className="icon-btn -mr-2 -mt-1.5 shrink-0"
        >
          {copied ? <Check size={16} className="text-sage-500" /> : <Copy size={15} />}
        </button>
      </div>

      <p className="mt-1.5 text-[12px] italic tracking-wide text-sumi-400">{phrase.romaji}</p>
      <p className="mt-2 text-[13.5px] font-semibold leading-snug text-sumi-700">{phrase.en}</p>

      {phrase.note && (
        <p className="mt-2 border-t border-sumi-100 pt-2 text-[12px] leading-relaxed text-sumi-400">
          {phrase.note}
        </p>
      )}
    </article>
  )
}
