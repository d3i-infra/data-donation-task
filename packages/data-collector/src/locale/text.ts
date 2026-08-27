import { Translator, MISSING_TRANSLATION } from '@eyra/feldspar'

// The fork's single entry point for turning researcher-supplied text into a
// string for the current locale. Everything routes through feldspar's
// Translator (exact locale -> default locale -> first available -> sentinel);
// the only thing added here is a shape guard.
//
// The guard is load-bearing. Translator.translate throws a TypeError on input
// that is neither a string nor a Translatable — reasonable for an entry point,
// fatal for us: this text comes from study config JSON, so a typo would take
// the participant's page down. These functions are total: bad input yields
// MISSING_TRANSLATION ('?text?'), never an exception.
//
// Note that an *empty* translation is a deliberate hit, not a miss — only a
// missing key falls back.

function isRecord (value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/**
 * Resolve nested text — a `{ translations: { <locale>: string } }` object
 * (which includes every `TextBundle`) or a plain string.
 */
export function resolveText (text: unknown, locale: string): string {
  if (typeof text === 'string') return text
  if (!isRecord(text) || !isRecord(text.translations)) return MISSING_TRANSLATION
  return Translator.translate(text as { translations: Record<string, string> }, locale)
}

/**
 * Resolve flat text — the `{ <locale>: string }` shape the study config JSON
 * uses for visualization labels. Plain strings pass through, so call sites can
 * hand over a `label ?? id` fallback without checking which they got.
 */
export function resolveFlatText (flat: unknown, locale: string): string {
  if (typeof flat === 'string') return flat
  if (!isRecord(flat)) return MISSING_TRANSLATION
  return resolveText({ translations: flat }, locale)
}

/**
 * Resolve a whole string table at once; every key of `bundles` is kept.
 * Takes `unknown` for the same reason the other two do: the TypeScript type is
 * a promise about compile time, not a guarantee about what actually arrives.
 * A container that is not a record yields an empty table, never a throw.
 */
export function resolveAll (
  bundles: unknown,
  locale: string
): Record<string, string> {
  if (!isRecord(bundles)) return {}
  const resolved: Record<string, string> = {}
  for (const [key, value] of Object.entries(bundles)) {
    resolved[key] = resolveText(value, locale)
  }
  return resolved
}
