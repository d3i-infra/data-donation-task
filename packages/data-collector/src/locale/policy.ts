import uiLocales from './ui_locales.json'

export const SUPPORTED_UI_LOCALES = uiLocales.supported
export const DEFAULT_UI_LOCALE = uiLocales.default
export const PROVISIONAL_UI_LOCALES = uiLocales.provisional

export function normalizeLocale (raw: unknown): string {
  if (typeof raw !== 'string') return DEFAULT_UI_LOCALE
  const primary = raw.trim().toLowerCase().split(/[-_]/)[0]
  return SUPPORTED_UI_LOCALES.includes(primary) ? primary : DEFAULT_UI_LOCALE
}
