// Hoisted Intl construction site (ADR-0035): formatters here are module-level
// singletons. Per-row/per-cell code elsewhere in the consent-viz tree must
// call these helpers, never construct its own Intl formatter.

const BERLIN_TIMESTAMP_FORMAT = new Intl.DateTimeFormat('de-DE', {
  timeZone: 'Europe/Berlin',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false
})

export function formatBerlinTimestamp (date: Date): string {
  return BERLIN_TIMESTAMP_FORMAT.format(date)
}
