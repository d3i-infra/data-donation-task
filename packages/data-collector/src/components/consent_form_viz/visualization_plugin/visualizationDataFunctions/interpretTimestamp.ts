import { DateColumnSpec, DateEncoding } from '../types'

export type Interpretation =
  | { status: 'instant', epochMs: number }
  | { status: 'local', wallClockMs: number }
  | { status: 'empty' }
  | { status: 'invalid' }
  | { status: 'ambiguous' }
  | { status: 'unsupported' }

interface InterpretContext { locale?: string }

// ---------------------------------------------------------------------------
// Hoisted tables (ADR-0035): built once, cached per locale / zone.
// ---------------------------------------------------------------------------

/** Unambiguous zone abbreviations Takeout writes, in minutes ahead of UTC. Anything not here
 *  (CST, IST, BST, EST, PST, …) means several zones and is never guessed. */
export const ZONE_OFFSET_MINUTES: Readonly<Record<string, number>> = Object.freeze({
  UTC: 0, GMT: 0, Z: 0, WET: 0, WEST: 60, CET: 60, CEST: 120, EET: 120, EEST: 180, MEZ: 60, MESZ: 120
})

const NUMERIC_ZONE = /^(?:GMT|UTC)([+-])(\d{1,2})(?::?(\d{2}))?$/i
const EMPTY = /^(?:\s*|none|null|nan)$/i

const monthTables = new Map<string, Map<string, number>>()

/** Month names for a locale, lowercased, trailing period stripped, short and long forms. */
// Every table also carries the English names from both major English locales: Google's
// English render abbreviates September as "Sept" (en-GB) where en-US data says "Sep", and
// exports in other locales carry stray English month names ("11 may 2020" in a Dutch export).
// Real Takeout sets showed both; without the merge those rows are left unplaced.
const ENGLISH_FALLBACK_LOCALES = ['en-GB', 'en-US'] as const

function addMonthNames (table: Map<string, number>, locale: string): void {
  for (const style of ['short', 'long'] as const) {
    const fmt = new Intl.DateTimeFormat(locale, { month: style, timeZone: 'UTC' })
    for (let m = 0; m < 12; m++) {
      const name = fmt.format(new Date(Date.UTC(2026, m, 15))).toLowerCase().replace(/\.$/, '')
      if (!table.has(name)) table.set(name, m + 1)
    }
  }
}

export function monthTable (locale: string): Map<string, number> {
  let table = monthTables.get(locale)
  if (table !== undefined) return table
  table = new Map()
  addMonthNames(table, locale)
  for (const fallback of ENGLISH_FALLBACK_LOCALES) addMonthNames(table, fallback)
  monthTables.set(locale, table)
  return table
}

const zoneFormatters = new Map<string, Intl.DateTimeFormat>()

/** The wall clock an instant shows in `timeZone`, as a UTC-space millisecond value, so that
 *  getUTC* getters and UTC formatters read it as that wall clock. */
export function wallClockInZone (epochMs: number, timeZone: string): number {
  let fmt = zoneFormatters.get(timeZone)
  if (fmt === undefined) {
    fmt = new Intl.DateTimeFormat('en-US', {
      timeZone, hourCycle: 'h23', year: 'numeric', month: 'numeric', day: 'numeric',
      hour: 'numeric', minute: 'numeric', second: 'numeric'
    })
    zoneFormatters.set(timeZone, fmt)
  }
  const p: Record<string, number> = {}
  for (const part of fmt.formatToParts(new Date(epochMs))) {
    if (part.type !== 'literal') p[part.type] = Number(part.value)
  }
  const ms = epochMs - Math.floor(epochMs / 1000) * 1000
  return Date.UTC(p.year, p.month - 1, p.day, p.hour === 24 ? 0 : p.hour, p.minute, p.second, ms)
}

// ---------------------------------------------------------------------------
// Shape parsers. Each returns null when the shape does not match, 'invalid' when it matches
// but the calendar rejects it, otherwise an interpretation. Never `new Date(string)`.
// ---------------------------------------------------------------------------

type Parsed = Interpretation | null

function utcOrInvalid (y: number, mo: number, d: number, h: number, mi: number, s: number, ms = 0): number | 'invalid' {
  if (mo < 1 || mo > 12 || d < 1 || d > 31 || Number.isNaN(h) || h > 23 || mi > 59 || s > 59) return 'invalid'
  const t = Date.UTC(y, mo - 1, d, h, mi, s, ms)
  const back = new Date(t)
  if (back.getUTCFullYear() !== y || back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return 'invalid'
  return t
}

function place (wall: number | 'invalid', offsetMinutes: number | 'invalid' | null): Parsed {
  if (wall === 'invalid' || offsetMinutes === 'invalid') return { status: 'invalid' }
  if (offsetMinutes === null) return { status: 'local', wallClockMs: wall }
  return { status: 'instant', epochMs: wall - offsetMinutes * 60_000 }
}

/** A written UTC offset in minutes; 'invalid' beyond 59 minutes or 14 hours, the widest in use. */
function numericOffset (sign: string, hours: number, minutes: number): number | 'invalid' {
  if (minutes > 59 || hours * 60 + minutes > 14 * 60) return 'invalid'
  const total = hours * 60 + minutes
  return sign === '-' ? -total : total
}

function zoneOffset (token: string | undefined): number | 'invalid' | null {
  if (token === undefined || token === '') return null
  const known = ZONE_OFFSET_MINUTES[token.toUpperCase()]
  if (known !== undefined) return known
  const m = NUMERIC_ZONE.exec(token)
  if (m === null) return null
  return numericOffset(m[1], Number(m[2]), Number(m[3] ?? 0))
}

/** The 24-hour value of a clock hour; NaN when a 12-hour marker sits beside an hour outside 1-12. */
function meridiemHour (hour: number, marker: string | undefined): number {
  if (marker === undefined) return hour
  if (hour < 1 || hour > 12) return NaN
  const pm = /^[pم]/i.test(marker)
  return (hour % 12) + (pm ? 12 : 0)
}

const EPOCH = /^-?\d+(?:\.\d+)?$/

function parseEpochSeconds (raw: string): Parsed {
  if (!EPOCH.test(raw)) return null
  const ms = Math.round(Number(raw) * 1000)
  if (!Number.isFinite(ms) || Math.abs(ms) > 8.64e15) return { status: 'invalid' }
  return { status: 'instant', epochMs: ms }
}

function parseEpochMicros (raw: string): Parsed {
  if (!/^-?\d+$/.test(raw)) return null
  const ms = Math.floor(Number(raw) / 1000)
  if (!Number.isFinite(ms) || Math.abs(ms) > 8.64e15) return { status: 'invalid' }
  return { status: 'instant', epochMs: ms }
}

const ISO = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?\s*(Z|[+-]\d{2}:?\d{2})?$/i

function parseIso (raw: string, spec: DateColumnSpec): Parsed {
  const m = ISO.exec(raw)
  if (m === null) return null
  const ms = m[7] !== undefined ? Number((m[7] + '000').slice(0, 3)) : 0
  const wall = utcOrInvalid(+m[1], +m[2], +m[3], +m[4], +m[5], m[6] !== undefined ? +m[6] : 0, ms)
  if (m[8] === undefined) return place(wall, spec.utcOffsetMinutes ?? null)
  if (/^z$/i.test(m[8])) return place(wall, 0)
  const digits = m[8].slice(1).replace(':', '')
  return place(wall, numericOffset(m[8][0], Number(digits.slice(0, 2)), Number(digits.slice(2))))
}

const TIKTOK = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\s+(?:UTC|GMT))?$/i

function parseTiktok (raw: string): Parsed {
  const m = TIKTOK.exec(raw)
  if (m === null) return null
  return place(utcOrInvalid(+m[1], +m[2], +m[3], +m[4], +m[5], +m[6]), 0)
}

// "Mar 02, 2026 4:57:45 pm" — Meta's html display clock, English month, 12-hour, no zone.
const META_HTML = /^([A-Za-z]{3}) (\d{1,2}), (\d{4}) (\d{1,2}):(\d{2})(?::(\d{2}))? ?([AaPp])\.?[Mm]\.?$/

function parseMetaHtml (raw: string, spec: DateColumnSpec): Parsed {
  const m = META_HTML.exec(raw)
  if (m === null) return null
  const month = monthTable('en').get(m[1].toLowerCase())
  if (month === undefined) return { status: 'invalid' }
  const wall = utcOrInvalid(+m[3], month, +m[2], meridiemHour(+m[4], m[7]), +m[5], m[6] !== undefined ? +m[6] : 0)
  return place(wall, spec.utcOffsetMinutes ?? null)
}

// Takeout's five rendered shapes; the trailing group is the zone token, if any.
const TAIL = '(?:\\s+(\\S+))?\\s*$'
const DAY_FIRST = new RegExp('^(\\d{1,2})\\.? ([^\\s,\\d]+?)\\.?,? (\\d{4}),? (\\d{1,2}):(\\d{2}):(\\d{2})' + TAIL)
const MONTH_FIRST = new RegExp('^([^\\s,\\d]+?)\\.? (\\d{1,2}), (\\d{4}), (\\d{1,2}):(\\d{2}):(\\d{2})(?:\\s*([AaPp])\\.?[Mm])?' + TAIL)
const NUMERIC_DAY_FIRST = new RegExp('^(\\d{1,2})\\.(\\d{1,2})\\.(\\d{4}),? (\\d{1,2}):(\\d{2}):(\\d{2})' + TAIL)
const CJK = new RegExp('^(\\d{4})年(\\d{1,2})月(\\d{1,2})日,? (\\d{1,2}):(\\d{2}):(\\d{2})' + TAIL)
const ARABIC = new RegExp('^(\\d{1,2})\\u200f?/(\\d{1,2})\\u200f?/(\\d{4})، (\\d{1,2}):(\\d{2}):(\\d{2}) ([صم])' + TAIL)

function parseTakeoutHtml (raw: string, ctx: InterpretContext): Parsed {
  const locale = ctx.locale
  let m: RegExpExecArray | null
  if ((m = NUMERIC_DAY_FIRST.exec(raw)) !== null) {
    return place(utcOrInvalid(+m[3], +m[2], +m[1], +m[4], +m[5], +m[6]), zoneOffset(m[7]))
  }
  if ((m = CJK.exec(raw)) !== null) {
    return place(utcOrInvalid(+m[1], +m[2], +m[3], +m[4], +m[5], +m[6]), zoneOffset(m[7]))
  }
  if ((m = ARABIC.exec(raw)) !== null) {
    return place(utcOrInvalid(+m[3], +m[2], +m[1], meridiemHour(+m[4], m[7]), +m[5], +m[6]), zoneOffset(m[8]))
  }
  // Below here a month name is needed. No locale, or a name the locale does not know, means
  // this encoding does not recognise the value: return null so a sibling encoding may, and
  // the caller reports `unsupported` only when none does.
  if (locale === undefined) return null
  const months = monthTable(locale)
  if ((m = DAY_FIRST.exec(raw)) !== null) {
    const month = months.get(m[2].toLowerCase())
    if (month === undefined) return null
    return place(utcOrInvalid(+m[3], month, +m[1], +m[4], +m[5], +m[6]), zoneOffset(m[7]))
  }
  if ((m = MONTH_FIRST.exec(raw)) !== null) {
    const month = months.get(m[1].toLowerCase())
    if (month === undefined) return null
    return place(utcOrInvalid(+m[3], month, +m[2], meridiemHour(+m[4], m[7]), +m[5], +m[6]), zoneOffset(m[8]))
  }
  return null
}

const PARSERS: Record<DateEncoding, (raw: string, spec: DateColumnSpec, ctx: InterpretContext) => Parsed> = {
  'epoch-seconds': (raw) => parseEpochSeconds(raw),
  'epoch-micros': (raw) => parseEpochMicros(raw),
  'iso-8601': (raw, spec) => parseIso(raw, spec),
  tiktok: (raw) => parseTiktok(raw),
  'takeout-html': (raw, _spec, ctx) => parseTakeoutHtml(raw, ctx),
  'meta-html': (raw, spec) => parseMetaHtml(raw, spec)
}

// `spec.encoding` is already a non-empty, frozen array by the time it reaches here: the zod
// `.transform` on `zDateColumnSpec` (visualization_plugin/types.ts) normalises a single
// encoding into a one-element array once, at the schema boundary (one parse per table —
// ADR-0035), so there is nothing left to normalise or cache per spec object here
// (final review; ts-idiom T9/T17).

/** Whether a cell counts as empty (blank, or one of the "none"/"null"/"nan" spellings),
 *  independent of any declared encoding. Tests the trimmed cell, not the raw one, so a value
 *  that is empty only after trimming (e.g. leading/trailing whitespace around "none") is still
 *  caught (bug fix: EMPTY used to be tested pre-trim, ts-idiom-merged §5 item 3). Exported so a
 *  column with no declared spec at all can still report `empty` for a blank cell, instead of
 *  every cell in it being `unsupported` (ts-idiom-merged T49 — the caller need not fabricate a
 *  `DateColumnSpec`-shaped sentinel to get this distinction). */
export function isEmptyCell (raw: string): boolean {
  return EMPTY.test(raw.trim())
}

/** Interpret one raw cell under its column's declared encoding(s).
 *
 *  `raw` is always a string here: a table's cells are validated once by `zTableRow`
 *  (types.ts) before any row reaches `interpretTimestamp` (table.tsx's `cellDisplay` and
 *  prepareChartData.ts's `prepareX` both read from an already-parsed `Table`), so there is no
 *  null/undefined case to guard against (ts-idiom T10). */
export function interpretTimestamp (raw: string, spec: DateColumnSpec, ctx: InterpretContext = {}): Interpretation {
  const text = raw.trim()
  if (EMPTY.test(text)) return { status: 'empty' }
  let result: Interpretation | null = null
  for (const encoding of spec.encoding) {
    const parsed = PARSERS[encoding](text, spec, ctx)
    if (parsed === null) continue
    if (result !== null) return { status: 'ambiguous' }
    result = parsed
  }
  return result ?? { status: 'unsupported' }
}
