import { interpretTimestamp, wallClockInZone, monthTable, Interpretation } from './interpretTimestamp'
import { DateColumnSpec, DateEncoding, zDateColumnSpec } from '../types'

// `DateColumnSpec.encoding` is zod *output*: a non-empty tuple the `.transform` builds once, at
// the schema boundary (visualization_plugin/types.ts). This helper calls the real schema
// (`zDateColumnSpec.parse`) rather than reimplementing its normalisation by hand, so a fixture
// is guaranteed to be shaped exactly the way a real `parseTable` call would produce it
// (ts-fix2-review check 1: "output-side fixtures should call zDateColumnSpec.parse").
const S = (encoding: DateEncoding | DateEncoding[], extra: Partial<DateColumnSpec> = {}): DateColumnSpec => ({
  ...zDateColumnSpec.parse({ encoding }),
  ...extra
})

// Proper narrowing on the tagged union's `status` discriminant instead of `(r as any)`
// (ts-idiom T13): an assertion function lets TypeScript narrow `r` after the check, with no
// cast anywhere in these helpers.
function assertStatus<Status extends Interpretation['status']> (
  r: Interpretation,
  status: Status
): asserts r is Extract<Interpretation, { status: Status }> {
  if (r.status !== status) throw new Error(`expected status '${status}', got '${r.status}'`)
}

const instant = (r: Interpretation): number => { assertStatus(r, 'instant'); return r.epochMs }
const local = (r: Interpretation): number => { assertStatus(r, 'local'); return r.wallClockMs }

describe('statuses', () => {
  it('empty and null-ish cells are empty, never invalid', () => {
    for (const raw of ['', '   ', 'None', 'null', 'NaN']) {
      expect(interpretTimestamp(raw, S('epoch-seconds')).status).toBe('empty')
    }
  })
  it('a value matching no declared encoding is unsupported', () => {
    expect(interpretTimestamp('yesterday', S('epoch-seconds')).status).toBe('unsupported')
  })
  it('a shape that matches but names an impossible date is invalid', () => {
    expect(interpretTimestamp('2026-02-30T10:00:00Z', S('iso-8601')).status).toBe('invalid')
    expect(interpretTimestamp('31 feb 2026, 10:00:00 CET', S('takeout-html'), { locale: 'nl' }).status).toBe('invalid')
  })
  it('a 12-hour marker beside an hour outside 1-12 is invalid, never wrapped', () => {
    expect(interpretTimestamp('Mar 02, 2026 25:00:00 pm', S('meta-html')).status).toBe('invalid')
    expect(interpretTimestamp('Mar 02, 2026 0:30:00 am', S('meta-html')).status).toBe('invalid')
    expect(interpretTimestamp('Jun 15, 2026, 13:30:41 PM UTC', S('takeout-html'), { locale: 'en' }).status).toBe('invalid')
    expect(interpretTimestamp('Mar 02, 2026 12:00:00 am', S('meta-html')).status).toBe('local')
  })
  it('a written offset beyond 59 minutes or 14 hours is invalid', () => {
    expect(interpretTimestamp('2026-06-15T18:30:41+00:99', S('iso-8601')).status).toBe('invalid')
    expect(interpretTimestamp('2026-06-15T18:30:41+15:00', S('iso-8601')).status).toBe('invalid')
    expect(interpretTimestamp('15 jun 2026, 18:30:41 GMT+2:75', S('takeout-html'), { locale: 'nl' }).status).toBe('invalid')
    expect(interpretTimestamp('2026-06-15T18:30:41+14:00', S('iso-8601')).status).toBe('instant')
    expect(interpretTimestamp('2026-06-15T18:30:41-09:30', S('iso-8601')).status).toBe('instant')
  })
})

describe('epoch encodings', () => {
  it('epoch seconds, integer or fractional, as text or number-like', () => {
    expect(instant(interpretTimestamp('1781548241', S('epoch-seconds')))).toBe(1781548241000)
    expect(instant(interpretTimestamp('1781548241.75', S('epoch-seconds')))).toBe(1781548241750)
  })
  it('epoch microseconds keep millisecond precision', () => {
    expect(instant(interpretTimestamp('1781548241123456', S('epoch-micros')))).toBe(1781548241123)
  })
  it('a number that is not an epoch under the declared unit is invalid', () => {
    expect(interpretTimestamp('99999999999999999999', S('epoch-seconds')).status).toBe('invalid')
  })
})

describe('iso-8601', () => {
  it('Z and offsets name an instant; fractions are kept to the millisecond', () => {
    expect(instant(interpretTimestamp('2026-06-15T18:30:41Z', S('iso-8601')))).toBe(Date.UTC(2026, 5, 15, 18, 30, 41))
    expect(instant(interpretTimestamp('2026-06-15T18:30:41.123Z', S('iso-8601')))).toBe(Date.UTC(2026, 5, 15, 18, 30, 41, 123))
    expect(instant(interpretTimestamp('2026-06-15T20:30:41+02:00', S('iso-8601')))).toBe(Date.UTC(2026, 5, 15, 18, 30, 41))
  })
  it('a naive ISO string, with T or a space, is a local wall clock', () => {
    expect(local(interpretTimestamp('2026-06-15T20:30:41', S('iso-8601')))).toBe(Date.UTC(2026, 5, 15, 20, 30, 41))
    expect(local(interpretTimestamp('2024-01-15 20:30:41', S('iso-8601')))).toBe(Date.UTC(2024, 0, 15, 20, 30, 41))
  })
  it('a naive ISO string with a declared utcOffsetMinutes of 0 is an instant at that wall clock', () => {
    expect(instant(interpretTimestamp('2026-06-15 20:30:41', S('iso-8601', { utcOffsetMinutes: 0 }))))
      .toBe(Date.UTC(2026, 5, 15, 20, 30, 41))
  })
  it('a naive ISO string with a declared positive utcOffsetMinutes is an instant earlier than the wall clock', () => {
    // Wall clock is UTC+60 minutes, so the instant is one hour before that wall clock.
    expect(instant(interpretTimestamp('2026-06-15 20:30:41', S('iso-8601', { utcOffsetMinutes: 60 }))))
      .toBe(Date.UTC(2026, 5, 15, 19, 30, 41))
  })
  it('an explicit Z or numeric offset in the value wins over a declared utcOffsetMinutes', () => {
    expect(instant(interpretTimestamp('2026-06-15T18:30:41Z', S('iso-8601', { utcOffsetMinutes: 60 }))))
      .toBe(Date.UTC(2026, 5, 15, 18, 30, 41))
    expect(instant(interpretTimestamp('2026-06-15T20:30:41+02:00', S('iso-8601', { utcOffsetMinutes: 60 }))))
      .toBe(Date.UTC(2026, 5, 15, 18, 30, 41))
  })
})

describe('tiktok', () => {
  it('txt names UTC, json leaves it implicit; both are instants', () => {
    const t = Date.UTC(2026, 4, 2, 10, 9, 50)
    expect(instant(interpretTimestamp('2026-05-02 10:09:50 UTC', S('tiktok')))).toBe(t)
    expect(instant(interpretTimestamp('2026-05-02 10:09:50', S('tiktok')))).toBe(t)
  })
})

describe('meta-html', () => {
  it('reads the 12-hour display clock and applies the declared fixed offset', () => {
    // 4:57:45 pm at UTC-8 is 00:57:45Z the next day
    expect(instant(interpretTimestamp('Mar 02, 2026 4:57:45 pm', S('meta-html', { utcOffsetMinutes: -480 })))).toBe(Date.UTC(2026, 2, 3, 0, 57, 45))
  })
  it('without a declared offset it is a local wall clock', () => {
    expect(local(interpretTimestamp('Mar 02, 2026 4:57:45 pm', S('meta-html')))).toBe(Date.UTC(2026, 2, 2, 16, 57, 45))
  })
  it('a Facebook HTML export clock, declared meta-html with no utcOffsetMinutes, is the account\'s own local wall clock (ADR-0043)', () => {
    const r = interpretTimestamp('Nov 08, 2005 4:19:54 am', S('meta-html'))
    expect(r.status).toBe('local')
    expect(local(r)).toBe(Date.UTC(2005, 10, 8, 4, 19, 54))
    expect(local(interpretTimestamp('Sep 02, 2006 5:46:44 pm', S('meta-html')))).toBe(Date.UTC(2006, 8, 2, 17, 46, 44))
  })
})

describe('takeout-html: the Python corpus, one instant per line', () => {
  const cases: Array<[string, string, number]> = [
    // wall clock as written, zone as named → instant
    ['nl', '15 jun 2026, 20:30:41 CEST', Date.UTC(2026, 5, 15, 18, 30, 41)],
    ['nl', '15 mrt 2026, 20:30:41 CET', Date.UTC(2026, 2, 15, 19, 30, 41)],
    ['nl', '1 mei 2026, 07:05:00 CEST', Date.UTC(2026, 4, 1, 5, 5, 0)],
    ['en', 'Aug 17, 2026, 1:14:48 PM CEST', Date.UTC(2026, 7, 17, 11, 14, 48)],
    // A narrow no-break space (U+202F) before AM/PM, as Google actually renders it
    // (final review, Minor 5) — \s already covers U+202F, this just pins it.
    ['en', 'Aug 17, 2026, 1:14:48 PM CEST', Date.UTC(2026, 7, 17, 11, 14, 48)],
    ['en', 'Aug 15, 2026, 11:39:58 AM CEST', Date.UTC(2026, 7, 15, 9, 39, 58)],
    ['en', 'Dec 31, 2026, 12:00:00 AM CET', Date.UTC(2026, 11, 30, 23, 0, 0)],
    ['en', 'Jan 1, 2026, 12:30:00 PM CET', Date.UTC(2026, 0, 1, 11, 30, 0)],
    ['de', '17. Aug. 2026, 22:14:48 MESZ', Date.UTC(2026, 7, 17, 20, 14, 48)],
    ['de', '27.08.2026, 20:04:54 MESZ', Date.UTC(2026, 7, 27, 18, 4, 54)],
    ['de', '12.07.2026, 23:29:21 MESZ', Date.UTC(2026, 6, 12, 21, 29, 21)],
    ['de', '07.12.2026, 09:00:00 MEZ', Date.UTC(2026, 11, 7, 8, 0, 0)],
    ['tr', '17 Ağu 2026, 22:14:48 GMT+3', Date.UTC(2026, 7, 17, 19, 14, 48)],
    ['zh', '2026年7月30日 00:23:06 CEST', Date.UTC(2026, 6, 29, 22, 23, 6)],
    ['zh', '2025年10月2日 11:40:30 CEST', Date.UTC(2025, 9, 2, 9, 40, 30)],
    ['ar', '23‏/07‏/2026، 4:20:22 م CEST', Date.UTC(2026, 6, 23, 14, 20, 22)],
    ['ar', '30‏/07‏/2026، 12:23:06 ص CEST', Date.UTC(2026, 6, 29, 22, 23, 6)],
    ['ar', '20‏/07‏/2026، 12:16:30 م CEST', Date.UTC(2026, 6, 20, 10, 16, 30)],
    ['ar', '28‏/05‏/2026، 8:28:13 ص CEST', Date.UTC(2026, 4, 28, 6, 28, 13)],
  ]
  it.each(cases)('%s %s', (locale, raw, expected) => {
    expect(instant(interpretTimestamp(raw, S('takeout-html'), { locale }))).toBe(expected)
  })
  it('a zone the table does not know is a local wall clock, never a guess', () => {
    const june = Date.UTC(2026, 5, 15, 20, 30, 41)
    expect(local(interpretTimestamp('15 jun 2026, 20:30:41 CST', S('takeout-html'), { locale: 'nl' }))).toBe(june)
    expect(local(interpretTimestamp('15 jun 2026, 20:30:41 IST', S('takeout-html'), { locale: 'nl' }))).toBe(june)
    expect(local(interpretTimestamp('17.08.2026, 22:14:48', S('takeout-html'), { locale: 'nl' }))).toBe(Date.UTC(2026, 7, 17, 22, 14, 48))
  })
  it('a numeric zone with minutes is applied', () => {
    expect(instant(interpretTimestamp('17 Ağu 2026, 22:14:48 GMT+5:30', S('takeout-html'), { locale: 'tr' }))).toBe(Date.UTC(2026, 7, 17, 16, 44, 48))
  })
  it('month names come from Intl and tolerate a trailing period', () => {
    expect(monthTable('de').get('aug')).toBe(8)
    expect(monthTable('nl').get('mrt')).toBe(3)
    expect(monthTable('tr').get('ağu')).toBe(8)
  })
  it('a Takeout sentence in a locale that is not declared falls to unsupported, not to a guess', () => {
    expect(interpretTimestamp('15 mrt 2026, 20:30:41 CET', S('takeout-html'), { locale: 'en' }).status).toBe('unsupported')
  })
})

describe('several encodings on one column', () => {
  it('picks the one whose shape matches; shapes are exclusive', () => {
    const spec = S(['iso-8601', 'takeout-html'])
    expect(interpretTimestamp('2026-06-15T18:30:41Z', spec, { locale: 'nl' }).status).toBe('instant')
    expect(interpretTimestamp('15 jun 2026, 20:30:41 CEST', spec, { locale: 'nl' }).status).toBe('instant')
  })
  it('a sibling encoding that does not recognise the value never makes it ambiguous', () => {
    // no locale: takeout-html cannot read month names, and must stay out of the way of iso-8601
    expect(interpretTimestamp('2026-06-15T18:30:41Z', S(['iso-8601', 'takeout-html'])).status).toBe('instant')
    expect(interpretTimestamp('15 jun 2026, 20:30:41 CEST', S(['iso-8601', 'takeout-html'])).status).toBe('unsupported')
  })
  it('reports ambiguous if two declared shapes both match one value', () => {
    // epoch-seconds and epoch-micros both match a digit string; declaring both is a config error the value surfaces
    expect(interpretTimestamp('1781548241', S(['epoch-seconds', 'epoch-micros'])).status).toBe('ambiguous')
  })
  it('an invalid parse still counts as a match toward ambiguity (final review M3)', () => {
    // Feb 30 is calendar-impossible, but both iso-8601 and tiktok shapes match the text, so
    // both count even though each individually would report 'invalid' on its own.
    expect(interpretTimestamp('2026-02-30 10:00:00', S(['iso-8601', 'tiktok'])).status).toBe('ambiguous')
  })
})

describe('wallClockInZone', () => {
  it('shifts an instant into the zone\'s wall clock, DST included', () => {
    expect(wallClockInZone(Date.UTC(2026, 5, 15, 18, 30, 41), 'Europe/Amsterdam')).toBe(Date.UTC(2026, 5, 15, 20, 30, 41))
    expect(wallClockInZone(Date.UTC(2026, 0, 15, 12, 0, 0), 'Europe/Amsterdam')).toBe(Date.UTC(2026, 0, 15, 13, 0, 0))
    expect(wallClockInZone(Date.UTC(2026, 5, 15, 18, 30, 41), 'America/Chicago')).toBe(Date.UTC(2026, 5, 15, 13, 30, 41))
  })
})

describe('hoisting (ADR-0035)', () => {
  it('interpreting 10,000 cells constructs at most one Intl formatter per locale and zone', () => {
    // Locale and zone chosen so no earlier test in this file has warmed the caches.
    const ctor = jest.spyOn(Intl, 'DateTimeFormat')
    const spec = S('takeout-html')
    for (let i = 0; i < 10_000; i++) interpretTimestamp('15 jun 2026, 20:30:41 CEST', spec, { locale: 'nl-BE' })
    for (let i = 0; i < 10_000; i++) wallClockInZone(1781548241000 + i * 1000, 'Europe/Brussels')
    // A month table is built once per locale from six formatters (short + long names for the
    // locale itself and for the en-GB / en-US fallbacks), plus one zone formatter: seven, for
    // 20,000 calls. The bound is per locale and zone, never per row.
    expect(ctor.mock.calls.length).toBeLessThanOrEqual(7)
    ctor.mockRestore()
  })
  // The `Array.isArray`-spy version of this test (final review M1) tested an allocation
  // implementation detail that no longer exists: `spec.encoding` is normalised once at the
  // zod schema boundary (visualization_plugin/types.ts's `.transform`), not per spec object
  // inside interpretTimestamp.ts, so there is nothing left here to spy on (ts-idiom T9/T17).
  it('rejects an empty encoding array at the type level, not just at the schema level (ts-fix2-review T9/T36)', () => {
    // DateColumnSpec.encoding is a non-empty tuple (`readonly [DateEncoding, ...DateEncoding[]]`),
    // so `[]` is a compile error here, not merely a value `zDateColumnSpec.min(1)`/`.nonempty()`
    // would reject at runtime. This line only proves anything once test files are type-checked
    // (tsconfig.test.json: isolatedModules false + types: ["jest"]).
    // @ts-expect-error — encoding: [] is not assignable to the non-empty tuple type.
    const rejected: DateColumnSpec = { encoding: [] }
    void rejected
  })
})

describe('EMPTY tests the trimmed cell (ts-idiom-merged §5 item 3)', () => {
  it('a value that is empty only after trimming leading/trailing whitespace is empty, not unsupported', () => {
    expect(interpretTimestamp(' none', S('epoch-seconds')).status).toBe('empty')
    expect(interpretTimestamp('null ', S('epoch-seconds')).status).toBe('empty')
  })
})

describe('takeout-html month names seen in real exports', () => {
  const spec = zDateColumnSpec.parse({ encoding: ['iso-8601', 'takeout-html'] })
  it('accepts the British four-letter "Sept" in an English render', () => {
    const r = interpretTimestamp('9 Sept 2020, 14:09:21 CEST', spec, { locale: 'en' })
    expect(r.status).toBe('instant')
  })
  it('accepts a stray English month name inside a Dutch render', () => {
    expect(interpretTimestamp('11 may 2020, 01:10:46 CEST', spec, { locale: 'nl' }).status).toBe('instant')
    expect(interpretTimestamp('13 oct 2020, 11:24:54 CEST', spec, { locale: 'nl' }).status).toBe('instant')
  })
  it('still reads the Dutch names themselves', () => {
    expect(interpretTimestamp('18 mei 2026, 09:43:05 CEST', spec, { locale: 'nl' }).status).toBe('instant')
  })
})
