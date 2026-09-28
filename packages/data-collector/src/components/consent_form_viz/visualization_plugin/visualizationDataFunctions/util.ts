import { DateFormat, Table } from "../types";
import { Interpretation } from "./interpretTimestamp";

export function formatDate(
  wallClockMs: Array<number | null>,
  format: DateFormat,
  minValues: number = 10
): [string[], Record<string, number> | null] {
  let formattedDate: string[];
  const nonNullMs = wallClockMs.filter((ms): ms is number => ms !== null);
  let domain: [number, number] | null = null;
  let formatter: (date: Date) => string = (date) => date.toISOString();

  if (format === "auto") format = autoFormatDate(nonNullMs, minValues);

  if (format === "year") formatter = (date) => date.getUTCFullYear().toString();

  if (format === "quarter") {
    formatter = (date) => {
      const year = date.getUTCFullYear().toString();
      const quarter = Math.floor(date.getUTCMonth() / 3) + 1;
      return `${year}-Q${quarter}`;
    };
  }

  if (format === "month") {
    const monthFormatter = new Intl.DateTimeFormat("default", { month: "short", timeZone: "UTC" });
    formatter = (date) => {
      const year = date.getUTCFullYear().toString();
      const month = monthFormatter.format(date);
      return `${year}-${month}`;
    };
  }

  if (format === "day") {
    const monthFormatter = new Intl.DateTimeFormat("default", { month: "short", timeZone: "UTC" });
    formatter = (date) => {
      const year = date.getUTCFullYear().toString();
      const month = monthFormatter.format(date);
      const day = date.getUTCDate().toString();
      return `${year}-${month}-${day}`;
    };
  }

  if (format === "hour") {
    const monthFormatter = new Intl.DateTimeFormat("default", { month: "short", timeZone: "UTC" });
    formatter = (date) => {
      const year = date.getUTCFullYear().toString();
      const month = monthFormatter.format(date);
      const day = date.getUTCDate().toString();
      const hour = date.getUTCHours();
      return `${year}-${month}-${day} ${hour}:00`;
    };
  }

  if (format === "month_cycle") {
    const intlFormatter = new Intl.DateTimeFormat("default", { month: "long", timeZone: "UTC" });
    formatter = (date) => intlFormatter.format(date);
    // can be any year, starting at january
    domain = [Date.UTC(2000, 0, 1), Date.UTC(2001, 0, 1)];
  }
  if (format === "weekday_cycle") {
    const intlFormatter = new Intl.DateTimeFormat("default", { weekday: "long", timeZone: "UTC" });
    formatter = (date) => intlFormatter.format(date);
    // can be any full week, starting at monday
    domain = [Date.UTC(2023, 10, 6), Date.UTC(2023, 10, 13)];
  }
  if (format === "hour_cycle") {
    const intlFormatter = new Intl.DateTimeFormat("default", { hour: "numeric", hour12: false, timeZone: "UTC" });
    formatter = (date) => intlFormatter.format(date);
    // can be any day, starting at midnight
    domain = [Date.UTC(2000, 0, 1), Date.UTC(2000, 0, 2)];
  }

  formattedDate = wallClockMs.map((ms) => (ms === null ? "" : formatter(new Date(ms))));
  if (domain == null) domain = getDomain(nonNullMs);
  const sortableDate: Record<string, number> | null = createSortable(domain, format, formatter);

  return [formattedDate, sortableDate];
}

function autoFormatDate(dateNumbers: number[], minValues: number): DateFormat {
  const [minTime, maxTime] = getDomain(dateNumbers);

  let autoFormat: DateFormat = "hour";
  if (maxTime - minTime > 1000 * 60 * 60 * 24 * minValues) autoFormat = "day";
  if (maxTime - minTime > 1000 * 60 * 60 * 24 * 30 * minValues) autoFormat = "month";
  if (maxTime - minTime > 1000 * 60 * 60 * 24 * 30 * 3 * minValues) autoFormat = "quarter";
  if (maxTime - minTime > 1000 * 60 * 60 * 24 * 365 * minValues) autoFormat = "year";

  return autoFormat;
}

function createSortable(
  domain: [number, number],
  interval: string,
  formatter: (date: Date) => string
): Record<string, number> | null {
  // creates a map of datestrings to sortby numbers. Also includes intervalls, so that
  // addZeroes can be used.
  const sortable: Record<string, number> = {};
  const [minTime, maxTime] = domain;

  // intervalnumbers don't need to be exact. Just small enough that they never
  // skip over an interval (e.g., month should be shortest possible month).
  // Duplicate dates are ignored in set
  let intervalNumber: number = 0;
  if (interval === "year") intervalNumber = 1000 * 60 * 60 * 24 * 364;
  if (interval === "quarter") intervalNumber = 1000 * 60 * 60 * 24 * 28 * 3;
  if (["month", "month_cycle"].includes(interval)) intervalNumber = 1000 * 60 * 60 * 24 * 28;
  if (["day", "weekday_cycle"].includes(interval)) intervalNumber = 1000 * 60 * 60 * 24;
  if (["hour", "hour_cycle"].includes(interval)) intervalNumber = 1000 * 60 * 60;

  if (intervalNumber > 0) {
    for (let i = minTime; i <= maxTime; i += intervalNumber) {
      const date = new Date(i);
      const datestring = formatter(date);
      if (sortable[datestring] !== undefined) continue;
      sortable[datestring] = i;
    }
  }

  return sortable;
}

function getDomain(numbers: number[]): [number, number] {
  let min = numbers[0];
  let max = numbers[0];
  numbers.forEach((nr) => {
    if (nr < min) min = nr;
    if (nr > max) max = nr;
  });
  return [min, max];
}

export function tokenize(text: string): string[] {
  const tokens = text.split(" ");
  return tokens.filter((token) => /\p{L}/giu.test(token)); // only tokens with word characters
}

export function getTableColumn(table: Table, column: string): string[] {
  if (column === ".COUNT") {
    // special case: just return array with values of 1
    return Array(table.body.rows.length).fill("1");
  }
  const columnIndex = table.head.cells.findIndex((cell) => cell === column);
  if (columnIndex < 0) throw new Error(`column ${table.id}.${column} not found`);
  return table.body.rows.map((row) => row.cells[columnIndex]);
}

export function rescaleToRange(value: number, min: number, max: number, newMin: number, newMax: number): number {
  let scaled = (value - min) / (max - min);
  scaled = isNaN(scaled) ? 0 : scaled; // prevent NaN
  return scaled * (newMax - newMin) + newMin;
}

const displayFormatters = new Map<string, Intl.DateTimeFormat>()

export function displayTimestamp (interp: Interpretation, timeZone: string, locale: string): string | null {
  if (interp.status === 'empty') return ''
  if (interp.status !== 'instant' && interp.status !== 'local') return null
  const zone = interp.status === 'instant' ? timeZone : 'UTC'
  const key = `${locale}|${zone}`
  let fmt = displayFormatters.get(key)
  if (fmt === undefined) {
    fmt = new Intl.DateTimeFormat(locale, { dateStyle: 'medium', timeStyle: 'medium', timeZone: zone })
    displayFormatters.set(key, fmt)
  }
  return fmt.format(new Date(interp.status === 'instant' ? interp.epochMs : interp.wallClockMs))
}

// `new Intl.DateTimeFormat` throws a RangeError for a `timeZone` the browser's ICU does not
// recognise. Callers validate a declared zone once (e.g. per table, in a memo — ADR-0035),
// not per row, and fall back to a default zone on failure.
export function isValidTimeZone(zone: string): boolean {
  try {
    void new Intl.DateTimeFormat('en-US', { timeZone: zone })
    return true
  } catch {
    return false
  }
}

// The one default display zone (bug: previously duplicated as a literal in table.tsx and
// prepareChartData.ts — ts-idiom-merged §5 items 1-2 / T41).
export const DEFAULT_DISPLAY_TIMEZONE = 'Europe/Amsterdam'

/** A table's declared `displayTimezone`, validated once and falling back to
 *  `DEFAULT_DISPLAY_TIMEZONE` for an undeclared or invalid zone. Both the table and chart
 *  paths go through this so an invalid zone can no longer reach `Intl.DateTimeFormat`
 *  unvalidated in either place (bug: the chart path used to skip validation entirely and
 *  throw — ts-idiom-merged §5 item 1). */
export function resolveDisplayTimezone(zone: string | undefined): string {
  if (zone !== undefined && isValidTimeZone(zone)) return zone
  return DEFAULT_DISPLAY_TIMEZONE
}

export function extractUrlDomain(x: string): string {
  let domain;
  try {
    const url = new URL(x);
    domain = url.hostname.replace(/^www\./, "").replace(/^m\./, "");
  } catch {
    domain = x;
  }
  return domain.trim();
}
