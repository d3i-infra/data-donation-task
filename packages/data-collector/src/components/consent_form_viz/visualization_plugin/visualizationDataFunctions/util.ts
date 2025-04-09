import { DateFormat, Table } from "../types";

export function formatDate(
  dateString: string[],
  format: DateFormat,
  minValues: number = 10
): [string[], Record<string, number> | null] {
  let formattedDate: string[] = dateString;
  const dateNumbers = dateString.map((date) => new Date(date).getTime());
  let domain: [number, number] | null = null;
  let formatter: (date: Date) => string = (date) => date.toISOString();

  if (format === "auto") format = autoFormatDate(dateNumbers, minValues);

  if (format === "year") formatter = (date) => date.getFullYear().toString();

  if (format === "quarter") {
    formatter = (date) => {
      const year = date.getFullYear().toString();
      const quarter = Math.floor(date.getMonth() / 3) + 1;
      return `${year}-Q${quarter}`;
    };
  }

  if (format === "month") {
    formatter = (date) => {
      const year = date.getFullYear().toString();
      const month = date.toLocaleString("default", { month: "short" });
      return `${year}-${month}`;
    };
  }

  if (format === "day") {
    formatter = (date) => {
      const year = date.getFullYear().toString();
      const month = date.toLocaleString("default", { month: "short" });
      const day = date.getDate().toString();
      return `${year}-${month}-${day}`;
    };
  }

  if (format === "hour") {
    formatter = (date) => {
      const year = date.getFullYear().toString();
      const month = date.toLocaleString("default", { month: "short" });
      const day = date.getDate().toString();
      const hour = date.getHours();
      return `${year}-${month}-${day} ${hour}:00`;
    };
  }

  if (format === "month_cycle") {
    formatter = (date) => {
      const intlFormatter = new Intl.DateTimeFormat("default", { month: "long" });
      return intlFormatter.format(date);
    };
    // can be any year, starting at january
    domain = [new Date("2000-01-01").getTime(), new Date("2001-01-01").getTime()];
  }
  if (format === "weekday_cycle") {
    formatter = (date) => {
      const intlFormatter = new Intl.DateTimeFormat("default", { weekday: "long" });
      return intlFormatter.format(date);
    };
    // can be any full week, starting at monday
    domain = [new Date("2023-11-06").getTime(), new Date("2023-11-13").getTime()];
  }
  if (format === "hour_cycle") {
    formatter = (date) => {
      const intlFormatter = new Intl.DateTimeFormat("default", { hour: "numeric", hour12: false });
      return intlFormatter.format(date);
    };
    // can be any day, starting at midnight
    domain = [new Date("2000-01-01").getTime(), new Date("2000-01-02").getTime()];
  }

  formattedDate = dateNumbers.map((date) => formatter(new Date(date)));
  if (domain == null) domain = getDomain(dateNumbers);
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

export function extractUrlDomain(x: string): string {
  let domain;
  try {
    const url = new URL(x);
    domain = url.hostname.replace(/^www\./, "").replace(/^m\./, "");
  } catch (_) {
    domain = x;
  }
  return domain.trim();
}
