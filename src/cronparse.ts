/**
 * EB-124: zero-dependency cron expression parser and next-occurrence
 * calculator for cron-driven recurring publishes
 * (`EventBus.scheduleRecurring`).
 *
 * Five fields — `minute hour day-of-month month day-of-week` — evaluated in
 * UTC (the WR-80 convention: schedule math never depends on the process
 * timezone). Supported per field:
 *
 * - `*`, `*\/n`, `n`, `n-m`, `n-m/s`, comma lists (`1,15,30`), and `n/s`
 *   (shorthand for `n-max/s`);
 * - month and day-of-week names (`JAN`–`DEC`, `SUN`–`SAT`), case-insensitive;
 *   day-of-week accepts `0`–`7` with `7` meaning Sunday;
 * - `L` extensions: `L` (day-of-month: last day of the month; day-of-week:
 *   last Saturday of the month, the quartz convention), `L-n` (n days
 *   before the last day of the month), `LW` (last weekday of the month),
 *   `nW` (weekday nearest to day n), `nL`/`NAMEL` (last nth weekday of the
 *   month), `n#k`/`NAME#k` (kth nth weekday of the month, k = 1–5).
 *
 * Day matching follows classic cron: when both day-of-month and
 * day-of-week are restricted, a day matching EITHER fires; when exactly
 * one is restricted it decides; when neither is restricted every day
 * matches. `L`/`W`/`#` forms count as restricted.
 *
 * Parsing is fail-fast: `parseCronExpression` throws `RangeError` for any
 * malformed expression — wrong field count, out-of-range values, reversed
 * ranges, non-positive steps, bad `L`/`W`/`#` forms, and expressions that
 * can never fire (e.g. February 30: no occurrence within 9 years). A
 * parsed schedule therefore never throws mid-schedule; the bus parses
 * once at `scheduleRecurring` time and reuses the schedule for every
 * subsequent trigger computation.
 *
 * `nextCronOccurrence` is a pure function of `(schedule, afterMs)`: it
 * returns the first trigger strictly greater than `afterMs`, so a job
 * that fires at T recomputes its next trigger from T without ever
 * re-firing the same instant (no busy loop when the clock does not
 * advance). The bus passes its injected clock (`EventBusOptions.now`),
 * keeping tests deterministic.
 */

const MONTH_NAMES: Record<string, number> = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
};

const DOW_NAMES: Record<string, number> = {
  sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6,
};

/** Day-of-month matching state for one parsed cron expression. */
export interface CronDomSpec {
  /** Exact days of month (1–31) from plain numeric items. */
  days: Set<number>;
  /** `L`: the last day of the month. */
  lastDay: boolean;
  /** `L-n`: n days before the last day of the month (null when absent). */
  lastDayOffset: number | null;
  /** `LW`: the last weekday (Mon–Fri) of the month. */
  lastWeekday: boolean;
  /** `nW`: days whose nearest weekday (Mon–Fri) is checked. */
  nearestWeekdays: Set<number>;
}

/** Day-of-week matching state for one parsed cron expression. */
export interface CronDowSpec {
  /** Weekdays (0–6, Sunday = 0) from plain numeric/name items. */
  days: Set<number>;
  /** `L` / `nL` / `NAMEL`: weekdays whose LAST monthly occurrence matches. */
  lastDows: Set<number>;
  /** `n#k` / `NAME#k`: weekday → k, matching the kth occurrence (k = 1–5). */
  nth: Map<number, number>;
}

/** A parsed, validated cron schedule. Never throws once constructed. */
export interface CronSchedule {
  /** Allowed minutes (0–59). */
  minutes: Set<number>;
  /** Allowed hours (0–23). */
  hours: Set<number>;
  /** Allowed months (1–12). */
  months: Set<number>;
  /** `minutes` ascending — the trigger-time search order. */
  minutesSorted: number[];
  /** `hours` ascending — the trigger-time search order. */
  hoursSorted: number[];
  /** Day-of-month match state. */
  dom: CronDomSpec;
  /** Day-of-week match state. */
  dow: CronDowSpec;
  /** True unless the day-of-month field was exactly `*`. */
  domRestricted: boolean;
  /** True unless the day-of-week field was exactly `*`. */
  dowRestricted: boolean;
  /** The original expression (trimmed), for stats and the durable log. */
  source: string;
}

/**
 * Resolves one numeric-or-name token to an integer in `[min, max]`.
 * Throws `RangeError` for anything else.
 */
function resolveValue(
  token: string,
  field: string,
  min: number,
  max: number,
  aliases?: Record<string, number>,
): number {
  const trimmed = token.trim();
  if (trimmed.length === 0) throw new RangeError(`${field}: empty value`);
  const aliased = aliases?.[trimmed.toLowerCase()];
  if (aliased !== undefined) return aliased;
  if (!/^-?\d+$/.test(trimmed)) {
    throw new RangeError(`${field}: invalid value "${token}"`);
  }
  const n = parseInt(trimmed, 10);
  if (!Number.isSafeInteger(n) || n < min || n > max) {
    throw new RangeError(`${field}: value ${trimmed} out of range ${min}-${max}`);
  }
  return n;
}

/**
 * Parses one comma-separated numeric field: `*`, `*\/n`, `n`, `n-m`,
 * `n-m/s`, `n/s` (= `n-max/s`). Returns the matched value set.
 */
function parseNumericList(
  part: string,
  field: string,
  min: number,
  max: number,
  aliases?: Record<string, number>,
): Set<number> {
  const out = new Set<number>();
  for (const raw of part.split(',')) {
    if (raw.length === 0) throw new RangeError(`${field}: empty list item`);
    const slash = raw.indexOf('/');
    const rangePart = slash < 0 ? raw : raw.slice(0, slash);
    const stepPart = slash < 0 ? null : raw.slice(slash + 1);
    let start: number;
    let end: number;
    if (rangePart === '*') {
      start = min;
      end = max;
    } else {
      const dash = rangePart.indexOf('-');
      if (dash < 0) {
        start = end = resolveValue(rangePart, field, min, max, aliases);
      } else {
        start = resolveValue(rangePart.slice(0, dash), field, min, max, aliases);
        end = resolveValue(rangePart.slice(dash + 1), field, min, max, aliases);
        if (start > end) throw new RangeError(`${field}: reversed range "${raw}"`);
      }
    }
    let step = 1;
    if (stepPart !== null) {
      if (!/^\d+$/.test(stepPart)) {
        throw new RangeError(`${field}: invalid step "${stepPart}" in "${raw}"`);
      }
      step = parseInt(stepPart, 10);
      if (step < 1) throw new RangeError(`${field}: step must be a positive integer in "${raw}"`);
    }
    for (let v = start; v <= end; v += step) out.add(v);
  }
  return out;
}

/** Parses the day-of-month field, including `L` / `L-n` / `LW` / `nW`. */
function parseDomField(part: string): CronDomSpec {
  const spec: CronDomSpec = {
    days: new Set(),
    lastDay: false,
    lastDayOffset: null,
    lastWeekday: false,
    nearestWeekdays: new Set(),
  };
  for (const raw of part.split(',')) {
    if (raw.length === 0) throw new RangeError('day-of-month: empty list item');
    const up = raw.toUpperCase();
    if (up === 'L') {
      spec.lastDay = true;
      continue;
    }
    if (up === 'LW') {
      spec.lastWeekday = true;
      continue;
    }
    const lastMinus = /^L-(\d+)$/.exec(up);
    if (lastMinus !== null) {
      const n = parseInt(lastMinus[1], 10);
      if (n < 1 || n > 30) {
        throw new RangeError(`day-of-month: L-${lastMinus[1]} out of range 1-30`);
      }
      // At most one L-offset is meaningful; the last one wins.
      spec.lastDayOffset = n;
      continue;
    }
    const nearest = /^(\d+)W$/.exec(up);
    if (nearest !== null) {
      const n = parseInt(nearest[1], 10);
      if (n < 1 || n > 31) {
        throw new RangeError(`day-of-month: ${nearest[1]}W out of range 1-31`);
      }
      spec.nearestWeekdays.add(n);
      continue;
    }
    for (const v of parseNumericList(raw, 'day-of-month', 1, 31)) spec.days.add(v);
  }
  return spec;
}

/** Resolves one day-of-week value token (name or 0–7, with 7 = Sunday). */
function resolveDowValue(token: string): number {
  const n = resolveValue(token, 'day-of-week', 0, 7, DOW_NAMES);
  return n === 7 ? 0 : n;
}

/** Parses the day-of-week field, including `L` / `nL` / `n#k` forms. */
function parseDowField(part: string): CronDowSpec {
  const spec: CronDowSpec = { days: new Set(), lastDows: new Set(), nth: new Map() };
  for (const raw of part.split(',')) {
    if (raw.length === 0) throw new RangeError('day-of-week: empty list item');
    const up = raw.toUpperCase();
    // Bare `L` = the last Saturday of the month (the quartz convention).
    if (up === 'L') {
      spec.lastDows.add(6);
      continue;
    }
    const lastDow = /^([A-Z]{3}|\d+)L$/.exec(up);
    if (lastDow !== null) {
      spec.lastDows.add(resolveDowValue(lastDow[1]));
      continue;
    }
    const nthAny = /^([A-Z]{3}|\d+)#(\d+)$/.exec(up);
    if (nthAny !== null) {
      const k = parseInt(nthAny[2], 10);
      if (k < 1 || k > 5) {
        throw new RangeError(`day-of-week: # occurrence must be 1-5 in "${raw}"`);
      }
      spec.nth.set(resolveDowValue(nthAny[1]), k);
      continue;
    }
    for (const v of parseNumericList(raw, 'day-of-week', 0, 7, DOW_NAMES)) {
      spec.days.add(v === 7 ? 0 : v);
    }
  }
  return spec;
}

function daysInMonth(year: number, month0: number): number {
  return new Date(Date.UTC(year, month0 + 1, 0)).getUTCDate();
}

/** The last Monday–Friday date of the month. */
function lastWeekdayOfMonth(year: number, month0: number, dim: number): number {
  let d = dim;
  for (;;) {
    const dow = new Date(Date.UTC(year, month0, d)).getUTCDay();
    if (dow >= 1 && dow <= 5) return d;
    d--;
  }
}

/**
 * The weekday (Mon–Fri) nearest to day `d`: Saturday shifts to the
 * Friday before (to the Monday after when `d` is the 1st, so the result
 * never leaves the month), Sunday shifts to the Monday after. Returns -1
 * when no valid day exists (`d` past the end of the month, or the shift
 * would leave the month) — `W` never crosses a month boundary.
 */
function nearestWeekday(year: number, month0: number, d: number, dim: number): number {
  if (d < 1 || d > dim) return -1;
  const dow = new Date(Date.UTC(year, month0, d)).getUTCDay();
  if (dow === 6) return d === 1 ? 3 : d - 1;
  if (dow === 0) {
    const shifted = d + 1;
    return shifted > dim ? -1 : shifted;
  }
  return d;
}

/** The last date of the month falling on weekday `targetDow` (0–6). */
function lastDowOfMonth(year: number, month0: number, targetDow: number, dim: number): number {
  const lastDow = new Date(Date.UTC(year, month0, dim)).getUTCDay();
  return dim - ((lastDow - targetDow + 7) % 7);
}

/** The date of the kth `targetDow` in the month, or -1 when there is none. */
function nthDowOfMonth(
  year: number,
  month0: number,
  targetDow: number,
  k: number,
  dim: number,
): number {
  const firstDow = new Date(Date.UTC(year, month0, 1)).getUTCDay();
  const nth = 1 + ((targetDow - firstDow + 7) % 7) + 7 * (k - 1);
  return nth > dim ? -1 : nth;
}

function matchesDom(spec: CronDomSpec, year: number, month0: number, dom: number, dim: number): boolean {
  if (spec.days.has(dom)) return true;
  if (spec.lastDay && dom === dim) return true;
  if (spec.lastDayOffset !== null && dom === dim - spec.lastDayOffset) return true;
  if (spec.lastWeekday && dom === lastWeekdayOfMonth(year, month0, dim)) return true;
  for (const d of spec.nearestWeekdays) {
    if (dom === nearestWeekday(year, month0, d, dim)) return true;
  }
  return false;
}

function matchesDow(
  spec: CronDowSpec,
  year: number,
  month0: number,
  dom: number,
  dim: number,
  dow: number,
): boolean {
  if (spec.days.has(dow)) return true;
  for (const d of spec.lastDows) {
    if (dom === lastDowOfMonth(year, month0, d, dim)) return true;
  }
  for (const [d, k] of spec.nth) {
    if (dom === nthDowOfMonth(year, month0, d, k, dim)) return true;
  }
  return false;
}

/**
 * Classic cron day rule: both day fields restricted → either may match;
 * exactly one restricted → it decides; neither restricted → every day.
 */
function dayMatches(
  schedule: CronSchedule,
  year: number,
  month0: number,
  dom: number,
  dow: number,
): boolean {
  if (!schedule.months.has(month0 + 1)) return false;
  const dim = daysInMonth(year, month0);
  const domMatch = matchesDom(schedule.dom, year, month0, dom, dim);
  const dowMatch = matchesDow(schedule.dow, year, month0, dom, dim, dow);
  const { domRestricted, dowRestricted } = schedule;
  if (!domRestricted && !dowRestricted) return true;
  if (domRestricted && !dowRestricted) return domMatch;
  if (!domRestricted && dowRestricted) return dowMatch;
  return domMatch || dowMatch;
}

/**
 * Forward search horizon in days. The longest real gap between two
 * occurrences of a matchable expression is 8 years (February 29 across a
 * non-leap century, e.g. 2096 → 2104); 9 years of day-granular search
 * always finds the next occurrence of a validated schedule.
 */
const MAX_SEARCH_DAYS = 366 * 9;

/** Deterministic reference for the never-matches validation in `parseCronExpression`. */
const MATCHABILITY_REFERENCE_MS = Date.UTC(2024, 0, 1, 0, 0, 0, 0);

/**
 * Parses and validates a 5-field cron expression. Throws `RangeError`
 * for anything malformed — including expressions that can never fire
 * (e.g. February 30) — so callers can treat a returned schedule as
 * total: `nextCronOccurrence` never fails on it.
 */
export function parseCronExpression(expr: string): CronSchedule {
  if (typeof expr !== 'string') {
    throw new RangeError('cron expression must be a string');
  }
  const source = expr.trim();
  const parts = source.split(/\s+/);
  if (parts.length !== 5) {
    throw new RangeError(
      `cron expression must have exactly 5 fields ` +
        `(minute hour day-of-month month day-of-week), got ${parts.length}: "${expr}"`,
    );
  }
  const [minutePart, hourPart, domPart, monthPart, dowPart] = parts;
  const minutes = parseNumericList(minutePart, 'minute', 0, 59);
  const hours = parseNumericList(hourPart, 'hour', 0, 23);
  const dom = parseDomField(domPart);
  const months = parseNumericList(monthPart, 'month', 1, 12, MONTH_NAMES);
  const dow = parseDowField(dowPart);
  const schedule: CronSchedule = {
    minutes,
    hours,
    months,
    minutesSorted: [...minutes].sort((a, b) => a - b),
    hoursSorted: [...hours].sort((a, b) => a - b),
    dom,
    dow,
    domRestricted: domPart !== '*',
    dowRestricted: dowPart !== '*',
    source,
  };
  try {
    nextCronOccurrence(schedule, MATCHABILITY_REFERENCE_MS);
  } catch {
    throw new RangeError(
      `cron expression "${source}" can never match ` +
        `(e.g. February 30): no occurrence within 9 years of 2024-01-01`,
    );
  }
  return schedule;
}

/**
 * The first trigger strictly greater than `afterMs`, evaluated in UTC.
 * Pure in `(schedule, afterMs)` — pass the bus clock for live use, an
 * injected clock for deterministic tests. Throws `RangeError` for a
 * non-finite `afterMs`, and (unreachable for schedules returned by
 * `parseCronExpression`) when no occurrence falls inside the search
 * horizon.
 */
export function nextCronOccurrence(schedule: CronSchedule, afterMs: number): number {
  if (typeof afterMs !== 'number' || !Number.isFinite(afterMs)) {
    throw new RangeError('nextCronOccurrence: afterMs must be a finite number of milliseconds');
  }
  let dayStart = Math.floor(afterMs / 86400000) * 86400000;
  for (let d = 0; d < MAX_SEARCH_DAYS; d++) {
    const date = new Date(dayStart);
    if (
      dayMatches(schedule, date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(), date.getUTCDay())
    ) {
      for (const h of schedule.hoursSorted) {
        for (const min of schedule.minutesSorted) {
          const t = dayStart + h * 3600000 + min * 60000;
          if (t > afterMs) return t;
        }
      }
    }
    dayStart += 86400000;
  }
  throw new RangeError(
    `cron expression "${schedule.source}" has no occurrence within ${MAX_SEARCH_DAYS} days`,
  );
}
