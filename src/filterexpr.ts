/**
 * Subscription filter expressions (EB-123).
 *
 * A tiny declarative predicate language over message payloads, evaluated
 * at fan-out — after the `filter` predicate, before composite patterns —
 * so a rejected message is never queued, never burns backpressure
 * budget, never counts as a sequence gap (the per-topic baseline
 * advances over it), never consumes delivery credit, and never creates
 * an ack obligation on reliable subscriptions.
 *
 * The expression is compiled once at subscribe time
 * (`compileFilterExpression`); every syntax error, unknown operator and
 * invalid regex literal throws `RangeError` there — never at delivery
 * time. Evaluation itself never throws: a missing field, a type
 * mismatch, or a non-object payload simply fails the predicate, so a
 * filter expression can never take down a publish call (unlike a
 * throwing `filter` predicate, which propagates by design).
 *
 * Grammar (precedence: `||` < `&&` < prefix `!` < comparison):
 *
 *   expr       := or
 *   or         := and ("||" and)*
 *   and        := unary ("&&" unary)*
 *   unary      := "!" unary | primary
 *   primary    := "(" expr ")"
 *               | "exists" "(" path ")"
 *               | path "exists"
 *               | path op literal
 *   op         := "==" | "!=" | ">" | ">=" | "<" | "<=" | "=~"
 *   literal    := string | number | "true" | "false" | "null"
 *   path       := ident ("." ident | "[" digits "]")*
 *
 * Operator classes:
 * - equality:   `type == "trade"`, `retries != 3`
 * - range:      `price > 100`, `price <= 500` (numbers numerically,
 *               strings lexicographically; mixed/other types never match)
 * - existence:  `exists(user.id)` or `user.id exists`
 * - regex:      `symbol =~ /^BTC/` (regex literals only: `/re/flags`,
 *               validated by the RegExp engine at compile time; the field
 *               must be a string at evaluation time)
 *
 * Zero dependencies — a hand-written tokenizer and recursive-descent
 * parser. No `eval`, no `Function` constructor: the compiled form is a
 * plain closure over an AST.
 */

/** Maximum expression source length, in UTF-16 code units. */
const MAX_EXPR_LENGTH = 65536;

/** One path step: a field name or an array index. */
export type FilterPathSegment = string | number;

export type FilterOperator = '==' | '!=' | '>' | '>=' | '<' | '<=' | '=~';

type AstNode =
  | { kind: 'or'; parts: AstNode[] }
  | { kind: 'and'; parts: AstNode[] }
  | { kind: 'not'; expr: AstNode }
  | { kind: 'exists'; path: FilterPathSegment[] }
  | {
      kind: 'compare';
      path: FilterPathSegment[];
      op: FilterOperator;
      value: unknown;
      regex?: RegExp;
    };

/**
 * A compiled filter expression: the original source plus a pure,
 * never-throwing predicate over message payloads.
 */
export interface CompiledFilterExpression {
  /** The original expression source, verbatim. */
  readonly source: string;
  /**
   * The filter-version stamp assigned by the caller (EB-229), when
   * one was given. Versioning lives in `filter-version.ts`; this
   * field is only the stamp — the predicate itself is version-free,
   * so the same compiled form is reusable across versions.
   */
  readonly version: number | undefined;
  /**
   * Evaluates the expression against a message payload. Returns `false`
   * (never throws) when a field is missing, types mismatch, or the
   * payload is not an object.
   */
  readonly test: (payload: unknown) => boolean;
}

type TokenType =
  | 'lparen'
  | 'rparen'
  | 'lbracket'
  | 'rbracket'
  | 'comma'
  | 'dot'
  | 'and'
  | 'or'
  | 'not'
  | 'eq'
  | 'ne'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'match'
  | 'string'
  | 'number'
  | 'regex'
  | 'ident'
  | 'exists'
  | 'true'
  | 'false'
  | 'null'
  | 'eof';

interface Token {
  type: TokenType;
  /** Decoded value for string/number/regex/ident tokens. */
  value: string;
  /** For regex tokens, the flags after the closing slash. */
  flags: string;
  /** Source offset where the token starts. */
  offset: number;
}

const IDENT_START = /[A-Za-z_$]/;
const IDENT_PART = /[A-Za-z0-9_$]/;
const DIGIT = /[0-9]/;
const NUMBER_RE = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/y;

function fail(source: string, offset: number, message: string): never {
  throw new RangeError(`filterExpr: ${message} (at offset ${offset} in ${JSON.stringify(source)})`);
}

/**
 * Tokenizes the expression source. Every malformed construct — an
 * unterminated string, a bad escape, an unknown operator, a dangling
 * `/` — throws `RangeError`.
 */
function tokenize(source: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  const n = source.length;
  const push = (type: TokenType, value: string, offset: number, flags = ''): void => {
    tokens.push({ type, value, offset, flags });
  };

  while (i < n) {
    const ch = source[i];
    if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' || ch === '\f' || ch === '\v') {
      i += 1;
      continue;
    }
    const start = i;
    switch (ch) {
      case '(':
        push('lparen', ch, start);
        i += 1;
        continue;
      case ')':
        push('rparen', ch, start);
        i += 1;
        continue;
      case '[':
        push('lbracket', ch, start);
        i += 1;
        continue;
      case ']':
        push('rbracket', ch, start);
        i += 1;
        continue;
      case ',':
        push('comma', ch, start);
        i += 1;
        continue;
      case '.':
        push('dot', ch, start);
        i += 1;
        continue;
      case '&':
        if (source[i + 1] === '&') {
          push('and', '&&', start);
          i += 2;
          continue;
        }
        fail(source, start, `unknown operator '&' (did you mean '&&'?)`);
        break;
      case '|':
        if (source[i + 1] === '|') {
          push('or', '||', start);
          i += 2;
          continue;
        }
        fail(source, start, `unknown operator '|' (did you mean '||'?)`);
        break;
      case '!':
        if (source[i + 1] === '=') {
          push('ne', '!=', start);
          i += 2;
          continue;
        }
        push('not', '!', start);
        i += 1;
        continue;
      case '=':
        if (source[i + 1] === '=') {
          push('eq', '==', start);
          i += 2;
          continue;
        }
        if (source[i + 1] === '~') {
          push('match', '=~', start);
          i += 2;
          continue;
        }
        fail(source, start, `unknown operator '=' (did you mean '=='?)`);
        break;
      case '>':
        if (source[i + 1] === '=') {
          push('gte', '>=', start);
          i += 2;
        } else {
          push('gt', '>', start);
          i += 1;
        }
        continue;
      case '<':
        if (source[i + 1] === '=') {
          push('lte', '<=', start);
          i += 2;
        } else {
          push('lt', '<', start);
          i += 1;
        }
        continue;
      case "'":
      case '"': {
        const quote = ch;
        let value = '';
        i += 1;
        let closed = false;
        while (i < n) {
          const c = source[i];
          if (c === quote) {
            closed = true;
            i += 1;
            break;
          }
          if (c === '\\') {
            const e = source[i + 1];
            switch (e) {
              case '\\':
                value += '\\';
                break;
              case "'":
                value += "'";
                break;
              case '"':
                value += '"';
                break;
              case 'n':
                value += '\n';
                break;
              case 't':
                value += '\t';
                break;
              case 'r':
                value += '\r';
                break;
              case 'b':
                value += '\b';
                break;
              case 'f':
                value += '\f';
                break;
              case 'v':
                value += '\v';
                break;
              case '0':
                value += '\0';
                break;
              case 'u': {
                const hex = source.slice(i + 2, i + 6);
                if (!/^[0-9a-fA-F]{4}$/.test(hex)) {
                  fail(source, i, 'bad unicode escape (expected \\uXXXX)');
                }
                value += String.fromCharCode(parseInt(hex, 16));
                i += 4;
                break;
              }
              default:
                fail(source, i, `bad escape '\\${e ?? ''}'`);
            }
            i += 2;
            continue;
          }
          value += c;
          i += 1;
        }
        if (!closed) fail(source, start, 'unterminated string literal');
        push('string', value, start);
        continue;
      }
      case '/': {
        // Regex literal: scan to the first unescaped '/' outside a
        // character class, then consume the flags.
        let pattern = '';
        let inClass = false;
        i += 1;
        let closed = false;
        while (i < n) {
          const c = source[i];
          if (c === '\\') {
            if (i + 1 >= n) break;
            pattern += c + source[i + 1];
            i += 2;
            continue;
          }
          if (c === '[') inClass = true;
          else if (c === ']') inClass = false;
          else if (c === '/' && !inClass) {
            closed = true;
            i += 1;
            break;
          }
          pattern += c;
          i += 1;
        }
        if (!closed) fail(source, start, 'unterminated regex literal');
        let flags = '';
        while (i < n && /[a-z]/.test(source[i])) {
          flags += source[i];
          i += 1;
        }
        push('regex', pattern, start, flags);
        continue;
      }
      default:
        break;
    }
    if (DIGIT.test(ch) || (ch === '-' && DIGIT.test(source[i + 1] ?? ''))) {
      NUMBER_RE.lastIndex = i;
      const m = NUMBER_RE.exec(source);
      if (m === null) fail(source, start, 'bad number literal');
      push('number', m[0], start);
      i += m[0].length;
      continue;
    }
    if (IDENT_START.test(ch)) {
      let j = i + 1;
      while (j < n && IDENT_PART.test(source[j])) j += 1;
      const word = source.slice(i, j);
      if (word === 'exists') push('exists', word, start);
      else if (word === 'true') push('true', word, start);
      else if (word === 'false') push('false', word, start);
      else if (word === 'null') push('null', word, start);
      else push('ident', word, start);
      i = j;
      continue;
    }
    fail(source, start, `unexpected character '${ch}'`);
  }
  push('eof', '', n);
  return tokens;
}

class Parser {
  private pos = 0;
  private readonly tokens: Token[];
  private readonly source: string;

  constructor(tokens: Token[], source: string) {
    this.tokens = tokens;
    this.source = source;
  }

  private peek(): Token {
    return this.tokens[this.pos];
  }

  private next(): Token {
    return this.tokens[this.pos++];
  }

  private expect(type: TokenType, what: string): Token {
    const t = this.next();
    if (t.type !== type) fail(this.source, t.offset, `expected ${what}, found ${describe(t)}`);
    return t;
  }

  parse(): AstNode {
    const node = this.parseOr();
    const t = this.peek();
    if (t.type !== 'eof') fail(this.source, t.offset, `unexpected ${describe(t)}`);
    return node;
  }

  private parseOr(): AstNode {
    const parts = [this.parseAnd()];
    while (this.peek().type === 'or') {
      this.next();
      parts.push(this.parseAnd());
    }
    return parts.length === 1 ? parts[0] : { kind: 'or', parts };
  }

  private parseAnd(): AstNode {
    const parts = [this.parseUnary()];
    while (this.peek().type === 'and') {
      this.next();
      parts.push(this.parseUnary());
    }
    return parts.length === 1 ? parts[0] : { kind: 'and', parts };
  }

  private parseUnary(): AstNode {
    if (this.peek().type === 'not') {
      this.next();
      return { kind: 'not', expr: this.parseUnary() };
    }
    return this.parsePrimary();
  }

  private parsePrimary(): AstNode {
    const t = this.peek();
    if (t.type === 'lparen') {
      this.next();
      const node = this.parseOr();
      this.expect('rparen', "')'");
      return node;
    }
    if (t.type === 'exists') {
      // exists(path)
      this.next();
      this.expect('lparen', "'(' after 'exists'");
      const path = this.parsePath();
      this.expect('rparen', "')'");
      return { kind: 'exists', path };
    }
    const path = this.parsePath();
    const after = this.peek();
    if (after.type === 'exists') {
      // path exists
      this.next();
      return { kind: 'exists', path };
    }
    const op = this.parseOperator();
    return this.parseComparison(path, op, after);
  }

  private parsePath(): FilterPathSegment[] {
    const segments: FilterPathSegment[] = [];
    const first = this.peek();
    if (first.type !== 'ident' && first.type !== 'exists') {
      fail(this.source, first.offset, `expected a field path, found ${describe(first)}`);
    }
    segments.push(this.next().value);
    for (;;) {
      const t = this.peek();
      if (t.type === 'dot') {
        this.next();
        const seg = this.peek();
        if (seg.type !== 'ident' && seg.type !== 'exists') {
          fail(this.source, seg.offset, `expected a field name after '.', found ${describe(seg)}`);
        }
        segments.push(this.next().value);
        continue;
      }
      if (t.type === 'lbracket') {
        this.next();
        const idx = this.expect('number', 'an array index');
        const parsed = Number(idx.value);
        if (!Number.isInteger(parsed) || parsed < 0) {
          fail(this.source, idx.offset, `array index must be a non-negative integer`);
        }
        this.expect('rbracket', "']'");
        segments.push(parsed);
        continue;
      }
      return segments;
    }
  }

  private parseOperator(): FilterOperator {
    const t = this.next();
    switch (t.type) {
      case 'eq':
        return '==';
      case 'ne':
        return '!=';
      case 'gt':
        return '>';
      case 'gte':
        return '>=';
      case 'lt':
        return '<';
      case 'lte':
        return '<=';
      case 'match':
        return '=~';
      default:
        fail(this.source, t.offset, `expected an operator (==, !=, >, >=, <, <=, =~), found ${describe(t)}`);
    }
  }

  private parseComparison(
    path: FilterPathSegment[],
    op: FilterOperator,
    opToken: Token,
  ): AstNode {
    const t = this.next();
    if (op === '=~') {
      if (t.type !== 'regex') {
        fail(
          this.source,
          t.offset,
          `'=~' requires a /.../ regex literal, found ${describe(t)}`,
        );
      }
      let regex: RegExp;
      try {
        regex = new RegExp(t.value, t.flags);
      } catch {
        fail(this.source, t.offset, `invalid regex /${t.value}/${t.flags}`);
      }
      return { kind: 'compare', path, op, value: undefined, regex };
    }
    if (t.type === 'regex') {
      fail(this.source, t.offset, `regex literals are only valid with '=~'`);
    }
    let value: unknown;
    switch (t.type) {
      case 'string':
        value = t.value;
        break;
      case 'number':
        value = Number(t.value);
        break;
      case 'true':
        value = true;
        break;
      case 'false':
        value = false;
        break;
      case 'null':
        value = null;
        break;
      default:
        fail(
          this.source,
          t.offset,
          `expected a string, number, boolean or null literal, found ${describe(t)}`,
        );
    }
    return { kind: 'compare', path, op, value };
  }
}

function describe(t: Token): string {
  if (t.type === 'eof') return 'end of expression';
  const shown = t.type === 'string' ? JSON.stringify(t.value) : t.value;
  return `'${shown}'`;
}

/**
 * Resolves a dotted/array path against a payload. Never throws: a
 * missing step, a non-object intermediate, a non-array index, or a
 * throwing getter all resolve to `undefined` (field absent).
 */
function resolvePath(root: unknown, path: FilterPathSegment[]): unknown {
  let current: unknown = root;
  for (const segment of path) {
    if (current === null || current === undefined) return undefined;
    try {
      if (typeof segment === 'number') {
        current = Array.isArray(current) ? current[segment] : undefined;
      } else if (typeof current === 'object') {
        current = (current as Record<string, unknown>)[segment];
      } else {
        return undefined;
      }
    } catch {
      return undefined;
    }
  }
  return current;
}

function compareValues(op: FilterOperator, left: unknown, right: unknown, regex?: RegExp): boolean {
  switch (op) {
    case '==':
      // A missing field never equals a literal — even `null`. This is
      // the documented non-object/missing-field contract: absence is
      // not a value, so only `exists()` observes it.
      return left !== undefined && left === right;
    case '!=':
      return !(left !== undefined && left === right);
    case '>':
    case '>=':
    case '<':
    case '<=': {
      if (left === undefined) return false;
      if (typeof left === 'number' && typeof right === 'number') {
        if (op === '>') return left > right;
        if (op === '>=') return left >= right;
        if (op === '<') return left < right;
        return left <= right;
      }
      if (typeof left === 'string' && typeof right === 'string') {
        if (op === '>') return left > right;
        if (op === '>=') return left >= right;
        if (op === '<') return left < right;
        return left <= right;
      }
      // Mixed or non-comparable types never match — no coercion, no throw.
      return false;
    }
    case '=~':
      return typeof left === 'string' && regex !== undefined && regex.test(left);
  }
}

function evaluate(node: AstNode, payload: unknown): boolean {
  switch (node.kind) {
    case 'or':
      return node.parts.some((p) => evaluate(p, payload));
    case 'and':
      return node.parts.every((p) => evaluate(p, payload));
    case 'not':
      return !evaluate(node.expr, payload);
    case 'exists':
      return resolvePath(payload, node.path) !== undefined;
    case 'compare':
      return compareValues(node.op, resolvePath(payload, node.path), node.value, node.regex);
  }
}

/**
 * Compiles a filter expression into a reusable predicate.
 *
 * @param expr the expression source, e.g. `type == "trade" && price > 100`.
 * @param options.version an optional version stamp (EB-229), recorded
 * on the returned object so a versioned predicate carries its version
 * with it. Must be a positive integer when given.
 * @returns the compiled expression; `test(payload)` evaluates it and
 * never throws.
 * @throws `TypeError` when `expr` is not a string.
 * @throws `RangeError` when the expression is empty, has a syntax error,
 * uses an unknown operator, carries an invalid regex, or carries an
 * invalid version stamp.
 */
export function compileFilterExpression(
  expr: string,
  options?: { version?: number },
): CompiledFilterExpression {
  if (typeof expr !== 'string') {
    throw new TypeError('filterExpr must be a string');
  }
  if (expr.length > MAX_EXPR_LENGTH) {
    throw new RangeError(`filterExpr: expression exceeds ${MAX_EXPR_LENGTH} characters`);
  }
  if (expr.trim().length === 0) {
    throw new RangeError('filterExpr: expression must not be empty');
  }
  const version = options?.version;
  if (
    version !== undefined &&
    (!Number.isInteger(version) || (version as number) < 1)
  ) {
    throw new RangeError(`filterExpr: version must be a positive integer, got ${version}`);
  }
  if (typeof expr !== 'string') {
    throw new TypeError('filterExpr must be a string');
  }
  if (expr.length > MAX_EXPR_LENGTH) {
    throw new RangeError(`filterExpr: expression exceeds ${MAX_EXPR_LENGTH} characters`);
  }
  if (expr.trim().length === 0) {
    throw new RangeError('filterExpr: expression must not be empty');
  }
  const tokens = tokenize(expr);
  const ast = new Parser(tokens, expr).parse();
  const test = (payload: unknown): boolean => {
    try {
      return evaluate(ast, payload);
    } catch {
      // Unreachable by construction (resolvePath never throws), kept so
      // the never-throws contract holds even if the AST grows new node
      // kinds later.
      return false;
    }
  };
  return { source: expr, version, test };
}
