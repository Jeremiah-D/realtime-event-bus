/**
 * GraphQL subscription gateway (EB-185): exposes an `EventBus` over the
 * graphql-ws protocol (https://github.com/enisdenjo/graphql-ws), so
 * GraphQL clients (Apollo, urql, graphql-ws) can subscribe to bus
 * topics with field selection, variables and filter expressions — zero
 * new dependencies (`node:net` + `node:crypto` only).
 *
 * This gateway is orthogonal to `gateway-ws.ts`: that one speaks the
 * bus's own framing, this one speaks the graphql-ws wire protocol. The
 * RFC 6455 server-side codec below mirrors the one in `gateway-ws.ts`
 * (same opcodes, same masking rules, same incremental parsing).
 *
 * Protocol (graphql-ws subset):
 *
 *   client -> server
 *   - `{type:'connection_init', payload?}` — optional `payload` (see
 *     auth below). Answered with `{type:'connection_ack'}`. Must arrive
 *     within `connectionInitTimeoutMs` of the handshake, otherwise the
 *     connection closes with 4429.
 *   - `{type:'subscribe', id, payload:{query, variables?, operationName?}}`
 *     — validates the query (see below) and subscribes the root
 *     field's `topic` argument on the bus. Query problems get an
 *     `error` frame and no subscription is created.
 *   - `{type:'complete', id}` — unsubscribes; answered with
 *     `{type:'complete', id}`. Unknown ids are ignored (idempotent).
 *   - `{type:'ping', payload?}` / `{type:'pong', payload?}` —
 *     protocol-level keepalive; ping is answered with pong (payload
 *     echoed when present).
 *
 *   server -> client
 *   - `{type:'next', id, payload}` — one bus message; `payload` carries
 *     exactly the selected root fields.
 *   - `{type:'error', id, payload:[{message}]}` — query validation or
 *     subscribe failure; the connection stays open.
 *   - `{type:'complete', id}` — confirms an unsubscribe.
 *
 *   Close codes (graphql-ws conventions): 4400 bad request, 4401
 *   unauthorized, 4409 duplicate subscription id, 4429 connection-init
 *   timeout.
 *
 * Query mapping: only `subscription` operations with a single root
 * field are accepted, e.g.
 *
 *   subscription ($t: String!) {
 *     events(topic: $t, filter: "price > 100") { topic payload seq }
 *   }
 *
 * Root-field arguments: `topic: String!` (required topic pattern),
 * `filter: String` (optional `filterexpr` expression, compiled with
 * `compileFilterExpression` and installed as the bus subscription's
 * `filter`). Variables (`$var`) are substituted from `variables`;
 * `operationName` picks one operation out of a multi-operation
 * document. The selection set decides each `next` payload's shape —
 * one of `topic`, `payload`, `seq`, `messageId`, `key`, `eventTime`.
 * Unknown fields, unknown arguments, a missing `topic`, a
 * non-subscription operation and uncompilable filters all produce
 * `error` frames instead of subscriptions.
 *
 * Authentication is opt-in: with `authToken` set, `connection_init`'s
 * payload must carry `authorization: "Bearer <token>"` (constant-time
 * compare); otherwise the connection closes with 4401.
 *
 * Delivery rides the normal bus subscriber path — `bus.subscribe`
 * with the compiled filter — so the subscriber's backpressure policy
 * applies unchanged. `complete` and connection teardown unsubscribe.
 * `GraphqlGateway.getStats()` reports `{connections, subscriptions,
 * messagesSent, protocolErrors, authFailures}`: `connections` is the
 * cumulative count of accepted sockets, `subscriptions` the live
 * subscription count, `messagesSent` the cumulative `next` frames
 * delivered.
 */
import {
  createServer as createTcpServer,
  type Server as TcpServer,
  type Socket,
} from 'node:net';
import { createHash, timingSafeEqual } from 'node:crypto';
import {
  EventBus,
  type BusMessage,
  type Subscription,
} from './bus.ts';
import { compileFilterExpression } from './filterexpr.ts';

// --- RFC 6455 codec (server side, mirrors gateway-ws.ts) ---------------------

const OP_CONT = 0x0;
const OP_TEXT = 0x1;
const OP_BINARY = 0x2;
const OP_CLOSE = 0x8;
const OP_PING = 0x9;
const OP_PONG = 0xa;
const WS_GUID = '258EAFA5-E91447DA-C5AB0DC85B11';
const WS_SUBPROTOCOL = 'graphql-ws';

function wsAcceptKey(key: string): string {
  return createHash('sha1').update(key + WS_GUID).digest('base64');
}

/** Encode one server-to-client frame: servers never mask (RFC 6455 §5.3). */
function encodeFrame(opcode: number, payload: Buffer): Buffer {
  const len = payload.length;
  const b0 = 0x80 | opcode;
  let header: Buffer;
  if (len < 126) {
    header = Buffer.from([b0, len]);
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = b0;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = b0;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

interface WsMessage {
  opcode: number;
  payload: Buffer;
}

/**
 * Incremental frame parser with fragmentation assembly: feed socket
 * chunks, pull out complete messages. Masked inbound frames are unmasked
 * (clients must mask; unmasked frames are tolerated). Throws on frames
 * whose declared length exceeds `maxFrameBytes`.
 */
class FrameParser {
  private buf = Buffer.alloc(0);
  private fragments: Buffer[] = [];
  private fragmentOpcode = -1;
  private readonly maxFrameBytes: number;

  constructor(maxFrameBytes: number) {
    this.maxFrameBytes = maxFrameBytes;
  }

  push(chunk: Buffer): WsMessage[] {
    this.buf = this.buf.length === 0 ? chunk : Buffer.concat([this.buf, chunk]);
    const out: WsMessage[] = [];
    for (;;) {
      const frame = this.nextFrame();
      if (frame === undefined) return out;
      const { fin, opcode, payload } = frame;
      if (opcode === OP_CONT) {
        if (this.fragmentOpcode === -1) throw new Error('websocket: unexpected continuation');
        this.fragments.push(payload);
        if (fin) {
          out.push({ opcode: this.fragmentOpcode, payload: Buffer.concat(this.fragments) });
          this.fragments = [];
          this.fragmentOpcode = -1;
        }
        continue;
      }
      if (!fin && (opcode === OP_TEXT || opcode === OP_BINARY)) {
        this.fragments = [payload];
        this.fragmentOpcode = opcode;
        continue;
      }
      out.push({ opcode, payload });
    }
  }

  private nextFrame(): { fin: boolean; opcode: number; payload: Buffer } | undefined {
    const b = this.buf;
    if (b.length < 2) return undefined;
    const fin = (b[0] & 0x80) !== 0;
    const rsv = b[0] & 0x70;
    if (rsv !== 0) throw new Error('websocket: RSV bits set without negotiated extension');
    const opcode = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) {
      if (b.length < 4) return undefined;
      len = b.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      if (b.length < 10) return undefined;
      const hi = b.readUInt32BE(2);
      const lo = b.readUInt32BE(6);
      if (hi !== 0 || lo > 0x7fffffff) throw new Error('websocket: frame too large');
      len = lo;
      off = 10;
    }
    if (len > this.maxFrameBytes) throw new Error('websocket: frame exceeds maxFrameBytes');
    const maskOff = masked ? 4 : 0;
    if (b.length < off + maskOff + len) return undefined;
    let payload = b.subarray(off + maskOff, off + maskOff + len);
    if (masked) {
      const mask = b.subarray(off, off + 4);
      const unmasked = Buffer.alloc(len);
      for (let i = 0; i < len; i++) unmasked[i] = payload[i] ^ mask[i % 4];
      payload = unmasked;
    }
    this.buf = b.subarray(off + maskOff + len);
    return { fin, opcode, payload };
  }
}

// graphql-ws close codes.
const CLOSE_BAD_REQUEST = 4400;
const CLOSE_UNAUTHORIZED = 4401;
const CLOSE_DUPLICATE_ID = 4409;
const CLOSE_INIT_TIMEOUT = 4429;

const HANDSHAKE_TIMEOUT_MS = 10_000;
const MAX_HANDSHAKE_BYTES = 8 * 1024;
const MAX_FRAME_BYTES = 1024 * 1024;
const DEFAULT_INIT_TIMEOUT_MS = 5_000;

function fail(msg: string): never {
  throw new RangeError(`invalid graphql gateway options: ${msg}`);
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null;
}

// --- GraphQL subscription subset parser --------------------------------------
// Hand-written tokenizer + recursive-descent parser for exactly the
// subset the gateway accepts: operations with a single root field and
// argument values (strings, numbers, booleans, null, enums carried as
// their name, lists, input objects, $variables). Fragments,
// directives and block strings are rejected — they have no meaning for
// the mapping below.

type GqlToken =
  | { kind: 'braceL' }
  | { kind: 'braceR' }
  | { kind: 'parenL' }
  | { kind: 'parenR' }
  | { kind: 'bracketL' }
  | { kind: 'bracketR' }
  | { kind: 'colon' }
  | { kind: 'dollar' }
  | { kind: 'bang' }
  | { kind: 'name'; value: string }
  | { kind: 'string'; value: string }
  | { kind: 'int'; value: string }
  | { kind: 'float'; value: string };

const GQL_NAME_START = /[_A-Za-z]/;
const GQL_NAME_PART = /[_0-9A-Za-z]/;
const GQL_NUMBER = /-?(?:0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?/y;

function tokenizeGraphql(src: string): GqlToken[] {
  const tokens: GqlToken[] = [];
  let i = 0;
  const n = src.length;
  const bad = (offset: number, message: string): never => {
    throw new Error(`graphql: ${message} (at offset ${offset})`);
  };
  while (i < n) {
    const ch = src[i];
    // Whitespace, commas and comments are insignificant.
    if (
      ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' ||
      ch === ',' || ch === '\ufeff'
    ) {
      i += 1;
      continue;
    }
    if (ch === '#') {
      while (i < n && src[i] !== '\n') i += 1;
      continue;
    }
    const start = i;
    switch (ch) {
      case '{':
        tokens.push({ kind: 'braceL' });
        i += 1;
        continue;
      case '}':
        tokens.push({ kind: 'braceR' });
        i += 1;
        continue;
      case '(':
        tokens.push({ kind: 'parenL' });
        i += 1;
        continue;
      case ')':
        tokens.push({ kind: 'parenR' });
        i += 1;
        continue;
      case '[':
        tokens.push({ kind: 'bracketL' });
        i += 1;
        continue;
      case ']':
        tokens.push({ kind: 'bracketR' });
        i += 1;
        continue;
      case ':':
        tokens.push({ kind: 'colon' });
        i += 1;
        continue;
      case '$':
        tokens.push({ kind: 'dollar' });
        i += 1;
        continue;
      case '!':
        // Non-null marker in variable definitions (`String!`) — the
        // definitions are skipped as balanced parens, this just needs
        // to tokenize.
        tokens.push({ kind: 'bang' });
        i += 1;
        continue;
      case '"': {
        if (src[i + 1] === '"' && src[i + 2] === '"') bad(start, 'block strings are not supported');
        let value = '';
        i += 1;
        let closed = false;
        while (i < n) {
          const c = src[i];
          if (c === '"') {
            closed = true;
            i += 1;
            break;
          }
          if (c === '\\') {
            const e = src[i + 1];
            switch (e) {
              case '"':
                value += '"';
                break;
              case '\\':
                value += '\\';
                break;
              case '/':
                value += '/';
                break;
              case 'b':
                value += '\b';
                break;
              case 'f':
                value += '\f';
                break;
              case 'n':
                value += '\n';
                break;
              case 'r':
                value += '\r';
                break;
              case 't':
                value += '\t';
                break;
              case 'u': {
                const hex = src.slice(i + 2, i + 6);
                if (!/^[0-9a-fA-F]{4}$/.test(hex)) bad(i, 'bad unicode escape (expected \\uXXXX)');
                value += String.fromCharCode(parseInt(hex, 16));
                i += 4;
                break;
              }
              default:
                bad(i, `bad escape '\\${e ?? ''}'`);
            }
            i += 2;
            continue;
          }
          if (c === '\n' || c === '\r') bad(i, 'unterminated string literal');
          value += c;
          i += 1;
        }
        if (!closed) bad(start, 'unterminated string literal');
        tokens.push({ kind: 'string', value });
        continue;
      }
      default:
        break;
    }
    if (ch === '-' || (ch >= '0' && ch <= '9')) {
      GQL_NUMBER.lastIndex = i;
      const m = GQL_NUMBER.exec(src);
      if (m === null) bad(start, 'bad number literal');
      const raw = m[0];
      tokens.push(
        m[1] !== undefined || m[2] !== undefined
          ? { kind: 'float', value: raw }
          : { kind: 'int', value: raw },
      );
      i += raw.length;
      continue;
    }
    if (GQL_NAME_START.test(ch)) {
      let j = i + 1;
      while (j < n && GQL_NAME_PART.test(src[j])) j += 1;
      tokens.push({ kind: 'name', value: src.slice(i, j) });
      i = j;
      continue;
    }
    bad(start, `unexpected character '${ch}'`);
  }
  return tokens;
}

type GqlValue =
  | string
  | number
  | boolean
  | null
  | { __variable: string }
  | GqlValue[]
  | { [key: string]: GqlValue };

interface GqlField {
  name: string;
  args: Record<string, GqlValue>;
  selections: GqlField[] | null;
}

interface GqlOperation {
  kind: 'query' | 'mutation' | 'subscription';
  name: string | null;
  fields: GqlField[];
}

class GqlParser {
  private pos = 0;
  private readonly tokens: GqlToken[];

  constructor(tokens: GqlToken[]) {
    this.tokens = tokens;
  }

  private peek(): GqlToken | undefined {
    return this.tokens[this.pos];
  }

  private next(): GqlToken {
    const t = this.tokens[this.pos++];
    if (t === undefined) throw new Error('graphql: unexpected end of query');
    return t;
  }

  private expect(kind: GqlToken['kind'], what: string): void {
    const t = this.next();
    if (t.kind !== kind) throw new Error(`graphql: expected ${what}`);
  }

  parseDocument(): GqlOperation[] {
    const ops: GqlOperation[] = [];
    while (this.peek() !== undefined) ops.push(this.parseOperation());
    return ops;
  }

  private parseOperation(): GqlOperation {
    const t = this.peek();
    if (t !== undefined && t.kind === 'braceL') {
      // Anonymous shorthand is always a query — rejected by validation.
      return { kind: 'query', name: null, fields: this.parseSelectionSet() };
    }
    if (
      t === undefined ||
      t.kind !== 'name' ||
      (t.value !== 'query' && t.value !== 'mutation' && t.value !== 'subscription')
    ) {
      throw new Error('graphql: expected query, mutation, subscription or a selection set');
    }
    const kind = t.value;
    this.next();
    let name: string | null = null;
    const after = this.peek();
    if (after !== undefined && after.kind === 'name') {
      name = after.value;
      this.next();
    }
    // Variable definitions (`$var: Type`) ride the wire as `variables`;
    // skip them as balanced parens.
    if (this.peek()?.kind === 'parenL') this.skipBalanced('parenL', 'parenR');
    return { kind, name, fields: this.parseSelectionSet() };
  }

  private skipBalanced(open: 'parenL' | 'bracketL' | 'braceL', close: 'parenR' | 'bracketR' | 'braceR'): void {
    this.expect(open, `'${open}'`);
    let depth = 1;
    for (;;) {
      const t = this.next();
      if (t.kind === open) depth += 1;
      else if (t.kind === close) {
        depth -= 1;
        if (depth === 0) return;
      }
    }
  }

  private parseSelectionSet(): GqlField[] {
    this.expect('braceL', "'{'");
    const fields: GqlField[] = [];
    for (;;) {
      const t = this.peek();
      if (t === undefined) throw new Error("graphql: unterminated selection set, expected '}'");
      if (t.kind === 'braceR') {
        this.next();
        return fields;
      }
      fields.push(this.parseField());
    }
  }

  private parseField(): GqlField {
    const t = this.next();
    if (t.kind !== 'name') throw new Error('graphql: expected a field name');
    const args: Record<string, GqlValue> = {};
    if (this.peek()?.kind === 'parenL') {
      this.next();
      for (;;) {
        const p = this.peek();
        if (p === undefined) throw new Error("graphql: unterminated argument list, expected ')'");
        if (p.kind === 'parenR') {
          this.next();
          break;
        }
        const an = this.next();
        if (an.kind !== 'name') throw new Error('graphql: expected an argument name');
        this.expect('colon', "':'");
        args[an.value] = this.parseValue();
      }
    }
    let selections: GqlField[] | null = null;
    if (this.peek()?.kind === 'braceL') selections = this.parseSelectionSet();
    return { name: t.value, args, selections };
  }

  private parseValue(): GqlValue {
    const t = this.next();
    switch (t.kind) {
      case 'string':
        return t.value;
      case 'int':
        return parseInt(t.value, 10);
      case 'float':
        return parseFloat(t.value);
      case 'dollar': {
        const n = this.next();
        if (n.kind !== 'name') throw new Error("graphql: expected a variable name after '$'");
        return { __variable: n.value };
      }
      case 'bracketL': {
        const items: GqlValue[] = [];
        for (;;) {
          const p = this.peek();
          if (p === undefined) throw new Error("graphql: unterminated list, expected ']'");
          if (p.kind === 'bracketR') {
            this.next();
            return items;
          }
          items.push(this.parseValue());
        }
      }
      case 'braceL': {
        const obj: Record<string, GqlValue> = {};
        for (;;) {
          const p = this.peek();
          if (p === undefined) throw new Error("graphql: unterminated input object, expected '}'");
          if (p.kind === 'braceR') {
            this.next();
            return obj;
          }
          const k = this.next();
          if (k.kind !== 'name') throw new Error('graphql: expected an input-object field name');
          this.expect('colon', "':'");
          obj[k.value] = this.parseValue();
        }
      }
      case 'name':
        if (t.value === 'true') return true;
        if (t.value === 'false') return false;
        if (t.value === 'null') return null;
        return t.value; // enum literal — carried as its name
      default:
        throw new Error('graphql: unexpected token in value position');
    }
  }
}

/** Root fields selectable on a subscription event. */
const SELECTABLE_FIELDS = new Set(['topic', 'payload', 'seq', 'messageId', 'key', 'eventTime']);

interface ResolvedSubscription {
  pattern: string;
  predicate: ((payload: unknown) => boolean) | undefined;
  fields: string[];
}

function isVariableRef(v: GqlValue): v is { __variable: string } {
  return typeof v === 'object' && v !== null && !Array.isArray(v) && '__variable' in v;
}

function substituteVariables(value: GqlValue, variables: Record<string, unknown>): unknown {
  if (isVariableRef(value)) {
    const name = value.__variable;
    if (!Object.prototype.hasOwnProperty.call(variables, name)) {
      throw new Error(`graphql: unknown variable $${name}`);
    }
    return variables[name];
  }
  if (Array.isArray(value)) return value.map((v) => substituteVariables(v, variables));
  if (typeof value === 'object' && value !== null) {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) out[k] = substituteVariables(v, variables);
    return out;
  }
  return value;
}

/**
 * Validates a `subscribe` payload's query and resolves it to a bus
 * subscription: the topic pattern, the optional filter predicate and
 * the selected output fields. Throws `Error` with a client-facing
 * message on any violation — the caller turns it into an `error`
 * frame, never a subscription.
 */
function resolveSubscription(
  query: unknown,
  variables: unknown,
  operationName: unknown,
): ResolvedSubscription {
  if (typeof query !== 'string' || query.trim().length === 0) {
    throw new Error('graphql: query must be a non-empty string');
  }
  if (variables !== undefined && (!isRecord(variables) || Array.isArray(variables))) {
    throw new Error('graphql: variables must be an object');
  }
  if (
    operationName !== undefined &&
    (typeof operationName !== 'string' || operationName.length === 0)
  ) {
    throw new Error('graphql: operationName must be a non-empty string');
  }
  const vars = (variables ?? {}) as Record<string, unknown>;
  let ops: GqlOperation[];
  try {
    ops = new GqlParser(tokenizeGraphql(query)).parseDocument();
  } catch (err) {
    throw new Error(`graphql: invalid query: ${(err as Error).message.replace(/^graphql: /, '')}`);
  }
  if (ops.length === 0) throw new Error('graphql: empty query document');
  let op: GqlOperation | undefined;
  if (ops.length === 1) {
    op = ops[0];
    if (operationName !== undefined && op.name !== operationName) {
      throw new Error(`graphql: unknown operation ${JSON.stringify(operationName)}`);
    }
  } else {
    if (operationName === undefined) {
      throw new Error('graphql: operationName is required when the query has multiple operations');
    }
    op = ops.find((o) => o.name === operationName);
    if (op === undefined) {
      throw new Error(`graphql: unknown operation ${JSON.stringify(operationName)}`);
    }
  }
  if (op.kind !== 'subscription') {
    throw new Error('graphql: only subscription operations are supported');
  }
  if (op.fields.length !== 1) {
    throw new Error('graphql: exactly one root field is required');
  }
  const root = op.fields[0];
  for (const key of Object.keys(root.args)) {
    if (key !== 'topic' && key !== 'filter') {
      throw new Error(`graphql: unknown argument ${JSON.stringify(key)}`);
    }
  }
  const topic: unknown =
    'topic' in root.args ? substituteVariables(root.args.topic, vars) : undefined;
  const filter: unknown =
    'filter' in root.args ? substituteVariables(root.args.filter, vars) : undefined;
  if (typeof topic !== 'string' || topic.length === 0) {
    throw new Error('graphql: topic argument is required and must be a non-empty string');
  }
  let predicate: ((payload: unknown) => boolean) | undefined;
  if (filter !== undefined) {
    if (typeof filter !== 'string') {
      throw new Error('graphql: filter argument must be a string');
    }
    try {
      const compiled = compileFilterExpression(filter);
      predicate = (payload: unknown) => compiled.test(payload);
    } catch (err) {
      throw new Error(`graphql: invalid filter expression: ${(err as Error).message}`);
    }
  }
  if (root.selections === null || root.selections.length === 0) {
    throw new Error('graphql: the root field must select at least one field');
  }
  const fields: string[] = [];
  for (const sel of root.selections) {
    if (!SELECTABLE_FIELDS.has(sel.name)) {
      throw new Error(`graphql: unknown field ${JSON.stringify(sel.name)}`);
    }
    if (Object.keys(sel.args).length > 0) {
      throw new Error(`graphql: field ${JSON.stringify(sel.name)} takes no arguments`);
    }
    if (sel.selections !== null) {
      throw new Error(`graphql: field ${JSON.stringify(sel.name)} takes no sub-selections`);
    }
    if (!fields.includes(sel.name)) fields.push(sel.name);
  }
  return { pattern: topic, predicate, fields };
}

// --- Public types ------------------------------------------------------------

export interface GraphqlGatewayOptions {
  port: number;
  host?: string;
  /**
   * Opt-in bearer auth: when set, `connection_init`'s payload must
   * carry `authorization: "Bearer <token>"` (constant-time compare),
   * otherwise the connection closes with 4401.
   */
  authToken?: string;
  /**
   * Cap on concurrent connections; past it new sockets are refused
   * with 503 before the handshake. Unset means unlimited.
   */
  maxConnections?: number;
  /**
   * Milliseconds a fresh connection may take to send `connection_init`
   * before it is closed with 4429. Default 5000.
   */
  connectionInitTimeoutMs?: number;
}

export interface GraphqlGatewayStats {
  /** Cumulative accepted sockets. */
  connections: number;
  /** Currently live bus subscriptions across all connections. */
  subscriptions: number;
  /** Cumulative `next` frames delivered. */
  messagesSent: number;
  protocolErrors: number;
  authFailures: number;
}

interface GqlConnection {
  socket: Socket;
  parser: FrameParser;
  handshook: boolean;
  closed: boolean;
  initialized: boolean;
  initTimer: ReturnType<typeof setTimeout> | undefined;
  subscriptions: Map<string, Subscription>;
}

// --- Gateway ------------------------------------------------------------------

export class GraphqlGateway {
  private readonly bus: EventBus;
  private readonly host: string;
  private readonly port: number;
  private readonly authToken: string | undefined;
  private readonly maxConnections: number | undefined;
  private readonly connectionInitTimeoutMs: number;
  private server: TcpServer | null = null;
  private boundPort = 0;
  private readonly connections = new Set<GqlConnection>();
  private readonly stats = {
    connections: 0,
    messagesSent: 0,
    protocolErrors: 0,
    authFailures: 0,
  };

  constructor(bus: EventBus, options: GraphqlGatewayOptions) {
    this.bus = bus;
    const port = options.port ?? 0;
    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      fail('port must be an integer between 0 and 65535');
    }
    this.port = port;
    this.host = options.host ?? '127.0.0.1';
    const authToken = options.authToken;
    if (authToken !== undefined && (typeof authToken !== 'string' || authToken.length === 0)) {
      fail('authToken must be a non-empty string');
    }
    this.authToken = authToken;
    const maxConnections = options.maxConnections;
    if (
      maxConnections !== undefined &&
      (!Number.isInteger(maxConnections) || maxConnections <= 0)
    ) {
      fail('maxConnections must be a positive integer');
    }
    this.maxConnections = maxConnections;
    const connectionInitTimeoutMs = options.connectionInitTimeoutMs ?? DEFAULT_INIT_TIMEOUT_MS;
    if (!Number.isFinite(connectionInitTimeoutMs) || connectionInitTimeoutMs <= 0) {
      fail('connectionInitTimeoutMs must be a positive finite number');
    }
    this.connectionInitTimeoutMs = connectionInitTimeoutMs;
  }

  /** Starts listening; resolves once bound. */
  start(): Promise<void> {
    if (this.server != null) throw new Error('GraphqlGateway is already started');
    return new Promise((resolve, reject) => {
      const server = createTcpServer((socket) => this.handleConnection(socket));
      this.server = server;
      server.once('error', reject);
      server.listen(this.port, this.host, () => {
        server.off('error', reject);
        const addr = server.address();
        this.boundPort = typeof addr === 'object' && addr != null ? addr.port : this.port;
        resolve();
      });
    });
  }

  /** Drops every connection (unsubscribing its bus subscriptions) and stops. */
  async stop(): Promise<void> {
    for (const conn of this.connections) this.destroyConnection(conn);
    this.connections.clear();
    const server = this.server;
    this.server = null;
    if (server == null) return;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }

  /** The bound port (0 until `start()` resolves). */
  getPort(): number {
    return this.boundPort;
  }

  getStats(): GraphqlGatewayStats {
    let subscriptions = 0;
    for (const conn of this.connections) subscriptions += conn.subscriptions.size;
    return { ...this.stats, subscriptions };
  }

  // --- connection lifecycle --------------------------------------------------

  private handleConnection(socket: Socket): void {
    socket.setNoDelay(true);
    if (
      this.maxConnections !== undefined &&
      this.connections.size >= this.maxConnections
    ) {
      try {
        socket.write(
          'HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n',
        );
      } catch {
        // Best effort; the destroy below finishes the job.
      }
      socket.destroy();
      return;
    }
    const conn: GqlConnection = {
      socket,
      parser: new FrameParser(MAX_FRAME_BYTES),
      handshook: false,
      closed: false,
      initialized: false,
      initTimer: undefined,
      subscriptions: new Map(),
    };
    this.connections.add(conn);
    this.stats.connections += 1;
    let handshakeBuf = Buffer.alloc(0);
    const handshakeTimer = setTimeout(() => {
      if (!conn.handshook) this.destroyConnection(conn);
    }, HANDSHAKE_TIMEOUT_MS);
    if (typeof handshakeTimer.unref === 'function') handshakeTimer.unref();

    socket.on('data', (chunk: Buffer) => {
      if (conn.closed) return;
      if (!conn.handshook) {
        handshakeBuf = Buffer.concat([handshakeBuf, chunk]);
        if (handshakeBuf.length > MAX_HANDSHAKE_BYTES) {
          this.destroyConnection(conn);
          return;
        }
        const end = handshakeBuf.indexOf('\r\n\r\n');
        if (end === -1) return;
        const head = handshakeBuf.subarray(0, end).toString('latin1');
        handshakeBuf = Buffer.alloc(0);
        clearTimeout(handshakeTimer);
        this.handleHandshake(conn, head);
        return;
      }
      let messages: WsMessage[] = [];
      try {
        messages = conn.parser.push(chunk);
      } catch {
        this.stats.protocolErrors += 1;
        this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'frame too large');
        return;
      }
      for (const msg of messages) this.handleWsMessage(conn, msg);
    });
    socket.on('error', () => {});
    socket.on('close', () => this.destroyConnection(conn));
  }

  private handleHandshake(conn: GqlConnection, head: string): void {
    const lines = head.split('\r\n');
    const requestLine = lines[0] ?? '';
    const headers = new Map<string, string>();
    for (const line of lines.slice(1)) {
      const idx = line.indexOf(':');
      if (idx === -1) continue;
      headers.set(line.slice(0, idx).trim().toLowerCase(), line.slice(idx + 1).trim());
    }
    const reject = (status: string) => {
      try {
        conn.socket.write(`HTTP/1.1 ${status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
      } catch {
        // Best effort; the destroy below finishes the job.
      }
      this.destroyConnection(conn);
    };
    if (!/^GET \S+ HTTP\/1\.1$/.test(requestLine)) return reject('400 Bad Request');
    const upgrade = headers.get('upgrade') ?? '';
    if (!upgrade.toLowerCase().split(',').map((s) => s.trim()).includes('websocket')) {
      return reject('400 Bad Request');
    }
    if ((headers.get('sec-websocket-version') ?? '').trim() !== '13') {
      return reject('426 Upgrade Required');
    }
    const key = (headers.get('sec-websocket-key') ?? '').trim();
    let keyBytes: Buffer;
    try {
      keyBytes = Buffer.from(key, 'base64');
    } catch {
      return reject('400 Bad Request');
    }
    if (keyBytes.length !== 16) return reject('400 Bad Request');
    const protocols = (headers.get('sec-websocket-protocol') ?? '')
      .split(',')
      .map((s) => s.trim());
    if (!protocols.includes(WS_SUBPROTOCOL)) return reject('400 Bad Request');
    conn.socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${wsAcceptKey(key)}\r\n` +
        `Sec-WebSocket-Protocol: ${WS_SUBPROTOCOL}\r\n` +
        '\r\n',
    );
    conn.handshook = true;
    const initTimer = setTimeout(() => {
      if (!conn.initialized) {
        this.closeWithCode(conn, CLOSE_INIT_TIMEOUT, 'connection_init timeout');
      }
    }, this.connectionInitTimeoutMs);
    if (typeof initTimer.unref === 'function') initTimer.unref();
    conn.initTimer = initTimer;
  }

  private destroyConnection(conn: GqlConnection): void {
    if (conn.closed) return;
    conn.closed = true;
    if (conn.initTimer !== undefined) {
      clearTimeout(conn.initTimer);
      conn.initTimer = undefined;
    }
    this.connections.delete(conn);
    this.unsubscribeAll(conn);
    try {
      conn.socket.destroy();
    } catch {
      // Already gone.
    }
  }

  /**
   * Closes with a graphql-ws close code. The close frame is written
   * first and the socket is destroyed on the next tick, so the peer
   * reliably sees the code; subscriptions are torn down immediately.
   */
  private closeWithCode(conn: GqlConnection, code: number, reason: string): void {
    if (conn.closed) return;
    conn.closed = true;
    if (conn.initTimer !== undefined) {
      clearTimeout(conn.initTimer);
      conn.initTimer = undefined;
    }
    this.connections.delete(conn);
    this.unsubscribeAll(conn);
    const payload = Buffer.alloc(2 + Buffer.byteLength(reason));
    payload.writeUInt16BE(code, 0);
    payload.write(reason, 2);
    try {
      conn.socket.write(encodeFrame(OP_CLOSE, payload));
    } catch {
      // Best effort; the deferred destroy below finishes the job.
    }
    const timer = setTimeout(() => {
      try {
        conn.socket.destroy();
      } catch {
        // Already gone.
      }
    }, 25);
    if (typeof timer.unref === 'function') timer.unref();
  }

  private unsubscribeAll(conn: GqlConnection): void {
    for (const sub of conn.subscriptions.values()) {
      try {
        sub.unsubscribe();
      } catch {
        // Unsubscribe is idempotent-safe; never let teardown throw.
      }
    }
    conn.subscriptions.clear();
  }

  // --- outbound --------------------------------------------------------------

  private sendJson(conn: GqlConnection, value: unknown): void {
    if (conn.closed) return;
    let text: string;
    try {
      text = JSON.stringify(value) ?? 'null';
    } catch {
      this.stats.protocolErrors += 1;
      return;
    }
    try {
      conn.socket.write(encodeFrame(OP_TEXT, Buffer.from(text, 'utf8')));
    } catch {
      this.destroyConnection(conn);
    }
  }

  private sendError(conn: GqlConnection, id: string, message: string): void {
    this.sendJson(conn, { type: 'error', id, payload: [{ message }] });
  }

  private sendNext(conn: GqlConnection, id: string, fields: string[], msg: BusMessage): void {
    const payload: Record<string, unknown> = {};
    for (const field of fields) {
      switch (field) {
        case 'topic':
          payload.topic = msg.topic;
          break;
        case 'payload':
          payload.payload = msg.payload;
          break;
        case 'seq':
          payload.seq = msg.seq;
          break;
        case 'messageId':
          if (msg.messageId !== undefined) payload.messageId = msg.messageId;
          break;
        case 'key':
          if (msg.key !== undefined) payload.key = msg.key;
          break;
        case 'eventTime':
          if (msg.eventTime !== undefined) payload.eventTime = msg.eventTime;
          break;
        default:
          // Unreachable: fields are validated at subscribe time.
          break;
      }
    }
    this.sendJson(conn, { type: 'next', id, payload });
    this.stats.messagesSent += 1;
  }

  // --- inbound ---------------------------------------------------------------

  private handleWsMessage(conn: GqlConnection, msg: WsMessage): void {
    if (conn.closed) return;
    switch (msg.opcode) {
      case OP_TEXT: {
        let frame: unknown;
        try {
          frame = JSON.parse(msg.payload.toString('utf8'));
        } catch {
          this.stats.protocolErrors += 1;
          this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'frame is not valid JSON');
          return;
        }
        this.handleMessage(conn, frame);
        return;
      }
      case OP_BINARY:
        this.stats.protocolErrors += 1;
        this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'binary frames are not supported');
        return;
      case OP_PING:
        try {
          conn.socket.write(encodeFrame(OP_PONG, msg.payload));
        } catch {
          this.destroyConnection(conn);
        }
        return;
      case OP_PONG:
        return;
      case OP_CLOSE:
        this.destroyConnection(conn);
        return;
      default:
        this.stats.protocolErrors += 1;
        this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'unknown opcode');
    }
  }

  private handleMessage(conn: GqlConnection, frame: unknown): void {
    if (conn.closed) return;
    if (!isRecord(frame) || typeof frame.type !== 'string') {
      this.stats.protocolErrors += 1;
      this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'message must be an object with a string type');
      return;
    }
    switch (frame.type) {
      case 'connection_init':
        this.handleConnectionInit(conn, frame);
        return;
      case 'subscribe':
        this.handleSubscribe(conn, frame);
        return;
      case 'complete':
        this.handleComplete(conn, frame);
        return;
      case 'ping':
        this.sendJson(conn, {
          type: 'pong',
          ...(frame.payload !== undefined ? { payload: frame.payload } : {}),
        });
        return;
      case 'pong':
        return;
      default:
        this.stats.protocolErrors += 1;
        this.closeWithCode(conn, CLOSE_BAD_REQUEST, `unknown message type: ${frame.type}`);
    }
  }

  private handleConnectionInit(conn: GqlConnection, frame: Record<string, unknown>): void {
    if (conn.initialized) {
      this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'already initialized');
      return;
    }
    if (this.authToken !== undefined) {
      const presented = isRecord(frame.payload) ? frame.payload.authorization : undefined;
      const expected = `Bearer ${this.authToken}`;
      const ok =
        typeof presented === 'string' &&
        presented.length === expected.length &&
        timingSafeEqual(Buffer.from(presented), Buffer.from(expected));
      if (!ok) {
        this.stats.authFailures += 1;
        this.closeWithCode(conn, CLOSE_UNAUTHORIZED, 'unauthorized');
        return;
      }
    }
    conn.initialized = true;
    if (conn.initTimer !== undefined) {
      clearTimeout(conn.initTimer);
      conn.initTimer = undefined;
    }
    this.sendJson(conn, { type: 'connection_ack' });
  }

  private handleSubscribe(conn: GqlConnection, frame: Record<string, unknown>): void {
    if (!conn.initialized) {
      this.closeWithCode(conn, CLOSE_UNAUTHORIZED, 'connection_init required first');
      return;
    }
    const id = frame.id;
    if (typeof id !== 'string' || id.length === 0) {
      this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'subscribe requires a non-empty string id');
      return;
    }
    if (conn.subscriptions.has(id)) {
      this.closeWithCode(conn, CLOSE_DUPLICATE_ID, `duplicate subscription id: ${id}`);
      return;
    }
    const payload = frame.payload;
    if (!isRecord(payload)) {
      this.sendError(conn, id, 'graphql: subscribe payload must be an object');
      return;
    }
    let resolved: ResolvedSubscription;
    try {
      resolved = resolveSubscription(payload.query, payload.variables, payload.operationName);
    } catch (err) {
      this.sendError(conn, id, (err as Error).message);
      return;
    }
    const handler = (msg: BusMessage): void => {
      this.sendNext(conn, id, resolved.fields, msg);
    };
    let sub: Subscription;
    try {
      sub = resolved.predicate !== undefined
        ? this.bus.subscribe(resolved.pattern, handler, {
          filter: (messagePayload: unknown) => resolved.predicate!(messagePayload),
        })
        : this.bus.subscribe(resolved.pattern, handler);
    } catch (err) {
      this.sendError(conn, id, (err as Error).message);
      return;
    }
    conn.subscriptions.set(id, sub);
  }

  private handleComplete(conn: GqlConnection, frame: Record<string, unknown>): void {
    if (!conn.initialized) {
      this.closeWithCode(conn, CLOSE_UNAUTHORIZED, 'connection_init required first');
      return;
    }
    const id = frame.id;
    if (typeof id !== 'string' || id.length === 0) {
      this.closeWithCode(conn, CLOSE_BAD_REQUEST, 'complete requires a non-empty string id');
      return;
    }
    const sub = conn.subscriptions.get(id);
    if (sub === undefined) return; // idempotent: unknown ids are ignored
    conn.subscriptions.delete(id);
    try {
      sub.unsubscribe();
    } catch {
      // Unsubscribe is idempotent-safe.
    }
    this.sendJson(conn, { type: 'complete', id });
  }
}
