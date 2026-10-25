import test from 'node:test';
import assert from 'node:assert/strict';
import { compileFilterExpression } from '../src/filterexpr.ts';
import {
  NO_FILTER_VERSION,
  TopicFilterVersions,
} from '../src/filter-version.ts';

test('filterVersion: create starts at v1 and stamps the compiled predicate', () => {
  const versions = new TopicFilterVersions();
  const record = versions.createTopicFilter('trades', 'price > 100');
  assert.equal(record.version, 1);
  assert.equal(record.source, 'price > 100');
  assert.equal(record.compiled.version, 1);
  assert.equal(record.compiled.test({ price: 150 }), true);
  assert.equal(record.compiled.test({ price: 50 }), false);
  assert.equal(record.supersededBy, undefined);
  assert.equal(record.rolledBackFrom, undefined);
  assert.equal(versions.currentVersion('trades'), 1);
  assert.equal(versions.current('trades'), record);
  assert.deepEqual(versions.history('trades').map((r) => r.version), [1]);
  assert.deepEqual(versions.topics(), ['trades']);
});

test('filterVersion: update appends v+1 and retains the old version for audit', () => {
  const versions = new TopicFilterVersions();
  versions.createTopicFilter('trades', 'price > 100', 1000);
  const v2 = versions.updateTopicFilter('trades', 'price > 200', 2000);
  assert.equal(v2.version, 2);
  assert.equal(v2.source, 'price > 200');
  assert.equal(v2.createdAt, 2000);
  const history = versions.history('trades');
  assert.deepEqual(history.map((r) => r.version), [1, 2]);
  assert.equal(history[0].source, 'price > 100');
  assert.equal(history[0].supersededBy, 2);
  assert.equal(history[0].createdAt, 1000);
  assert.equal(history[1].supersededBy, undefined);
  // Historical evaluation is unchanged by the update.
  assert.equal(versions.evaluateAtVersion('trades', 1, { price: 150 }), true);
  assert.equal(versions.evaluateAtVersion('trades', 2, { price: 150 }), false);
  assert.equal(versions.evaluateCurrent('trades', { price: 250 }), true);
});

test('filterVersion: create twice and update before create both fail', () => {
  const versions = new TopicFilterVersions();
  assert.throws(() => versions.updateTopicFilter('t', 'x == 1'), /createTopicFilter/);
  versions.createTopicFilter('t', 'x == 1');
  assert.throws(() => versions.createTopicFilter('t', 'x == 2'), /updateTopicFilter/);
  assert.throws(() => versions.rollback('t', 9), /no filter version 9/);
  assert.throws(() => versions.rollback('missing', 1), /no filter version 1/);
  assert.throws(() => versions.createTopicFilter('', 'x == 1'), /non-empty string/);
});

test('filterVersion: rollback appends a new version copying the old source', () => {
  const versions = new TopicFilterVersions();
  versions.createTopicFilter('t', 'price > 100');
  versions.updateTopicFilter('t', 'price > 200');
  versions.updateTopicFilter('t', 'price > 300');
  const rolled = versions.rollback('t', 1);
  assert.equal(rolled.version, 4);
  assert.equal(rolled.source, 'price > 100');
  assert.equal(rolled.rolledBackFrom, 1);
  assert.equal(versions.currentVersion('t'), 4);
  assert.deepEqual(versions.history('t').map((r) => r.version), [1, 2, 3, 4]);
  // The counter never rewinds: the next update is v5, not v2.
  assert.equal(versions.updateTopicFilter('t', 'price > 400').version, 5);
  assert.equal(versions.evaluateCurrent('t', { price: 150 }), false);
  assert.equal(versions.evaluateAtVersion('t', 1, { price: 150 }), true);
});

test('filterVersion: evaluateCurrent admits when no filter is set', () => {
  const versions = new TopicFilterVersions();
  assert.equal(versions.evaluateCurrent('unset', { anything: true }), true);
  assert.equal(versions.current('unset'), undefined);
  assert.equal(versions.currentVersion('unset'), undefined);
  assert.deepEqual(versions.history('unset'), []);
});

test('filterVersion: unknown topic or version fails closed, never falls back to current', () => {
  const versions = new TopicFilterVersions();
  versions.createTopicFilter('t', 'price > 100');
  assert.equal(versions.evaluateAtVersion('t', 99, { price: 9999 }), false);
  assert.equal(versions.evaluateAtVersion('missing', 1, { price: 9999 }), false);
  assert.equal(versions.evaluateRecord('t', { payload: { price: 9999 }, filterVersion: 99 }), false);
  // Version 0 means "admitted with no filter": replay keeps it admitted.
  assert.equal(versions.evaluateAtVersion('t', NO_FILTER_VERSION, { price: 1 }), true);
});

test('filterVersion: stamp + evaluateRecord replay under the admit-time version', () => {
  const versions = new TopicFilterVersions();
  versions.createTopicFilter('t', 'price > 100');
  const admitted = versions.stamp('t', { price: 150 });
  assert.equal(admitted.filterVersion, 1);
  // The predicate tightens after admit: replay must still use v1.
  versions.updateTopicFilter('t', 'price > 200');
  assert.equal(versions.evaluateRecord('t', admitted), true);
  assert.equal(versions.evaluateCurrent('t', { price: 150 }), false);
  const rejected = versions.stamp('t', { price: 150 });
  assert.equal(rejected.filterVersion, 2);
  assert.equal(versions.evaluateRecord('t', rejected), false);
  // A record admitted before any filter existed stays admitted.
  const unfiltered = versions.stamp('u', { price: 1 });
  assert.equal(unfiltered.filterVersion, NO_FILTER_VERSION);
  versions.createTopicFilter('u', 'price > 100');
  assert.equal(versions.evaluateRecord('u', unfiltered), true);
});

test('filterVersion: compileFilterExpression version stamp validation', () => {
  assert.equal(compileFilterExpression('x == 1').version, undefined);
  assert.equal(compileFilterExpression('x == 1', { version: 3 }).version, 3);
  assert.throws(() => compileFilterExpression('x == 1', { version: 0 }), /positive integer/);
  assert.throws(() => compileFilterExpression('x == 1', { version: -2 }), /positive integer/);
  assert.throws(() => compileFilterExpression('x == 1', { version: 1.5 }), /positive integer/);
  // An invalid expression still fails at compile time, never at delivery.
  assert.throws(() => versionsCreateInvalid(), /filterExpr/);
  function versionsCreateInvalid(): unknown {
    const versions = new TopicFilterVersions();
    return versions.createTopicFilter('t', 'price >> 100');
  }
});
