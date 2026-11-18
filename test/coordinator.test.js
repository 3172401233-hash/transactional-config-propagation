import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { ConfigCoordinator } from '../src/coordinator.js';

const V1 = { featureX: false, limits: { qps: 100 } };
const V2 = { featureX: true, newField: 'enabled', limits: { qps: 200 } };
const V3 = { featureX: true, newField: 'v3', limits: { qps: 300 } };

function makeCoordinator({ ackTimeoutMs = 80 } = {}) {
  const c = new ConfigCoordinator({ ackTimeoutMs });
  c.registerService('db-proxy');
  c.registerService('auth', { dependsOn: ['db-proxy'] });
  c.registerService('api', { dependsOn: ['auth'] });
  c.registerService('worker', { dependsOn: ['db-proxy'] });
  return c;
}

async function commit(c, version, config, services) {
  const p = c.publish(version, config);
  for (const s of services) c.ack(s, version);
  const result = await p;
  assert.equal(result.status, 'committed');
  return result;
}

test('happy path: barrier then atomic activation, all services see same version', async () => {
  const c = makeCoordinator();
  const r1 = await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);
  assert.deepEqual(r1.visibleVersions, { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });

  // During PREPARING the visible version must not move.
  const p2 = c.publish('v2', V2);
  c.ack('db-proxy', 'v2');
  c.ack('auth', 'v2');
  assert.equal(c.visibleVersion('api'), 'v1', 'un-acked service still on v1');
  assert.equal(c.visibleVersion('db-proxy'), 'v1', 'acked service still staged, not visible');
  c.ack('api', 'v2');
  c.ack('worker', 'v2');
  const r2 = await p2;
  assert.equal(r2.status, 'committed');
  assert.deepEqual(r2.visibleVersions, { 'db-proxy': 'v2', auth: 'v2', api: 'v2', worker: 'v2' });
});

test('slow instance: ack timeout aborts, no partial switch, late ack ignored', async () => {
  const c = makeCoordinator({ ackTimeoutMs: 40 });
  await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);

  const p = c.publish('v2', V2);
  c.ack('db-proxy', 'v2');
  c.ack('auth', 'v2');
  c.ack('api', 'v2');
  // 'worker' is slow and never acks in time.
  const result = await p;
  assert.equal(result.status, 'aborted');
  assert.equal(result.reason, 'ack-timeout');
  assert.deepEqual(result.pendingServices, ['worker']);
  // No partial switch: everyone still on v1.
  assert.deepEqual(c.visibleVersions(), { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });

  // The slow ack finally arrives — it is stale and must be ignored.
  const late = c.ack('worker', 'v2');
  assert.equal(late.accepted, false);
  assert.equal(late.reason, 'stale-or-unknown-version');
  assert.deepEqual(c.visibleVersions(), { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });
});

test('duplicate acks are idempotent and do not double-count', async () => {
  const c = makeCoordinator();
  const p = c.publish('v1', V1);
  assert.equal(c.ack('db-proxy', 'v1').accepted, true);
  const dup = c.ack('db-proxy', 'v1');
  assert.equal(dup.accepted, false);
  assert.equal(dup.reason, 'duplicate');
  // Publish still waiting for the rest — duplicates did not complete it.
  assert.equal(c.activeVersion, null);
  c.ack('auth', 'v1');
  c.ack('api', 'v1');
  c.ack('worker', 'v1');
  const result = await p;
  assert.equal(result.status, 'committed');
});

test('late ack from a previous (aborted) publish cannot corrupt a new publish', async () => {
  const c = makeCoordinator({ ackTimeoutMs: 30 });
  await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);

  // v2 publish aborts because worker is slow.
  const p2 = c.publish('v2', V2);
  c.ack('db-proxy', 'v2');
  const r2 = await p2;
  assert.equal(r2.status, 'aborted');

  // Retry as v2b with the same config; stale v2 acks must not count.
  const p3 = c.publish('v2b', V2);
  assert.equal(c.ack('auth', 'v2').accepted, false, 'stale v2 ack ignored');
  assert.equal(c.ack('worker', 'v2').accepted, false, 'late slow ack ignored');
  c.ack('db-proxy', 'v2b');
  c.ack('auth', 'v2b');
  c.ack('api', 'v2b');
  c.ack('worker', 'v2b');
  const r3 = await p3;
  assert.equal(r3.status, 'committed');
  assert.deepEqual(c.visibleVersions(), {
    'db-proxy': 'v2b', auth: 'v2b', api: 'v2b', worker: 'v2b',
  });
});

test('nack from any single service aborts the whole publish', async () => {
  const c = makeCoordinator();
  await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);

  const p = c.publish('v2', V2);
  c.ack('db-proxy', 'v2');
  c.ack('auth', 'v2', { ok: false, reason: 'schema-mismatch' });
  const result = await p;
  assert.equal(result.status, 'aborted');
  assert.match(result.reason, /nack:auth:schema-mismatch/);
  // Remaining acks after abort are stale.
  assert.equal(c.ack('api', 'v2').accepted, false);
  assert.deepEqual(c.visibleVersions(), { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });
});

test('dependency DAG: prepare order is topological, cycles rejected', async () => {
  const c = makeCoordinator();
  const p = c.publish('v1', V1);
  const started = c.events.find((e) => e.type === 'publish-started');
  const order = started.order;
  assert.ok(order.indexOf('db-proxy') < order.indexOf('auth'));
  assert.ok(order.indexOf('auth') < order.indexOf('api'));
  assert.ok(order.indexOf('db-proxy') < order.indexOf('worker'));
  c.cancel('v1');
  await p;

  const cyclic = new ConfigCoordinator();
  cyclic.registerService('a', { dependsOn: ['b'] });
  cyclic.registerService('b', { dependsOn: ['a'] });
  assert.throws(() => cyclic.publish('v1', {}), /dependency cycle/);
});

test('publish cancel: staged config discarded, visible version unchanged', async () => {
  const c = makeCoordinator();
  await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);

  const p = c.publish('v2', V2);
  c.ack('db-proxy', 'v2');
  assert.equal(c.cancel('v2'), true);
  const result = await p;
  assert.equal(result.status, 'aborted');
  assert.equal(result.reason, 'cancelled');
  assert.deepEqual(c.visibleVersions(), { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });
  assert.equal(c.committed.has('v2'), false, 'cancelled version never committed');
  // Cancel is a no-op once nothing is in flight.
  assert.equal(c.cancel('v2'), false);
});

test('rollback: flips every service back to the previous complete version', async () => {
  const c = makeCoordinator();
  await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);
  await commit(c, 'v2', V2, ['db-proxy', 'auth', 'api', 'worker']);
  await commit(c, 'v3', V3, ['db-proxy', 'auth', 'api', 'worker']);
  assert.equal(c.activeVersion, 'v3');

  const rb = c.rollback();
  assert.equal(rb.status, 'rolled-back');
  assert.equal(rb.from, 'v3');
  assert.equal(rb.to, 'v2');
  assert.deepEqual(rb.visibleVersions, { 'db-proxy': 'v2', auth: 'v2', api: 'v2', worker: 'v2' });
  // Config reads after rollback resolve to the previous version's payload.
  assert.deepEqual(c.readConfig('api').config, V2);

  const rb2 = c.rollback();
  assert.equal(rb2.to, 'v1');
  assert.deepEqual(c.visibleVersions(), { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });

  // Nothing left to roll back to.
  assert.equal(c.rollback().status, 'noop');
});

test('reads during rollback are never mixed across services', async () => {
  const c = makeCoordinator();
  await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);
  await commit(c, 'v2', V2, ['db-proxy', 'auth', 'api', 'worker']);

  const snapshots = [];
  const readAll = () => {
    const snap = c.visibleVersions();
    snapshots.push(snap);
    return snap;
  };

  readAll();
  const rb = c.rollback(); // synchronous atomic flip
  readAll();
  assert.equal(rb.status, 'rolled-back');

  // Every snapshot is version-consistent: all services on the same version.
  for (const snap of snapshots) {
    const versions = new Set(Object.values(snap));
    assert.equal(versions.size, 1, `mixed versions visible: ${JSON.stringify(snap)}`);
  }
  assert.deepEqual(snapshots[0], { 'db-proxy': 'v2', auth: 'v2', api: 'v2', worker: 'v2' });
  assert.deepEqual(snapshots[1], { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });
});

test('rollback aborts an in-flight publish instead of partially switching', async () => {
  const c = makeCoordinator();
  await commit(c, 'v1', V1, ['db-proxy', 'auth', 'api', 'worker']);
  await commit(c, 'v2', V2, ['db-proxy', 'auth', 'api', 'worker']);

  const p = c.publish('v3', V3);
  c.ack('db-proxy', 'v3');
  const rb = c.rollback();
  const result = await p;
  assert.equal(result.status, 'aborted');
  assert.equal(result.reason, 'superseded-by-rollback');
  assert.equal(rb.to, 'v1');
  assert.deepEqual(c.visibleVersions(), { 'db-proxy': 'v1', auth: 'v1', api: 'v1', worker: 'v1' });
});

test('concurrent publish is rejected while one is in flight', async () => {
  const c = makeCoordinator();
  const p = c.publish('v1', V1);
  await assert.rejects(() => c.publish('v2', V2), /still in progress/);
  c.cancel('v1');
  await p;
});
