import { ConfigCoordinator } from '../src/coordinator.js';

const c = new ConfigCoordinator({ ackTimeoutMs: 200 });
c.registerService('db-proxy');
c.registerService('auth', { dependsOn: ['db-proxy'] });
c.registerService('api', { dependsOn: ['auth'] });

const show = (label) => console.log(label.padEnd(34), c.visibleVersions());

// v1 commits.
let p = c.publish('v1', { featureX: false });
for (const s of ['db-proxy', 'auth', 'api']) c.ack(s, 'v1');
await p;
show('after v1 commit');

// v2: 'api' is slow -> timeout -> abort, no partial switch.
p = c.publish('v2', { featureX: true, newField: 'on' });
c.ack('db-proxy', 'v2');
c.ack('auth', 'v2');
console.log('v2 aborted:', (await p).reason);
show('after v2 abort (still v1)');

// v3 commits, then rolls back to v1.
p = c.publish('v3', { featureX: true, newField: 'v3' });
for (const s of ['db-proxy', 'auth', 'api']) c.ack(s, 'v3');
await p;
show('after v3 commit');
c.rollback();
show('after rollback to v1');
