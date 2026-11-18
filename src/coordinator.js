/**
 * ConfigCoordinator — transactional config propagation across services.
 *
 * Protocol per publish:
 *   1. PREPARING  : every target service is asked (in dependency order) to
 *                   load the candidate version into staging and ack it.
 *                   The visible (active) version is NOT changed yet.
 *   2. BARRIER    : once ALL targets have acked, the version barrier is
 *                   reached. Only now may activation happen.
 *   3. COMMITTED  : activation flips the visible version of every target
 *                   atomically (single synchronous step). No partial switch
 *                   is possible.
 *   4. ABORTED    : any nack, ack-timeout or cancel before the barrier
 *                   aborts the publish; staged config is discarded and every
 *                   service keeps its previous visible version.
 *
 * Rollback flips every service back to the previous fully-committed version.
 */
export class ConfigCoordinator {
  constructor({ ackTimeoutMs = 500 } = {}) {
    this.ackTimeoutMs = ackTimeoutMs;
    this.services = new Map();        // name -> { dependsOn: string[] }
    this.committed = new Map();       // version -> config (fully committed only)
    this.history = [];                // committed versions, oldest -> newest
    this.activeVersion = null;        // currently visible version (global barrier)
    this.visible = new Map();         // service -> version it currently sees
    this.currentPublish = null;       // in-flight publish record or null
    this.events = [];                 // audit log of transitions
  }

  registerService(name, { dependsOn = [] } = {}) {
    if (!this.services.has(name)) {
      this.services.set(name, { dependsOn: [...dependsOn] });
      if (this.activeVersion !== null) this.visible.set(name, this.activeVersion);
    }
    return this;
  }

  _log(type, detail = {}) {
    this.events.push({ type, ...detail });
  }

  /** Topological order (dependencies first) restricted to the target set. */
  _topoOrder(targets) {
    const inSet = new Set(targets);
    const indegree = new Map(targets.map((t) => [t, 0]));
    for (const t of targets) {
      for (const dep of this.services.get(t).dependsOn) {
        if (inSet.has(dep)) indegree.set(t, indegree.get(t) + 1);
      }
    }
    const ready = targets.filter((t) => indegree.get(t) === 0);
    const order = [];
    while (ready.length) {
      const node = ready.shift();
      order.push(node);
      for (const t of targets) {
        if (this.services.get(t).dependsOn.includes(node)) {
          indegree.set(t, indegree.get(t) - 1);
          if (indegree.get(t) === 0) ready.push(t);
        }
      }
    }
    if (order.length !== targets.length) {
      throw new Error('dependency cycle detected among target services');
    }
    return order;
  }

  /**
   * Start a publish. Returns a promise resolving to the publish result:
   *   { status: 'committed', version, visibleVersions }
   *   { status: 'aborted',  version, reason, visibleVersions }
   */
  publish(version, config, { targets } = {}) {
    if (this.currentPublish) {
      return Promise.reject(
        new Error(`publish of ${this.currentPublish.version} still in progress`)
      );
    }
    const targetList = targets ?? [...this.services.keys()];
    for (const t of targetList) {
      if (!this.services.has(t)) throw new Error(`unknown service: ${t}`);
    }
    if (this.committed.has(version)) {
      return Promise.reject(new Error(`version ${version} already committed`));
    }

    const order = this._topoOrder(targetList);
    const record = {
      version,
      config,
      targets: targetList,
      order,
      state: 'preparing',
      pending: new Set(order),
      acked: new Set(),
      resolve: null,
      timer: null,
    };
    const promise = new Promise((resolve) => {
      record.resolve = resolve;
    });
    record.timer = setTimeout(() => {
      this._abort(record, 'ack-timeout', [...record.pending]);
    }, this.ackTimeoutMs);

    this.currentPublish = record;
    this._log('publish-started', { version, order });
    return promise;
  }

  /**
   * Service confirmation. `ok=false` is a nack and aborts the publish.
   * Duplicate acks and acks for stale/unknown versions are ignored.
   */
  ack(service, version, { ok = true, reason = 'nack' } = {}) {
    const record = this.currentPublish;
    if (!record || record.version !== version || record.state !== 'preparing') {
      this._log('ack-ignored-stale', { service, version });
      return { accepted: false, reason: 'stale-or-unknown-version' };
    }
    if (record.acked.has(service)) {
      this._log('ack-ignored-duplicate', { service, version });
      return { accepted: false, reason: 'duplicate' };
    }
    if (!record.pending.has(service)) {
      this._log('ack-ignored-unknown', { service, version });
      return { accepted: false, reason: 'not-a-target' };
    }
    if (!ok) {
      this._log('ack-nack', { service, version, reason });
      this._abort(record, `nack:${service}:${reason}`);
      return { accepted: true, reason: 'nack-recorded' };
    }
    record.pending.delete(service);
    record.acked.add(service);
    this._log('ack', { service, version });
    if (record.pending.size === 0) this._commit(record);
    return { accepted: true };
  }

  /** Cancel an in-flight publish before the barrier. No-op afterwards. */
  cancel(version) {
    const record = this.currentPublish;
    if (record && record.version === version && record.state === 'preparing') {
      this._abort(record, 'cancelled');
      return true;
    }
    return false;
  }

  _commit(record) {
    clearTimeout(record.timer);
    record.state = 'committed';
    this.currentPublish = null;
    this.committed.set(record.version, record.config);
    this.history.push(record.version);
    this.activeVersion = record.version;
    // Atomic activation: every target flips in one synchronous step.
    for (const service of record.targets) this.visible.set(service, record.version);
    this._log('committed', { version: record.version });
    record.resolve({
      status: 'committed',
      version: record.version,
      visibleVersions: this.visibleVersions(),
    });
  }

  _abort(record, reason, pending = []) {
    if (record.state !== 'preparing') return;
    clearTimeout(record.timer);
    record.state = 'aborted';
    this.currentPublish = null;
    // Staged config is discarded; visible versions are untouched.
    this._log('aborted', { version: record.version, reason, pending });
    record.resolve({
      status: 'aborted',
      version: record.version,
      reason,
      pendingServices: pending,
      visibleVersions: this.visibleVersions(),
    });
  }

  /**
   * Roll back to the previous fully-committed version. Aborts any in-flight
   * publish first. The flip is atomic: readers see either the old or the
   * previous version for every service, never a mix.
   */
  rollback() {
    if (this.currentPublish && this.currentPublish.state === 'preparing') {
      this._abort(this.currentPublish, 'superseded-by-rollback');
    }
    if (this.history.length < 2) {
      return { status: 'noop', reason: 'no-previous-version', activeVersion: this.activeVersion };
    }
    const from = this.history.pop();
    const to = this.history[this.history.length - 1];
    this.activeVersion = to;
    for (const service of this.services.keys()) this.visible.set(service, to);
    this._log('rolled-back', { from, to });
    return { status: 'rolled-back', from, to, visibleVersions: this.visibleVersions() };
  }

  /** Consistent read: a service always sees exactly one complete version. */
  visibleVersion(service) {
    if (!this.services.has(service)) throw new Error(`unknown service: ${service}`);
    return this.visible.get(service) ?? null;
  }

  readConfig(service) {
    const version = this.visibleVersion(service);
    return { version, config: version === null ? null : this.committed.get(version) };
  }

  visibleVersions() {
    const out = {};
    for (const service of this.services.keys()) {
      out[service] = this.visible.get(service) ?? null;
    }
    return out;
  }
}
