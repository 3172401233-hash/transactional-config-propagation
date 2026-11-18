# transactional-config-propagation

Node.js 配置传播协调器：以**版本屏障 + 两阶段（prepare → commit）**的方式把配置版本
事务化地发布到一组服务，杜绝"部分实例切新版本、部分实例还在旧版本"的裂脑窗口。

## 运行

```bash
node --test          # 运行全部测试
npm run demo         # 打印每次发布后每个服务的 visible version
```

## 协议

对每次 `publish(version, config, { targets })`：

1. **PREPARING**：按依赖 DAG 的拓扑序（依赖先确认）要求所有目标服务把候选版本
   加载到 *staging* 并 `ack(service, version)`。此阶段任何服务的
   **visible version 不变**。
2. **BARRIER**：当且仅当**所有**目标服务都确认后，才到达版本屏障。
3. **COMMITTED**：激活是一次同步的原子翻转——所有目标服务在同一时刻切到新版本，
   不存在部分切换的中间态。
4. **ABORTED**：任何一个服务 nack、确认超时（`ackTimeoutMs`）或 `cancel(version)`，
   都会中止本次发布：staging 被丢弃，所有服务保持在之前的完整版本。

`rollback()` 把**所有**服务原子地切回上一个**已完整提交**的版本
（`history` 中前一个版本），并会先中止任何进行中的发布。

## 一致性保证

- **无部分切换**：激活与回滚都是单个同步步骤；任意时刻做一次一致性读
  （`visibleVersions()`），所有服务要么全在新版本、要么全在旧版本，绝不混合。
- **屏障前零可见性**：确认阶段只写 staging，消费者读不到未提交的版本。
- **幂等确认**：重复 ack 被忽略；已中止/已提交版本的迟到 ack（慢实例、旧版本迟到）
  一律视为 stale 并丢弃，不会污染进行中的新发布。
- **依赖顺序**：目标集合内按拓扑序 prepare；依赖环直接报错拒绝发布。
- **单发布**：同一时间只允许一个进行中的发布，并发 `publish` 被拒绝。

## 不可回滚的步骤（重要）

回滚只是**版本指针**的原子翻转，以下步骤一旦发生就**无法**通过 `rollback()` 撤销：

- **激活后的下游写入**：新版本一旦 COMMITTED，依赖方可能已基于新字段
  （如 `newField`）写入数据。回滚切回旧版本后，这些已写入的数据仍在，
  旧代码必须能容忍/忽略它们（前向兼容），否则需要带外的数据修复。
- **已提交的版本记录**：`committed` 中的版本与配置不会被回滚删除，
  只能再向前发布一个修复版本。
- **外部副作用**：新版本生效期间发出的 RPC、消息、缓存失效等外部效应不可回收。

因此兼容策略应是：**新字段先做到可忽略（消费者可选读），再发布写入方**；
回滚只保证"配置视图"一致，不保证"数据视图"回退。

## API 摘要

| 方法 | 说明 |
| --- | --- |
| `registerService(name, { dependsOn })` | 注册服务及其依赖（DAG） |
| `publish(version, config, { targets })` | 发起发布，返回 Promise 解析为 `{status: 'committed' \| 'aborted', visibleVersions, ...}` |
| `ack(service, version, { ok, reason })` | 服务确认；`ok:false` 为 nack |
| `cancel(version)` | 屏障前取消发布 |
| `rollback()` | 原子切回上一个完整版本 |
| `visibleVersion(service)` / `visibleVersions()` | 查询每个服务当前可见版本 |
| `readConfig(service)` | 一致性读：返回 `{version, config}` 快照 |

## 测试覆盖（`test/coordinator.test.js`）

慢实例超时中止、重复确认幂等、旧版本迟到 ack 被忽略、依赖 DAG 拓扑序与环检测、
发布取消、回滚期间读取不出现混合版本、回滚中止进行中的发布、并发发布拒绝。
