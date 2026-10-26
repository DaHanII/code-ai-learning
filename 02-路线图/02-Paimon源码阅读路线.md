# Paimon 源码阅读指南（第 10–14 周）

> 专题资料：文中原有周次与大型练习属于旧版排期；当前按 [27 周学习目标参考版](../02-路线图/00-总路线图.md) 选读，不叠加任务。

> 统一排期见 [总学习计划](00-总路线图.md)。将前面的订单、支付、退款和维度模型迁移至湖表，复用 SQL 基线验证读写与更新。

> 仓库：apache/paimon（Java）。锁定 `release-1.4.2`（commit `ac223f4`，见 `00-版本锁定.md`），先把版本号写死在笔记顶部。
> 与 Flink 1.20.5 搭配使用 `paimon-flink-1.20` 模块；Paimon 的代码结构非常规整：`paimon-core` 是主体，`paimon-flink` / `paimon-spark` 是引擎适配。

## 为什么第二个读它

你的 Flink 作业在写 Paimon，问题大概率集中在：写入慢、小文件多、snapshot 膨胀、compaction 不及时、读得慢。这些问题的答案全在 `paimon-core` 里。而且它的数据结构清晰，是读 Flink 之前最好的热身——Paimon 的 snapshot 和 Flink 的 checkpoint 本质是同一类「分布式一致性快照」问题。

## 前置知识

- LSM Tree 的基本概念（memtable / sstable / compaction / 布隆过滤器）。不懂的话先花 2 小时补，否则读 `KeyValueFileStore` 会很痛苦。
- 列式文件格式的常识（ORC/Parquet 的 row group、stripe）。
- 知道 Flink 的 checkpoint 是什么（不用懂实现）。

## Stage 1：概念模型（第 10 周）

**这一周不读实现，只读概念。** 读官方文档的 Concepts 章节 + `paimon-core` 里的模型类。

要建立的层次感：

```
Table（表定义 + Schema）
  └── Snapshot（某一时刻的完整视图，带 id 和 schema）
        ├── ManifestList（清单的清单）
        │     └── ManifestFile（一批数据文件的元信息）
        │           └── ManifestEntry（单个文件的增/删记录）
        └── DataFile（实际数据，ORC/Parquet）
```

配套要搞懂的概念：分区（partition）、桶（bucket）、主键（primary key）、LSM 的 level、sequence number、文件类型（data / changelog / index）。

用 P1 把 `paimon-core` 的包结构画出来。用 P3 问：「一次写入从开始到产生新 snapshot，元数据是怎么演进的？」

验收：能在纸上画出「表 → snapshot → manifest → data file」的关系，并解释为什么要三层（为什么不把文件列表直接放在 snapshot 里）。

## Stage 2：写链路（第 11 周）

主链路（Flink 写入一张主键表）：

```
Flink Sink → TableWrite → BatchWrite → 写文件 + 收集元数据
            → TableCommit → 生成 Manifest → 生成 Snapshot → 原子提交
```

按顺序读：

1. `FileStoreTable` / `KeyValueFileStore`：表的抽象，决定用哪种 store（append-only vs key-value）。
2. `TableWrite` 与 `BatchWrite`：一次 checkpoint 对应一个 batch；内存里怎么攒数据（`SortBuffer`）、什么时候 flush 成文件。
3. `MergeFunction`：主键表的核心。`Deduplicate` / `PartialUpdate` / `Aggregation` 分别怎么合并同 key 的多条记录。
4. `TableCommit`：最精彩的部分。多个 writer 的元数据怎么合并、怎么保证提交原子性（靠文件系统的 rename？靠锁？）、冲突怎么重试。
5. `Snapshot` 的生成与清理：snapshot 过期策略、旧文件何时删。

重点问题：
- 为什么 commit 要两阶段（prepare/commit）？失败在中间会怎样？
- 多个并发 writer 提交时，谁负责合并？乐观锁怎么做的？
- bucket 数量怎么影响写入并行度和读放大？
- `bucket = -1`（dynamic bucket）和固定 bucket 的差别在哪？

## Stage 3：读链路（第 12 周）

```
TableScan → FileStoreScan（按 snapshot + 分区/过滤条件筛 manifest）
          → 生成 DataSplit（可能多个文件需要合并）
          → MergeTreeSplit / MergeOnRead（主键表要合并多版本）
          → 返回给引擎（Flink Source / Spark Scan）
```

重点：
- 谓词下推在哪一层做（manifest 级别的分区裁剪、文件级别的 min/max 统计、行级别的 ORC/Parquet 索引）。
- 主键表「读时合并」的代价：读 10 个文件只为拿到最新版本，这就是为什么要 compaction。
- `MergeTreeSplit` 怎么决定哪些文件一起读（level 的概念在这里落地）。

验收：能解释「查一条主键最新值」为什么可能读多个文件，以及什么情况下会退化成全表扫。

## Stage 4：专题（第 13 周，按工作问题挑 1 个，其余选读）

- **Compaction**：谁触发（Flink 里的 compact 算子 / 独立 compact 作业 / 写时触发）、合并策略（minor/major、full compaction）、和 snapshot 的关系、为什么 compaction 会拖慢写入。
- **Changelog Producer**：`none` / `input` / `lookup` / `full-compaction` 四种模式的实现差异，对流读的影响。**这是你们做实时链路时最容易踩坑的地方。**
- **Lookup**：点查是怎么做的（本地缓存 + 远程读），和 Flink lookup join 的配合。
- **小文件问题**：什么配置控制文件大小、为什么会有小文件、怎么合并。
- **Z-order / 排序**：多维聚簇的实现，读 `ZOrderUDF` 一类的东西。
- **Spark 集成**：`paimon-spark` 的 Scan/Write 怎么复用 core，DataSourceV2 的接口在哪实现。

## Stage 5：回填工作（第 14 周）

写一篇《我们的 Paimon 表该怎么建》实践笔记，包含：分区键/主键/桶数的选择依据（引用代码里 bucket 影响读放大的地方）、compaction 配置建议、changelog 模式选择、监控指标清单。这篇笔记要能直接给团队用。

## 关键类速查表

| 关注点 | 类 | 模块 |
| --- | --- | --- |
| 表抽象 | `FileStoreTable`、`AbstractFileStoreTable` | paimon-core |
| 主键表 | `KeyValueFileStore`、`KeyValueTableWrite` | paimon-core |
| 追加表 | `AppendOnlyFileStore` | paimon-core |
| 写入批次 | `BatchWrite` 及其实现 | paimon-core |
| 提交 | `TableCommit`、`FileStoreCommit` | paimon-core |
| 元数据 | `Snapshot`、`ManifestFile`、`ManifestEntry`、`DataFileMeta` | paimon-core |
| 扫描 | `FileStoreScan`、`DataSplit`、`MergeTreeSplit` | paimon-core |
| 合并函数 | `MergeFunction` 及 `Deduplicate` 等 | paimon-core |
| Flink 适配 | `FlinkSink`、`FlinkSource`、`FlinkCatalog`、compact 相关算子 | paimon-flink |
| Spark 适配 | Spark Scan/Write | paimon-spark |

> 类名以你 checkout 的版本为准，Stage 1 核对后修正。

## AI 提示词（针对 Paimon）

- P2：`从 org.apache.paimon.table.sink.TableWrite 的 write 方法开始，追踪「Flink 一次 checkpoint 内的一批数据」如何变成一个新的 snapshot。`
- P3：`解释 Paimon 的 commit 流程：prepare 和 commit 各做什么，如何保证原子性，并发冲突怎么处理。`
- P4：`对比 Paimon 的 snapshot 和 Iceberg 的 snapshot 在元数据组织上的差异。`
- P8：`设计实验：本地写一张主键表，连续写 3 批，每批后打印 snapshot 列表和 manifest 内容，验证元数据演进。`

## 验收问题（能答 80% 算过）

1. 表 → snapshot → manifest → data file 的层次关系，为什么这么设计？
2. 一次 Flink checkpoint 的数据在 Paimon 里经历了什么？
3. 多个 writer 并发提交怎么保证不丢数据、不重复？
4. 主键表的读为什么要合并多个文件？compaction 如何缓解？
5. 四种 changelog 模式的差别，分别适用什么场景？
6. 小文件是怎么产生的？哪几个配置能控制？
7. 谓词下推发生在哪几层？
8. 如果线上出现「snapshot 数量暴涨」，按代码路径该怎么排查？

## 常见坑

- 不要一开始就读 `paimon-flink`，先读 `paimon-core` 的模型和写入，否则会被 Flink 的算子代码带偏。
- `ManifestEntry` 有 ADD/DELETE 两种，读元数据演进时一定要注意「删除」是怎么表达的——这是理解 snapshot 隔离性的关键。
- Paimon 版本迭代快，类名和方法签名变化大，AI 训练数据里的可能是 0.5/0.8 的版本，务必以你的 tag 为准。
