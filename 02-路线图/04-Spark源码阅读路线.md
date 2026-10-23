# Spark 源码阅读路线（按需，2 周起）

> 仓库：apache/spark（Scala）。锁定 `v3.5.9`（commit `7c14a3c`，Scala 2.12 预编译包，见 `00-版本锁定.md`；Paimon 侧对应 `paimon-spark-3.5`）。
> **这条路线的定位是「按需」**：如果 Spark 在你的工作里主要是「跑批 + 读 Paimon 的另一个引擎」，读完 Stage 1、2 就够；只有要做查询优化、排查 OOM/倾斜时才往下走。

## 先定目标：你读 Spark 是为了什么

三种典型目标，读法完全不同：

- **A. 会用 + 能看懂执行计划**（大多数数据开发）：Stage 1 + Stage 2 的物理计划部分，会看 `explain` 输出即可。
- **B. 排查性能问题**（倾斜、OOM、shuffle 慢）：Stage 1 + Stage 3（AQE、shuffle、内存），配合 Stage 4 的 Paimon 集成。
- **C. 做引擎改造 / 深度定制**（扩展 Catalyst、写 connector）：全读，重点是 Stage 2 的 Rule 机制和 Stage 4 的 DataSourceV2。

先选一个，别贪。

## 前置知识

- Scala 的 case class、模式匹配、隐式转换（读 Catalyst 必需，`TreeNode` 大量用隐式）。
- 知道 logical plan / physical plan 的区别。
- 知道 shuffle 是什么、为什么贵。

## Stage 1：一条 SQL 的一生（3 天）

这是 Spark 最优雅的部分，一条链讲清楚整个引擎：

```
SQL 文本 / DataFrame API
  → Unresolved LogicalPlan（Parser / Dataset 构建）
  → Analyzer（SessionCatalog 解析表、列、函数，变成 Resolved LogicalPlan）
  → Optimizer（规则批量改写：谓词下推、列裁剪、常量折叠、join 重排）
  → SparkPlanner（Strategy 把逻辑算子转成物理算子）
  → PhysicalPlan（可选：AQE 在运行时改写）
  → SparkPlan.execute / doExecute（WholeStageCodegen 生成字节码）
  → RDD → Task → Shuffle / 读文件
```

配套概念：`TreeNode`（一切皆树）、`RuleExecutor`（一批 Rule 反复执行直到不动）、`Strategy`（逻辑到物理的映射）。

验收：给一条带 join + 聚合的 SQL，能说出 Analyzer 和 Optimizer 大致改了哪些地方，并能在 `explain(true)` 输出里指出对应的节点。

## Stage 2：Catalyst 的读法（3-4 天）

**不要试图读完所有 rule**（几百条）。读法：

1. 先读 `RuleExecutor` 和 `Batch` 的执行模型（一次 batch 内规则顺序执行，直到 fixpoint 或达到 maxIterations）。
2. 挑 5 条最常用的 rule 精读，每条按「匹配什么 → 改成什么 → 为什么安全」三段式做笔记：
   - `ResolveRelations`（表名 → 表）
   - `PushDownPredicates` / `PushPredicateThroughJoin`（谓词下推）
   - `ColumnPruning`（列裁剪）
   - `ConstantFolding`（常量折叠）
   - `ReorderJoin` 或 `CostBasedJoinReorder`（join 顺序）
3. 每条 rule 都问：它会不会不触发？什么情况下触发不了（比如 UDF 挡住了下推）？

进阶（目标 C 才需要）：`TreeNode.transformDown` / `transformUp` 的差异，自定义 rule 怎么注入（`SparkSessionExtensions`）。

## Stage 3：运行时（3-5 天，目标 B 必读）

- **Shuffle**：`ShuffleManager` → `SortShuffleWriter`（map 端排序 + spill）→ 索引文件 → `BlockStoreShuffleReader`（reduce 端拉取）。搞懂 spill 什么时候发生、`spark.shuffle.file.buffer` 之类的参数作用在哪。
- **AQE**：`AdaptiveSparkPlanExec` 在 stage 边界重新优化；`CoalesceShufflePartitions`（合并小分区）、`OptimizeSkewedJoin`（倾斜 join 拆分）、`DemoteBroadcastHashJoin`。这是解决倾斜和分区过多的主要手段。
- **内存与 Tungsten**：`UnsafeRow`（堆外二进制行）、`MemoryManager`（execution vs storage）、OOM 时的内存分布。解释了「为什么 off-heap 也会 OOM」。
- **WholeStageCodegen**：`WholeStageCodegenExec` 把多个算子编译成一个函数，解释了为什么 `explain` 里看到的节点和实际执行不对应。

## Stage 4：Paimon 集成（2-3 天，和工作直接相关）

Paimon 走 Spark 的 DataSourceV2 API，读这条链能同时学会两件事：Spark 怎么接外部存储、Paimon 怎么适配引擎。

```
SparkCatalog / SparkSession → Table(SparkTable)
  → ScanBuilder → Scan（做分区裁剪、谓词下推、列裁剪）
  → Batch → PartitionReaderFactory → PartitionReader（读 Paimon 文件）
写入：WriteBuilder → BatchWrite → Commit
```

对照读 `paimon-spark` 和 `paimon-core`：适配层做了什么、哪些能力（下推、统计信息）是 Paimon 提供、哪些是 Spark 要求实现的。**这一节读完，你对「湖格式如何服务多引擎」会有完整认知。**

## 关键类速查表

| 关注点 | 类 | 模块 |
| --- | --- | --- |
| 入口 | `SparkSession`、`Dataset`、`QueryExecution` | sql/core |
| 解析与优化 | `Analyzer`、`Optimizer`、`RuleExecutor`、`Batch` | catalyst |
| 树 | `TreeNode`、`LogicalPlan`、`Expression` | catalyst |
| 规划 | `SparkPlanner`、`Strategy`、`SparkPlan` | sql/core |
| 代码生成 | `WholeStageCodegenExec`、`CodegenContext` | sql/core |
| AQE | `AdaptiveSparkPlanExec`、`CoalesceShufflePartitions`、`OptimizeSkewedJoin` | sql/core |
| Shuffle | `ShuffleManager`、`SortShuffleWriter`、`BlockStoreShuffleReader` | core |
| 内存 | `UnsafeRow`、`MemoryManager`、`TaskMemoryManager` | core |
| 外部数据源 | `Table`、`ScanBuilder`、`Batch`、`PartitionReaderFactory` | sql/core (v2) |

## AI 提示词（针对 Spark）

- P2：`从 SparkSession.sql("<SQL>") 开始，追踪到生成 PhysicalPlan，列出经过的 Analyzer batch、Optimizer batch、Planner strategy。`
- P3：`解释 RuleExecutor 的执行模型：batch 之间和 batch 内部的顺序、fixpoint 判定、maxIterations 的作用。`
- P4：`对比 Spark 的 AQE 和 Flink 的动态调度，在「运行时改计划/改并行度」上的思路差异。`
- P8：`设计实验：构造一个 10 万行小表 join 1000 万行大表、且 key 严重倾斜的场景，观察 AQE 的 OptimizeSkewedJoin 是否生效，看日志里哪个关键字。`

## 验收问题（能答 80% 算过）

1. 一条 SQL 从文本到执行经过哪几个阶段？每个阶段的输入输出是什么？
2. `TreeNode` + `Rule` 的模式解决了什么问题？为什么规则要反复执行？
3. 谓词下推在什么情况下失效？
4. AQE 在哪些时机重新优化？解决了哪三类问题？
5. shuffle 的 map 端和 reduce 端分别做了什么？spill 何时发生？
6. WholeStageCodegen 为什么能提速？它和 `explain` 的节点是什么关系？
7. Paimon 的 Spark connector 实现了 DataSourceV2 的哪些接口？分区裁剪在哪做的？

## 常见坑

- Spark 源码是 Scala，隐式转换多，跳转经常跳到奇怪的地方。用 IDEA 时先看方法签名，别被隐式带跑。
- Catalyst 的 rule 有几百条，读多了会迷失。始终围绕「这条规则改变了哪个节点、为什么安全」。
- 网上大量 Spark 2.x 的资料（尤其是 DataSource V1），和 3.x/4.x 差异大，注意 `v2` 包。
- Spark 4.x 有较大改动（ANSI 默认开启、Variant 类型、Spark Connect），如果生产是 3.5，别读 4.x 的代码。
