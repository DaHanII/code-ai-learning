# Flink 源码阅读路线（5-6 周）

> 仓库：apache/flink（Java）。读之前先确定你的目标版本（生产用的 1.18/1.20，或 2.x），不同版本 API 差异不小，尤其 `SinkV2`、`StreamTask`、Table 层。
> **只读三个模块**：`flink-runtime`、`flink-streaming-java`、`flink-table`。其余（connectors、kubernetes、python、ml）一概不看。

## 为什么第三个读

Flink 是这四个里最大的坑，也是最容易半途而废的。放在 Paimon 之后读，是因为你已经理解「快照」这套语义，Flink 的 checkpoint 会顺很多；而做完前两个项目，你的阅读方法也已经成熟。

## 先建立一个心法：永远先问「这段代码跑在哪个进程、哪个线程」

Flink 的代码难，难在跨进程、跨线程的状态。读每个方法前先定位：

- JobManager 侧：`Dispatcher`、`JobMaster`、`Scheduler`、`CheckpointCoordinator`
- TaskManager 侧：`TaskExecutor`、`Task`、`StreamTask`、算子线程、IO 线程、网络线程
- 客户端侧：`CliFrontend`、`StreamGraph` 生成

笔记里每个方法都标上「JM / TM / Client」，链路表里加一列「线程」。这一列比任何注释都有用。

## Stage 1：作业提交链路（第 1-2 周）

从 `flink-examples` 的 WordCount 出发，追一条链：

```
CliFrontend.main
  → parseAndRun → PackagedProgram(用户 main)
  → StreamExecutionEnvironment.execute
  → StreamGraphGenerator → StreamGraph        （逻辑图：算子 + 边）
  → StreamingJobGraphGenerator → JobGraph    （物理图：算子链、分区）
  → RestClusterClient.submitJob
  → Dispatcher → JobManagerRunner → JobMaster
  → DefaultExecutionGraphBuilder → ExecutionGraph（并行度、中间结果）
  → DefaultScheduler → SlotPool 申请 slot
  → TaskDeploymentDescriptor → TaskExecutor
  → Task.run → StreamTask.invoke
```

每一步要回答：**输入是什么对象，输出是什么对象，图上发生了什么变化**。四种图（StreamGraph / JobGraph / ExecutionGraph / 物理执行）的区别是这一阶段的核心收获。

配套要读的：
- 算子链（operator chain）是怎么形成的（`StreamingJobGraphGenerator` 里的 chain 条件），这直接解释了「为什么上游并行了下游也并行」和「为什么有时不能 chain」。
- Slot / SlotSharingGroup / 资源申请，解释了「为什么 slot 不够作业起不来」。
- 作业失败后的重启：`RestartStrategy`、`DefaultScheduler` 的 failover 逻辑。

验收：能画出四种图的转换关系，并解释每一步转换解决了什么问题。

## Stage 2：任务执行与数据交换（第 3 周）

```
Task.run → StreamTask.invoke
  → 初始化 OperatorChain（StreamOperator 按链顺序 head/tail）
  → StreamTask 主循环：处理输入 → 调用算子 → 输出
  → StreamOperator.processElement → 用户函数
  → RecordWriter → ResultPartition → ResultSubpartition → Netty → InputGate → RecordReader
```

重点：
- `StreamTask` 的 mailbox 模型（Flink 1.13+ 之后所有事件都走 mailbox），解释了 checkpoint、watermark、数据记录为什么能在同一个线程里有序处理——**这是理解后面所有机制的前提**。
- 网络栈：`ResultPartition` / `InputGate` / `BufferPool`，基于 credit 的反压（`BufferDebloater`），解释了「反压」到底反在哪、为什么看 `outPoolUsage` / `inPoolUsage`。
- 内存管理：`MemorySegment`、`MemoryManager`、`NetworkBufferPool`，解释了「网络缓冲不够」和「直接内存 OOM」。

验收：能解释一条记录从上游算子到下游算子经过了哪些缓冲区和线程，以及反压如何逐级传导。

## Stage 3：Checkpoint 全链路（第 4 周，核心中的核心）

这是和 Paimon 对接最紧的部分，也是线上问题最多的部分。

```
CheckpointCoordinator.triggerCheckpoint（JM 定时器触发）
  → 向 Source 下发 CheckpointBarrier
  → Source 注入 barrier → 随数据流向下游
  → StreamTask 收到 barrier → CheckpointBarrierHandler
      （对齐：等所有输入都到 barrier；非对齐：先落盘再对齐）
  → SubtaskCheckpointCoordinator → snapshotState（算子 + keyed state + timer）
  → 状态写入 CheckpointStorage（JM 内存 / 文件系统）
  → 各 subtask 上报 ack → CheckpointCoordinator 收到全部 ack
  → 完成：CompletedCheckpointStore 记录，通知算子（两阶段提交的 commit）
```

重点问题：
- 精确一次是怎么保证的？barrier 对齐到底对齐了什么？
- 非对齐 checkpoint（unaligned）解决什么问题，代价是什么？
- 状态大的时候 checkpoint 慢在哪：快照（RocksDB 增量）/ 上传 / 对齐？
- 两阶段提交：`CheckpointListener` 的 notifyCheckpointComplete 在什么时候调用，Sink 如何借此实现 exactly-once（这和 Paimon 的 commit 是同一套思路，对照读）。
- 失败恢复：从哪个 checkpoint 恢复、状态如何重新分配（rescale）。

验收：能画一张完整的 checkpoint 时序图（JM 和 TM 两个泳道），并能解释「checkpoint 一直超时」的五种可能原因分别对应代码哪一段。

## Stage 4：按工作问题二选一深挖（第 5 周）

**方向 A：状态与性能**（如果你们状态大、作业不稳）
- `StateBackend` / `KeyedStateBackend`：Heap vs RocksDB，状态如何组织（key group、namespace）。
- 状态 TTL、增量 checkpoint、本地恢复（local recovery）。
- 数据倾斜：keyBy 之后的分布，`rescale` vs `rebalance` vs `forward`。

**方向 B：Table / SQL 层**（如果你们用 Flink SQL 多）
- `TableEnvironmentImpl` → `SqlPlanner`/`StreamPlanner` → `StreamPhysicalRel` → `StreamExecNode` → `Transformation`。
- 一条 `INSERT INTO paimon_table SELECT ...` 怎么变成 JobGraph。
- 动态表、changelog 语义（+I/-U/+U/-D）在算子间怎么流转——和 Paimon 的 changelog producer 对照。
- 维表 join / lookup join 的实现。

**方向 C（可选）：时间与窗口**
- `WatermarkGenerator`、`TimerService`、`WindowOperator`，迟到数据的处理。工作中如果用事件时间，这块必读。

## Stage 5：输出（第 6 周）

写两篇：
1. 《一个 Flink SQL 作业的一生》：从 SQL 文本到 TaskManager 上跑起来，全链路。
2. 《我们的作业为什么 checkpoint 慢 / 反压 / 重启》：把工作里的真实案例按代码路径复盘。

## 关键类速查表

| 关注点 | 类 | 模块 |
| --- | --- | --- |
| 提交入口 | `CliFrontend`、`StreamExecutionEnvironment` | flink-clients / streaming-java |
| 图生成 | `StreamGraphGenerator`、`StreamingJobGraphGenerator` | streaming-java |
| 调度 | `Dispatcher`、`JobMaster`、`DefaultScheduler`、`SlotPool` | flink-runtime |
| 图执行 | `ExecutionGraph`、`Execution`、`ExecutionVertex` | flink-runtime |
| 任务执行 | `Task`、`StreamTask`、`OperatorChain` | flink-runtime / streaming-java |
| 网络 | `ResultPartition`、`InputGate`、`BufferPool`、`RecordWriter` | flink-runtime |
| Checkpoint | `CheckpointCoordinator`、`CheckpointBarrierHandler`、`SubtaskCheckpointCoordinator` | flink-runtime |
| 状态 | `StateBackend`、`KeyedStateBackend`、`RocksDBKeyedStateBackend` | flink-runtime / state-backends |
| Table | `TableEnvironmentImpl`、`StreamPlanner`、`StreamExecNode` | flink-table |

> 版本差异提示：1.20 与 2.0 在 Sink、StreamTask、Table planner 上都有调整，类名以你的版本为准。

## AI 提示词（针对 Flink）

- P2：`从 StreamExecutionEnvironment.execute 开始，追踪一个 WordCount 作业到 TaskManager 上第一个算子开始处理数据，标注每一步运行在 Client / JM / TM 哪个进程。`
- P3：`解释 barrier 对齐：一个有两路输入的算子，收到一路 barrier 后到另一路 barrier 到来之前，做了什么？数据被缓存到哪？`
- P4：`对比 Flink 的 checkpoint 和 Paimon 的 snapshot commit，在「谁协调、如何保证原子、失败如何回滚」上的异同。`
- P8：`设计实验：本地起一个两并行度的作业，人为让一个 subtask 变慢，观察反压指标如何变化，并指出对应代码。`

## 验收问题（能答 80% 算过）

1. StreamGraph / JobGraph / ExecutionGraph 分别是什么，为什么要分三层？
2. 算子链是怎么形成的？什么情况下不会 chain？
3. 一个 slot 里能跑哪些 task？slot 不够时作业会怎样？
4. StreamTask 的 mailbox 模型解决了什么问题？
5. barrier 对齐的完整过程，非对齐模式的区别？
6. checkpoint 完成后的通知链路，两阶段提交如何工作？
7. 反压的指标怎么看，逐级传导的机制是什么？
8. 作业失败后从哪里恢复？状态如何重新分配？
9. 一条 Flink SQL 怎么变成 JobGraph？
10. 「checkpoint 一直失败」的排查顺序是什么（按代码路径）？

## 常见坑

- 直接读 `flink-runtime` 的网络栈或 `flink-table` 的 planner，会崩。必须按 Stage 顺序。
- 忽略「线程」维度。Flink 大量逻辑是异步、回调、mailbox 驱动的，不标线程会读成一团。
- 1.13 之前的博客和源码结构差异巨大（尤其是 StreamTask 和 Source），别拿老资料对照新代码。
- Table 层的类名几经重构（`ExecNode` → `StreamExecNode`，planner 拆分），AI 很容易给出旧版本类名，务必核对。
