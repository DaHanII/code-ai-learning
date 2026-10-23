# LangChain / LangGraph 巩固提高路线（3 周）

> 定位：不是把 `Langchain1.0-Langgraph1.0-Learning-main` 的教程再刷一遍，而是补上「基础」和「提高」之间的三段差距——抽象层、运行时、工程化。
> 这一阶段同时是读 Datus 的热身：你在这里读懂了 LangGraph 的运行时，读 Datus 时才有对照物。

## 先诊断：基础薄弱通常缺的是这三样

1. **抽象层**：会用 `prompt | model | parser`，但说不清 `Runnable` 是怎么把任意函数变成可组合管道的；会用 `bind_tools`，但不知道工具描述是怎么序列化进请求的。
2. **运行时**：会画 `StateGraph`，但说不清「一步」到底怎么执行、状态怎么合并、checkpointer 什么时候写盘。
3. **工程化**：能跑通 demo，但没做过评估、重试、流式、人在回路、可观测性这些「上生产」的环节。

对症下药，每周补一样。判断标准不是「看完了」，而是**能徒手写出来**。

## Stage 1（第 1 周）：LangChain 抽象层

**主动回忆练习**（每天 1 小时，不查文档）：把已有的 phase1/phase2 练习挑 6 个，关掉仓库、关掉文档，从空白文件重写。写不出来的地方就是漏洞。

**源码精读**（挑 3 个文件，不求读全）：

- `langchain_core/runnables/base.py`：`Runnable`、`RunnableSequence`、`RunnableParallel`、`RunnableLambda`。搞清楚 `invoke / batch / stream / astream_events` 四套方法是怎么统一的，为什么管道能自动支持流式。
- `langchain_core/language_models/chat_models.py`：`BaseChatModel` 的调用路径，`bind_tools` 把工具塞到了请求的哪个字段。
- `langchain_core/tools/base.py`：`@tool` 装饰器做了什么，参数 schema 从哪来，工具调用结果怎么回填成 `ToolMessage`。

**产出**：
- 一个 300 行左右的 `mini_chain.py`，实现 `Runnable` 风格的 `|` 组合、`invoke`、`stream`，能跑一个「prompt → model → 解析」的链路。
- 笔记《Runnable 是怎么把函数变成可组合管道的》。

## Stage 2（第 2 周）：LangGraph 运行时

这是最重要的一周。LangGraph 1.0 是 LangChain 1.0 的运行时底座，读懂它，`create_agent` 和 middleware 就都是「上面的糖」。

**按这条线读**：

```
StateGraph（图定义）
  → Pregel（超步执行模型：plan → execute → update）
  → Channel / Reducer（状态怎么合并，为什么默认是覆盖、什么时候要 add_messages）
  → Checkpointer（每个超步后写什么、存在哪、如何 resume）
  → interrupt / Command（人在回路怎么暂停和恢复）
  → Subgraph / Send（子图和动态并行）
```

**源码入口**（版本以你安装的为准）：`langgraph/graph/state.py`、`langgraph/pregel/`（`loop.py` 是核心）、`langgraph/checkpoint/`。

**动手**：
- 用 LangSmith 跑一次多轮对话 + 一次 interrupt，把 trace 里每个 span 和源码里的调用对上。**trace 是最好的源码地图**。
- 写一个 200 行的 `mini_graph.py`：支持节点、边、条件边、状态 reducer、内存 checkpointer。

**产出**：
- 笔记《LangGraph 的一步是怎么执行的》（超步、并发、状态合并、checkpoint 时机）。
- 一张对比表：`create_agent` / 手写 StateGraph / 你的 mini_graph，三者在「循环、工具调用、中断、恢复」上分别怎么实现。

## Stage 3（第 3 周）：工程化 + 综合

从 `phase4_projects` 里挑一个（建议 multi-agent 或 rag 那个），**重做一遍，但按生产标准**：

1. **可观测**：全链路接 LangSmith，能看出每次模型调用的 token、耗时、工具入参出参。
2. **健壮性**：模型超时重试、工具异常兜底、结构化输出解析失败的重试与降级。
3. **评估**：写 10 条固定测试用例（输入 → 期望行为），每次改动跑一遍，记录通过率。
4. **流式与中断**：前端能逐字显示，能在关键步骤暂停等人确认。
5. **成本与延迟**：统计一次请求的 token 和耗时，找出最贵的一步并优化（换小模型、裁剪上下文、缓存）。

**产出**：
- 一个能演示的项目 + 一份《这个 Agent 上生产还差什么》清单。
- 一篇总结《我从「会用」到「会造」补上的三件事》。

## 和 Datus 的衔接

第 3 周末做一次 30 分钟的预演：打开 Datus 的 README 和目录结构，只回答一个问题——「如果让我用 LangGraph 实现它，我会怎么画图？」把这张图存下来。三周后读完 Datus，再回来对照，差异就是你要学的东西。

## 验收问题（能答 80% 才算巩固完成）

1. `Runnable` 的 `invoke` 和 `stream` 在基类里是什么关系？为什么自定义组件只实现一个就能两个都能用？
2. `bind_tools` 之后，工具描述出现在请求的哪个位置？模型返回的工具调用长什么样？
3. LangGraph 的「超步」是什么？同一超步里的多个节点是并发还是串行？状态冲突怎么解决？
4. reducer 什么时候被调用？`add_messages` 相比默认覆盖解决了什么问题？
5. checkpointer 在哪个时刻写入？恢复时从哪个点继续？中断恢复和失败重试走的是同一条路吗？
6. `interrupt` 暂停后，进程重启还能恢复吗？靠什么？
7. 你的 mini_graph 和 LangGraph 最大的差距在哪三处？
8. 一个 Agent 上生产，除了功能，还必须有哪四样东西？

## 时间不够时的压缩方案

只有 2 周：砍掉 Stage 3 的评估和成本优化，只保留可观测 + 健壮性；Stage 1 的主动回忆减到 3 个练习。
只有 1 周：只做 Stage 2，并且只读 Pregel 循环 + checkpointer 两块，mini_graph 只实现节点、边、内存 checkpointer。

## 常见坑

- **不要重刷教程**。看视频和抄示例会产生「会了」的错觉，主动回忆才会暴露真实水平。
- **不要试图读完 langchain-core**。它是巨型库，只读上面列的三个文件，其余用到再看。
- **不要跳过 trace**。LangGraph 是异步、批量、并发的，光看代码很难建立时序感，trace 能直接给你真实执行顺序。
- LangChain 1.0 的 middleware 是新东西，网上资料少，读源码时以 `langchain/agents/middleware/` 为准。
