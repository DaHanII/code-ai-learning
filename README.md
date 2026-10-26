# AI 辅助读源码：AI Agent 学习

主线是学习 AI Agent 的调用链、状态、上下文、工具、评估与恢复。现有 `data-employee` 项目用于验证这些机制，LangChain/LangGraph 和 Datus 用于补充与对照。Themis 语义层只提供了解 Ossie 的具体场景；Paimon、Flink、Spark 不纳入当前 AI 主线。

**从 [AI Agent 学习主线](02-路线图/00-总路线图.md) 开始。** 原 22 周排期已取消；建议每周额外 2–3 小时，现有开发和排障能证明同一学习目标时直接计入。

## 第一次怎么做

选择 `D:\hy\project\data\data-employee` 中一个现成的演示或脱敏问题，从执行记录找出“提问 → 模型工具调用 → ToolMessage → 最终回答”；对照 `investigation/agent.py` 和 `investigation/registry.py` 画一页调用链。具体步骤与完成标准见总计划。无需重写框架或额外搭 Agent demo。

## 资料入口

| 文件 | 用途 |
| --- | --- |
| [AI Agent 学习主线](02-路线图/00-总路线图.md) | 六张学习卡、前两次行动和 Ossie 支线边界 |
| [问题清单](03-问题清单/工作问题backlog.md) / [学习记录](04-笔记/README.md) | 记录 AI 机制疑问及验证结果 |
| [LangChain/LangGraph](02-路线图/00-前置-LangChain-LangGraph巩固路线.md) | 碰到框架机制时选读 |
| [Datus](02-路线图/01-Datus源码阅读路线.md) | 对照一个数据 Agent 的主链路 |
| [数据建模与 Ossie 实践指南](02-路线图/05-数据建模与Ossie实践指南.md) / [知识框架](01-方法论/数据建模与语义建模知识框架.md) | Ossie 支线的参考；不按旧周次执行 |
| [AI 辅助读源码方法论](01-方法论/AI辅助读源码方法论.md) / [提示词库](01-方法论/提示词库.md) | 定位、核查、复述代码 |
| [版本锁定](02-路线图/00-版本锁定.md) | 研究上游源码时参考，先核对工作项目的实际版本 |
| [Paimon](02-路线图/02-Paimon源码阅读路线.md)、[Flink](02-路线图/03-Flink源码阅读路线.md)、[Spark](02-路线图/04-Spark源码阅读路线.md) | 数据系统专题资料，当前不排期 |

旧资料中的“第 N 周”和大作业是历史练习，不是当前任务。上游环境、缓存和运行记录默认放在 `D:\hy\codex\ai-source-learning\unified-study`，最终学习记录留在本目录。
