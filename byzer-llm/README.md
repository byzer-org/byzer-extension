<H1 align="center">
LLM Extension for Byzer-SQL
</H1>

<h3 align="center">
Make LLM easy to use by SQL
</h3>

<p align="center">
| <a href="https://docs.byzer.org/#/byzer-lang/zh-cn/byzer-llm/quick-tutorial"><b>Documentation</b></a> | <a href="#"><b>Blog</b></a> | | <a href="#"><b>Discord</b></a> |

</p>

---

*Latest News* 🔥

- [2024/02] Release LLM Extension 0.1.18 for Byzer-SQL. 
- [2024/01] Release LLM Extension 0.1.17 for Byzer-SQL. 


---

This is the LLM Extension for Byzer-SQL, which is a SQL-like language for LLM. It is designed to make it easy to work with LLM by SQL.

---

* [Versions](#Versions)
* [Installation](#Installation)
* [Quick Start](#Quick-Start)
* [Contributing](#Contributing)

---

## Versions
- [0.1.8](https://download.byzer.org/byzer-extensions/nightly-build/byzer-llm-3.3_2.12-0.1.8.jar): Use python project byzer-llm as the default client for LLM.
- [0.1.7](https://download.byzer.org/byzer-extensions/nightly-build/byzer-llm-3.3_2.12-0.1.7.jar)： Support Byzer-Retrieval and Byzer-LLM 

---

## Installation

1. Download the latest version from [here](https://download.byzer.org/byzer-extensions/nightly-build/).
2. Copy the jar file to the `plugin` folder of Byzer-SQL.
3. Add the following configuration to the `conf/byzer.properties.override` file of Byzer-SQL:

```properties
streaming.plugin.clzznames=tech.mlsql.plugins.llm.LLMApp,[YOUR_OTHER_PLUGINS_LIST]
```

---

## Quick Start

todo
