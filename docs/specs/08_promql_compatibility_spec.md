# Module 8: PromQL 兼容层深度审计报告（庖丁解牛版 v3）

> 代码基线：openGemini `origin/docs/spec-md-chinese` @ `9717d662`（2026-04-11）。

> 时序图 + 核心代码逐行解释。先画图，再对照代码，一步一步拆解。

---

## 1. PromQL 兼容层概述

### 1.1 什么是 PromQL 兼容层？

PromQL（Prometheus Query Language）是 Prometheus 生态系统的核心查询语言。openGemini 作为时序数据库，为了无缝对接 Prometheus 生态（包括 Grafana、AlertManager、各类 Exporter），必须原生支持 PromQL 查询协议。

PromQL 兼容层的本质是：**将 PromQL 语法树（AST）转译为 openGemini 原生的 InfluxQL 语法树，然后利用 openGemini 既有的查询引擎执行，最终将 InfluxQL 的执行结果转换回 Prometheus 标准的 JSON/Protobuf 响应格式。**

这套方案的核心优势在于：
- **不重新实现 PromQL 引擎**：复用 openGemini 已有的 InfluxQL 查询引擎，避免维护两套查询执行器
- **原生兼容 Prometheus HTTP API**：直接暴露 `/api/v1/query`、`/api/v1/query_range` 等标准端点
- **支持远程读写协议**：兼容 Prometheus Remote Write / Remote Read 协议，可直接作为 Prometheus 的远端存储

### 1.2 三层架构

PromQL 兼容层采用三层架构设计：

| 层级 | 职责 | 关键文件 |
|------|------|----------|
| **Transpiler 层** | 将 PromQL AST 转译为 InfluxQL AST | `lib/util/lifted/promql2influxql/` |
| **Engine 层** | 在查询引擎中实现 PromQL 专属函数（rate、irate 等） | `engine/prom_*.go` |
| **HTTP Handler 层** | 处理 HTTP 请求/响应，结果格式转换 | `lib/util/lifted/influx/httpd/handler_prom.go` |

### 1.3 整体架构时序图

```mermaid
sequenceDiagram
    participant Client as Prometheus 客户端/Grafana
    participant HTTP as HTTP Handler
    participant Trans as Transpiler
    participant Engine as 查询引擎
    participant Store as 存储层

    Note over Client,Store: 查询路径：POST /api/v1/query
    Client->>HTTP: POST /api/v1/query?query=rate(http_requests_total[5m])&time=...
    HTTP->>HTTP: servePromBaseQuery()
    HTTP->>HTTP: parser.ParseExpr(query) -- 解析 PromQL
    HTTP->>Trans: Transpiler.Transpile(expr)
    Trans->>Trans: transpileExpr() 递归转译 AST
    Trans-->>HTTP: influxql.SelectStatement
    HTTP->>Engine: QueryExecutor.ExecuteQuery(q, opts)
    Engine->>Store: 读取原始数据（record/chunk）
    Store-->>Engine: 原始时间序列数据
    Engine->>Engine: InstantVectorCursor / RangeVectorCursor 采样
    Engine->>Engine: PromFunction（rate_prom 等）计算
    Engine-->>HTTP: query.Result
    HTTP->>HTTP: Receiver.InfluxResultToPromQLValue()
    HTTP-->>Client: PromQueryResponse JSON

    Note over Client,Store: 写入路径：POST /api/v1/write
    Client->>HTTP: POST /api/v1/write (snappy + protobuf)
    HTTP->>HTTP: servePromWriteBase()
    HTTP->>HTTP: Parser.Parse() 解析 Protobuf
    HTTP->>HTTP: timeSeries2Rows() 转换为 InfluxDB Row
    HTTP->>Store: PointsWriter.RetryWritePointRows()
    Store-->>HTTP: 204 No Content
```

### 1.4 核心数据流总览

```mermaid
graph LR
    A["PromQL 语法<br/>rate(http_requests_total[5m])"] --> B["PromQL Parser<br/>parser.ParseExpr()"]
    B --> C["PromQL AST<br/>parser.Call{rate, [MatrixSelector]}"]
    C --> D["Transpiler<br/>transpileExpr()"]
    D --> E["InfluxQL AST<br/>SelectStatement"]
    E --> F["查询引擎<br/>ExecuteQuery()"]
    F --> G["InfluxQL Result<br/>query.Result"]
    G --> H["Receiver<br/>InfluxResultToPromQLValue()"]
    H --> I["PromQL Response<br/>PromQueryResponse"]
```

---

## 2. Transpiler 核心结构

### 2.1 Transpiler 结构体

**代码位置**：`lib/util/lifted/promql2influxql/transpiler.go:16-28`

```go
// Transpiler 负责将单个 PromQL 表达式转译为 InfluxQL 表达式。
// 它在工作完成后会被 GC 回收。
type Transpiler struct {
    PromCommand                        // 嵌入查询命令参数（时间范围、步长、数据库等）
    timeRange          time.Duration   // 当前 MatrixSelector 的时间窗口范围（如 5m）
    parenExprCount     int             // 括号表达式计数，用于生成 InfluxQL ParenExpr
    dropMetric         bool            // 是否丢弃 __name__ 标签（聚合查询时为 true）
    duplicateResult    bool            // 是否需要复制结果（StepInvariant 表达式时为 true）
    minT, maxT         int64           // 查询的最小/最大时间戳（毫秒）
    timeCondition      influxql.Expr   // 缓存的时间条件表达式
    isStepVariantExpr  bool            // 表达式是否与步长相关
    upperSubquery      int             // 嵌套子查询的层级深度
    subStartT, subEndT int64           // 子查询的起止时间
    lowerStepInvariant bool            // 下层是否为步长无关表达式
}
```

**通俗解释**：

- **PromCommand**：嵌入的查询命令，包含了用户请求中的所有参数——查询语句（Cmd）、数据库（Database）、时间范围（Start/End/Step）、回溯窗口（LookBackDelta）等
- **timeRange**：当遇到 `http_requests_total[5m]` 这样的 MatrixSelector 时，`[5m]` 就会被存入 `timeRange`，后续用于生成 InfluxQL 的时间窗口
- **dropMetric**：这是聚合查询的关键标志。当你写 `sum(rate(...))` 时，外层的 `sum` 聚合会设置 `dropMetric = true`，意味着最终结果中不需要 `__name__` 标签（因为聚合后的指标名已无意义）
- **duplicateResult**：当表达式是步长无关的（如 `vector(1)`），值在每个时间步都一样，只需要改变时间戳即可，设置此标志后 Receiver 层会自动复制结果

### 2.2 PromCommand 模型

**代码位置**：`lib/util/lifted/promql2influxql/models.go:29-66`

```go
// PromCommand 包装了一个原始查询表达式及其相关属性
type PromCommand struct {
    Cmd             string           // 原始 PromQL 查询语句
    Database        string           // 目标数据库
    RetentionPolicy string           // 保留策略
    Measurement     string           // 度量名称（MetricStore 模式下使用）
    Exact           bool             // 是否精确查询（元数据查询用）
    Start      *time.Time            // range query 的起始时间
    End        *time.Time            // range query 的结束时间
    Timezone   *time.Location        // 时区
    Evaluation *time.Time            // instant query 的评估时间点
    Step       time.Duration         // 评估步长（如 15s、1m）

    // LookBackDelta 回溯窗口，PromQL 计算中点查找的最大回溯间隔，默认 5m
    LookBackDelta time.Duration

    DataType    DataType             // 查询数据类型（TABLE/GRAPH/LABEL_KEYS 等）
    ValueFieldKey string             // 使用的字段名（默认 "value"）
    LabelName   string               // label values 查询时的标签名
}
```

### 2.3 DataType 枚举

**代码位置**：`lib/util/lifted/promql2influxql/models.go:19-27`

```go
type DataType int

const (
    TABLE_DATA      DataType = iota // 原始表格数据查询
    GRAPH_DATA                      // 时间绑定的图形数据查询
    LABEL_KEYS_DATA                 // 标签键查询（如 /api/v1/labels）
    LABEL_VALUES_DATA               // 标签值查询（如 /api/v1/label/{name}/values）
    SERIES_DATA                     // 序列查询（如 /api/v1/series）
    META_DATA                       // 元数据查询（如 /api/v1/metadata）
    UNKNOWN_DATA                    // 未知类型
)
```

**通俗解释**：
- `TABLE_DATA`：Grafana 的 Table 面板使用，返回原始时间点
- `GRAPH_DATA`：Grafana 的 Time Series 面板使用，返回带有时间戳的矩阵数据
- `LABEL_KEYS_DATA`/`LABEL_VALUES_DATA`：Grafana 的标签选择器（变量模板、面板查询编辑器中的 label 过滤器）
- `SERIES_DATA`：返回匹配的序列集合

### 2.4 FunctionType 与常量定义

**代码位置**：`lib/util/lifted/promql2influxql/constant.go:16-29`

```go
type FunctionType int

const (
    AGGREGATE_FN  FunctionType = iota + 1 // 聚合函数（如 sum、avg）
    SELECTOR_FN                           // 选择器函数（如 max、min、topk）
    TRANSFORM_FN                          // 变换函数（如 rate、abs）
    PREDICTOR_FN                          // 预测函数（如 predict_linear）
)

const (
    ArgNameOfTimeFunc          string = "prom_time"
    DefaultFieldKey            string = "value"       // Prometheus 默认字段名
    DefaultMetricKeyLabel      string = "__name__"    // Prometheus 指标名标签
    DefaultMeasurementName     string = "prom_metric_not_specified"
    DefaultDatabaseName        string = "prom"
    DefaultRetentionPolicyName string = "autogen"
    TimeField                  string = "time"
    LastCall                   string = "last"
    PromSuffix                 string = "_prom"       // 后缀，用于区分 openGemini 原生函数与 Prom 函数
)

const DefaultLookBackDelta = 5 * time.Minute  // 默认回溯窗口
const FieldCountForKeepMetric = 2             // keepMetric 模式下的字段数
```

**通俗解释**：

- `_prom` 后缀约定：openGemini 原生的 InfluxQL 已经有 `max`、`min`、`count` 等函数，但它们的语义可能与 Prometheus 的实现不完全相同。为了避免冲突，PromQL 兼容层的函数使用 `_prom` 后缀（如 `max_prom`、`min_prom`、`count_prom`），确保两套函数系统互不干扰。
- `DefaultFieldKey = "value"`：Prometheus 中所有样本都是 `metric_name{labels} value`，字段名固定为 `value`，而 InfluxDB/InfluxQL 支持多字段，因此需要指定使用哪个字段。

---

## 3. 递归转译流程

### 3.1 入口函数 Transpile()

**代码位置**：`lib/util/lifted/promql2influxql/transpiler.go:45-59`

```go
// Transpile 将设置了时间范围的 PromQL 表达式转换为 InfluxQL 表达式。
// 生成的 InfluxQL 表达式可以被执行，结果需要使用 InfluxResultToPromQLValue()
// 转换为与原生 PromQL 执行完全等价的结果值。
// 在转译过程中，转译器递归地将 PromQL AST 翻译为等价的 InfluxQL AST。
func (t *Transpiler) Transpile(expr parser.Expr) (influxql.Node, error) {
    // 步骤1：如果不是元数据查询，先进行预处理
    if !IsMetaQuery(t.DataType) {
        s := t.newEvalStmt(expr)           // 创建评估语句，设置 Start/End/Interval
        t.rewriteMinMaxTime()              // 计算 minT 和 maxT
        // 步骤2：处理 @ 修饰符，调整 selector 的偏移量
        setOffsetForAtModifier(timeMilliseconds(s.Start), s.Expr)
        expr = s.Expr
    }
    // 步骤3：递归转译
    influxNode, err := t.transpile(expr)
    if err != nil {
        return nil, errno.NewError(errno.TranspileExprFail, err.Error())
    }
    return influxNode, nil
}
```

**通俗解释**：

`Transpile()` 是整个转译过程的入口。它做三件事：
1. **预处理**：创建 `EvalStmt`，计算查询的时间范围（minT/maxT），处理 `@` 修饰符（如 `metric @ 1234567890` 这种固定时间点查询）
2. **递归转译**：调用 `transpile()` -> `transpileExpr()` 进行递归 AST 转换
3. **返回**：返回 InfluxQL 的 AST 节点（`influxql.Node`）

### 3.2 transpileExpr() 调度表

```mermaid
graph TD
    A["transpileExpr(expr)"] --> B{"expr 类型?"}
    B -->|"*promql.AggregateExpr"| C["transpileAggregateExpr()"]
    B -->|"*promql.Call"| D["transpileCall()"]
    B -->|"*promql.BinaryExpr"| E["transpileBinaryExpr()"]
    B -->|"*promql.ParenExpr"| F["transpileParenExpr()"]
    B -->|"*promql.UnaryExpr"| M["transpileUnaryExpr()"]
    B -->|"*promql.VectorSelector"| G["transpileInstantVectorSelector()"]
    B -->|"*promql.MatrixSelector"| H["transpileRangeVectorSelector()"]
    B -->|"*promql.NumberLiteral"| I["内联构造 influxql.NumberLiteral"]
    B -->|"*promql.StringLiteral"| J["内联构造 influxql.StringLiteral"]
    B -->|"*promql.SubqueryExpr"| K["transpileSubqueryExpr()"]
    B -->|"*promql.StepInvariantExpr"| L["transpileStepInvariantExpr()"]
    B -->|"其他"| N["返回错误: 未知表达式类型"]
```

> 注意：当前 `transpileExpr()` 没有 `VectorLiteral` / `transpileVectorLiteral()` 分支。PromQL 的 `vector(1)` 按普通函数调用路径处理；如果 AST 节点不在上述 switch 分支内，会返回 `UnsupportedNodeType`。

**代码位置**：`lib/util/lifted/promql2influxql/transpiler.go:69-96`

```go
// transpileExpr 递归转译 PromQL 表达式
func (t *Transpiler) transpileExpr(expr parser.Expr) (influxql.Node, error) {
    switch e := expr.(type) {
    case *parser.ParenExpr:         // 括号表达式：(expr)
        return t.transpileParenExpr(e)
    case *parser.UnaryExpr:         // 一元表达式：-expr
        return t.transpileUnaryExpr(e)
    case *parser.NumberLiteral:     // 数字字面量：1.5, 42
        return &influxql.NumberLiteral{Val: e.Val}, nil
    case *parser.StringLiteral:     // 字符串字面量："hello"
        return &influxql.StringLiteral{Val: e.Val}, nil
    case *parser.VectorSelector:    // 即时向量选择器：http_requests_total{method="GET"}
        return t.transpileInstantVectorSelector(e)
    case *parser.MatrixSelector:    // 范围向量选择器：http_requests_total[5m]
        return t.transpileRangeVectorSelector(e)
    case *parser.AggregateExpr:     // 聚合表达式：sum by (method)(rate(...))
        return t.transpileAggregateExpr(e)
    case *parser.BinaryExpr:        // 二元表达式：a + b, a > b
        return t.transpileBinaryExpr(e)
    case *parser.Call:              // 函数调用：rate(...), histogram_quantile(...)
        return t.transpileCall(e)
    case *parser.StepInvariantExpr: // 步长无关表达式：由预处理阶段包装
        return t.transpileStepInvariantExpr(e)
    case *parser.SubqueryExpr:      // 子查询：http_requests_total[5m:1m]
        return t.transpileSubqueryExpr(e)
    default:
        return nil, errno.NewError(errno.UnsupportedNodeType, expr.String())
    }
}
```

**通俗解释**：

这是整个转译器的"调度中心"。它根据 PromQL AST 节点的类型，分发到不同的转译函数：

| PromQL 节点类型 | 示例 | 转译目标 |
|----------------|------|----------|
| `NumberLiteral` | `42` | 直接映射为 `influxql.NumberLiteral` |
| `StringLiteral` | `"hello"` | 直接映射为 `influxql.StringLiteral` |
| `VectorSelector` | `http_requests_total{method="GET"}` | 转译为 `SelectStatement`，WHERE 子句包含标签条件 |
| `MatrixSelector` | `http_requests_total[5m]` | 先设置 `timeRange`，再转译内部的 VectorSelector |
| `AggregateExpr` | `sum by (method)(...)` | 转译为带 GROUP BY 的 `SelectStatement` |
| `BinaryExpr` | `a + b` | 转译为 `BinaryExpr` 或特殊的 `BinOp` 节点 |
| `Call` | `rate(...)` | 查函数表，转译为对应的 InfluxQL 函数调用 |
| `SubqueryExpr` | `metric[5m:1m]` | 递归处理，调整时间范围和步长 |

### 3.3 具体例子：完整转译链

以 `rate(http_requests_total{method="GET"}[5m])` 为例：

```
PromQL: rate(http_requests_total{method="GET"}[5m])

步骤1: parser.ParseExpr() 生成 AST:
  Call{
    Func: rate,
    Args: [
      MatrixSelector{
        Range: 5m,
        VectorSelector: VectorSelector{
          Name: "http_requests_total",
          LabelMatchers: [method="GET"]
        }
      }
    ]
  }

步骤2: transpileExpr(Call) -> transpileCall(rate)
  a. 先转译参数：transpileExpr(MatrixSelector)
    -> transpileRangeVectorSelector: 设置 timeRange=5m, 递归调用 transpileExpr(VectorSelector)
    -> transpileInstantVectorSelector: 生成 SelectStatement
      Sources: [http_requests_total]
      Condition: time >= (minT - 5m) AND time <= maxT AND method = 'GET'
      Fields: [value]
      Dimensions: [*]
  b. 查 rangeVectorFunctions 表，找到 "rate" -> name="rate_prom"
  c. 调用 setAggregateFields 设置字段为 rate_prom(value)
  d. 设置 GROUP BY time(step) 和 NoFill

最终 InfluxQL:
  SELECT rate_prom(value) FROM http_requests_total
  WHERE time >= (start - 5m) AND time <= end AND method = 'GET'
  GROUP BY *, time(step) fill(none)
```

---

## 4. Selector 处理

### 4.1 InstantVectorSelector 转译

**代码位置**：`lib/util/lifted/promql2influxql/selector.go:250-363`

```go
// transpileInstantVectorSelector 将 PromQL VectorSelector 转译为 InfluxQL 语句
func (t *Transpiler) transpileInstantVectorSelector(v *parser.VectorSelector) (influxql.Node, error) {
    var (
        err           error
        timeCondition influxql.Expr
        tagCondition  influxql.Expr
    )
    // 步骤1：生成时间条件和标签条件
    timeCondition, tagCondition, err = t.transpileVectorSelector2ConditionExpr(v)
    if err != nil {
        return nil, errno.NewError(errno.TranspileIVSFail, err.Error())
    }
    condition := CombineConditionAnd(timeCondition, tagCondition)

    // 步骤2：根据 DataType 生成不同的 InfluxQL 语句
    switch t.DataType {
    case LABEL_KEYS_DATA:
        // 生成 SHOW TAG KEYS 语句
        showTagKeysStatement := influxql.ShowTagKeysStatement{
            Database:  t.Database,
            Condition: condition,
        }
        // ... 设置 Sources
        return &showTagKeysStatement, nil

    case LABEL_VALUES_DATA:
        // 生成 SHOW TAG VALUES 语句
        showTagValuesStatement := influxql.ShowTagValuesStatement{
            Database:   t.Database,
            Op:         influxql.EQ,
            TagKeyExpr: &influxql.StringLiteral{Val: t.LabelName},
            Condition:  condition,
        }
        // ... 设置 Sources
        return &showTagValuesStatement, nil

    case SERIES_DATA:
        // 生成 SHOW SERIES 语句
        // ...

    default:
        // 步骤3：常规查询，生成 SELECT 语句
        selectStatement := &influxql.SelectStatement{
            Condition:   condition,
            Dimensions:  []*influxql.Dimension{{Expr: &influxql.Wildcard{}}},
            IsPromQuery: true,
        }
        // 确定 measurement 来源
        if t.HaveMetricStore() {
            selectStatement.Sources = []influxql.Source{&influxql.Measurement{Name: t.Measurement}}
        } else {
            mst, err := getMeasurementBySelector(v)
            // ...
            selectStatement.Sources = []influxql.Source{mst}
        }
        // 添加 value 字段
        selectStatement.Fields = append(selectStatement.Fields, &influxql.Field{
            Expr:  &influxql.VarRef{Val: valueFieldKey, Alias: DefaultFieldKey},
            Alias: DefaultFieldKey,
        })
        // 设置 PromQL 查询参数
        if t.Step > 0 {
            selectStatement.Step = t.Step
        }
        if t.timeRange > 0 {
            selectStatement.Range = t.timeRange
            t.timeRange = 0  // 使用后重置
        }
        selectStatement.LookBackDelta = t.LookBackDelta
        selectStatement.QueryOffset = v.Offset
        return selectStatement, nil
    }
}
```

**通俗解释**：

`VectorSelector` 是 PromQL 中最基本的查询单元，如 `http_requests_total{method="GET"}`。转译过程：

1. **生成条件**：
   - 时间条件：`time >= (minT - lookBackDelta) AND time <= maxT`
   - 标签条件：`method = 'GET'`（支持 `=`、`!=`、`=~`、`!~` 四种匹配符）
   - 两者用 AND 组合

2. **生成 SELECT 语句**：
   - `FROM http_requests_total`：指标名作为 measurement
   - `SELECT value`：默认字段
   - `GROUP BY *`：保留所有标签作为分组维度
   - `IsPromQuery: true`：标记这是 PromQL 查询，引擎会使用 PromQL 专属的采样逻辑

3. **设置查询参数**：
   - `Step`：查询步长，用于 `GROUP BY time(step)`
   - `Range`：窗口范围（从 MatrixSelector 继承）
   - `LookBackDelta`：回溯窗口（默认 5m）
   - `QueryOffset`：查询偏移量

### 4.2 标签条件生成

**代码位置**：`lib/util/lifted/promql2influxql/selector.go:73-122`

```go
func GetTagCondition(v *parser.VectorSelector, haveMetricStore bool) (influxql.Expr, error) {
	var tagCond influxql.Expr
	for _, item := range v.LabelMatchers {
		if _, ok := reservedTags[item.Name]; ok && !haveMetricStore {
			continue
		}
		if len(item.Value) == 0 {
			continue
		}
		var cond *influxql.BinaryExpr
		switch item.Type {
		case labels.MatchEqual, labels.MatchNotEqual:
			cond = &influxql.BinaryExpr{
				Op: influxql.EQ,
				LHS: &influxql.VarRef{
					Val: item.Name,
				},
				RHS: &influxql.StringLiteral{
					Val: escapeSingleQuotes(item.Value),
				},
			}
			if item.Type == labels.MatchNotEqual {
				cond.Op = influxql.NEQ
			}
		case labels.MatchRegexp, labels.MatchNotRegexp:
			// TODO: to support the FastRegexMatcher
			promRegexStr := escapeSlashes(item.Value)
			re, err := regexp.Compile(promRegexStr)
			if err != nil {
				return nil, errno.NewError(errno.ErrRegularExpSyntax, err.Error())
			}
			cond = &influxql.BinaryExpr{
				Op: influxql.EQREGEX,
				LHS: &influxql.VarRef{
					Val: item.Name,
				},
				RHS: &influxql.RegexLiteral{
					Val: re,
				},
			}
			if item.Type == labels.MatchNotRegexp {
				cond.Op = influxql.NEQREGEX
			}
		default:
			return nil, errno.NewError(errno.UnsupportedMatchType, item.Type.String())
		}
		tagCond = CombineConditionAnd(tagCond, cond)
	}
	return tagCond, nil
}
```

### 4.3 RangeVectorSelector 转译

**代码位置**：`lib/util/lifted/promql2influxql/selector.go:366-371`

```go
// transpileRangeVectorSelector 将 PromQL MatrixSelector 转译为 InfluxQL SelectStatement
func (t *Transpiler) transpileRangeVectorSelector(v *parser.MatrixSelector) (influxql.Node, error) {
    if v.Range > 0 {
        t.timeRange = v.Range  // 将 [5m] 中的 5m 存入 timeRange
    }
    return t.transpileExpr(v.VectorSelector)  // 递归转译内部的 VectorSelector
}
```

**通俗解释**：

`MatrixSelector`（如 `http_requests_total[5m]`）的转译非常简洁：
1. 将范围参数 `[5m]` 保存到 `t.timeRange`
2. 递归调用 `transpileExpr` 处理内部的 `VectorSelector`

`timeRange` 会在后续 `transpileInstantVectorSelector` 中被使用，用于：
- 调整查询的起始时间（`start = start - timeRange`）
- 设置 `SelectStatement.Range` 字段
- 传递给引擎层的 `RangeVectorCursor`

### 4.4 时间条件生成

**代码位置**：`lib/util/lifted/promql2influxql/selector.go:32-71`

```go
func GetTimeCondition(start, end *time.Time) influxql.Expr {
    var timeLhs, timeRhs *influxql.BinaryExpr
    if start != nil {
        timeLhs = &influxql.BinaryExpr{
            Op:  influxql.GTE,  // >=
            LHS: &influxql.VarRef{Val: TimeField},
            RHS: &influxql.TimeLiteral{Val: *start},
        }
    }
    if end != nil {
        timeRhs = &influxql.BinaryExpr{
            Op:  influxql.LTE,  // <=
            LHS: &influxql.VarRef{Val: TimeField},
            RHS: &influxql.TimeLiteral{Val: *end},
        }
    }
    if timeLhs == nil && timeRhs == nil {
        return nil
    }
    if timeLhs == nil { return timeRhs }
    if timeRhs == nil { return timeLhs }
    // 组合为 AND：time >= start AND time <= end
    return &influxql.BinaryExpr{
        Op:  influxql.AND,
        LHS: timeLhs,
        RHS: timeRhs,
    }
}
```

---

## 5. 函数调用翻译

### 5.1 六大函数映射表

PromQL 兼容层定义了 6 个函数映射表，覆盖了 PromQL 标准库中的绝大多数函数：

#### 5.1.1 rangeVectorFunctions（范围向量函数，22 个）

**代码位置**：`lib/util/lifted/promql2influxql/call.go:18-110`

```go
var rangeVectorFunctions = map[string]aggregateFn{
    "sum_over_time":     {name: "sum_over_time",         functionType: AGGREGATE_FN},
    "avg_over_time":     {name: "avg_over_time",         functionType: AGGREGATE_FN},
    "max_over_time":     {name: "max_over_time",         functionType: SELECTOR_FN},
    "min_over_time":     {name: "min_over_time",         functionType: SELECTOR_FN},
    "count_over_time":   {name: "count_over_time",       functionType: AGGREGATE_FN},
    "stddev_over_time":  {name: "stddev_over_time_prom", functionType: AGGREGATE_FN},
    "present_over_time": {name: "present_over_time_prom",functionType: AGGREGATE_FN},
    "last_over_time":    {name: "last_over_time_prom",   functionType: AGGREGATE_FN, keepMetric: true},
    "quantile_over_time":{name: "quantile_over_time_prom",functionType: SELECTOR_FN, vectorPosition: 1},
    "rate":              {name: "rate_prom",             functionType: TRANSFORM_FN},
    "irate":             {name: "irate_prom",            functionType: TRANSFORM_FN},
    "deriv":             {name: "deriv",                 functionType: TRANSFORM_FN},
    "predict_linear":    {name: "predict_linear",        functionType: TRANSFORM_FN},
    "increase":          {name: "increase",              functionType: TRANSFORM_FN},
    "delta":             {name: "delta_prom",            functionType: TRANSFORM_FN},
    "idelta":            {name: "idelta_prom",           functionType: TRANSFORM_FN},
    "stdvar_over_time":  {name: "stdvar_over_time_prom", functionType: TRANSFORM_FN},
    "holt_winters":      {name: "holt_winters_prom",     functionType: SELECTOR_FN, vectorPosition: 0},
    "changes":           {name: "changes_prom",          functionType: TRANSFORM_FN},
    "resets":            {name: "resets_prom",           functionType: TRANSFORM_FN},
    "absent_over_time":  {name: "absent_over_time_prom", functionType: AGGREGATE_FN},
    "mad_over_time":     {name: "mad_over_time_prom",    functionType: SELECTOR_FN},
}
```

**通俗解释**：

这些函数处理范围向量（带 `[5m]` 的查询），如 `rate(http_requests_total[5m])`。

关键的映射关系：
- `rate` -> `rate_prom`：名字加了 `_prom` 后缀
- `sum_over_time` -> `sum_over_time`：没有后缀，因为不存在命名冲突
- `stddev_over_time` -> `stddev_over_time_prom`：加了 `_prom` 后缀

`functionType` 决定了函数在引擎中的执行方式：
- `AGGREGATE_FN`：聚合函数，需要在滑动窗口内进行聚合计算
- `SELECTOR_FN`：选择器函数，从窗口中选择特定值
- `TRANSFORM_FN`：变换函数，对整个窗口的数据进行变换计算

`vectorPosition` 指定了哪个参数是向量数据（默认为最后一个参数位置）。例如 `quantile_over_time(0.99, metric[5m])` 中，`0.99` 是第一个参数，`metric[5m]` 是第二个（position=1）。

#### 5.1.2 instantVectorFunctions（即时向量函数，3 个）

**代码位置**：`lib/util/lifted/promql2influxql/call.go:112-127`

```go
var instantVectorFunctions = map[string]aggregateFn{
    "histogram_quantile": {name: "histogram_quantile", functionType: AGGREGATE_FN, vectorPosition: 1},
    "scalar":            {name: "scalar_prom",        functionType: AGGREGATE_FN},
    "absent":            {name: "absent_prom",        functionType: AGGREGATE_FN},
}
```

#### 5.1.3 vectorMathFunctions（数学函数，27 个）

**代码位置**：`lib/util/lifted/promql2influxql/call.go:128-264`

```go
var vectorMathFunctions = map[string]aggregateFn{
    "abs":   {name: "abs",       functionType: TRANSFORM_FN, KeepFill: true},
    "ceil":  {name: "ceil",      functionType: TRANSFORM_FN, KeepFill: true},
    "floor": {name: "floor",     functionType: TRANSFORM_FN, KeepFill: true},
    "exp":   {name: "exp",       functionType: TRANSFORM_FN, KeepFill: true},
    "sqrt":  {name: "sqrt",      functionType: TRANSFORM_FN, KeepFill: true},
    "ln":    {name: "ln",        functionType: TRANSFORM_FN, KeepFill: true},
    "log2":  {name: "log2",      functionType: TRANSFORM_FN, KeepFill: true},
    "log10": {name: "log10",     functionType: TRANSFORM_FN, KeepFill: true},
    "round": {name: "round_prom",functionType: TRANSFORM_FN, KeepFill: true},
    "acos":  {name: "acos",      functionType: TRANSFORM_FN, KeepFill: true},
    "asin":  {name: "asin",      functionType: TRANSFORM_FN, KeepFill: true},
    "atan":  {name: "atan",      functionType: TRANSFORM_FN, KeepFill: true},
    "cos":   {name: "cos",       functionType: TRANSFORM_FN, KeepFill: true},
    "sin":   {name: "sin",       functionType: TRANSFORM_FN, KeepFill: true},
    "tan":   {name: "tan",       functionType: TRANSFORM_FN, KeepFill: true},
    "clamp":     {name: "clamp_prom",     functionType: TRANSFORM_FN, KeepFill: true},
    "clamp_max": {name: "clamp_max_prom", functionType: TRANSFORM_FN, KeepFill: true},
    "clamp_min": {name: "clamp_min_prom", functionType: TRANSFORM_FN, KeepFill: true},
    "rad":   {name: "rad",       functionType: TRANSFORM_FN, KeepFill: true},
    "deg":   {name: "deg",       functionType: TRANSFORM_FN, KeepFill: true},
    "sinh":  {name: "sinh",      functionType: TRANSFORM_FN, KeepFill: true},
    "cosh":  {name: "cosh",      functionType: TRANSFORM_FN, KeepFill: true},
    "tanh":  {name: "tanh",      functionType: TRANSFORM_FN, KeepFill: true},
    "asinh": {name: "asinh",     functionType: TRANSFORM_FN, KeepFill: true},
    "atanh": {name: "atanh",     functionType: TRANSFORM_FN, KeepFill: true},
    "sgn":   {name: "sgn",       functionType: TRANSFORM_FN, KeepFill: true},
    "acosh": {name: "acosh",     functionType: TRANSFORM_FN, KeepFill: true},
}
```

**注意 `KeepFill: true`**：数学函数不会改变数据的填充方式，因此保留原始的 fill 行为。

#### 5.1.4 vectorLabelFunctions（标签函数，2 个）

```go
var vectorLabelFunctions = map[string]aggregateFn{
    "label_replace": {name: "label_replace", functionType: TRANSFORM_FN, KeepFill: true},
    "label_join":    {name: "label_join",    functionType: TRANSFORM_FN, KeepFill: true},
}
```

#### 5.1.5 vectorTimeFunctions（时间函数，12 个，包含 pi）

```go
var vectorTimeFunctions = map[string]aggregateFn{
    "year":          {name: "year_prom",          functionType: TRANSFORM_FN, KeepFill: true},
    "time":          {name: "time_prom",          functionType: TRANSFORM_FN, KeepFill: true},
    "timestamp":     {name: "timestamp_prom",     functionType: TRANSFORM_FN, KeepFill: true},
    "month":         {name: "month_prom",         functionType: TRANSFORM_FN, KeepFill: true},
    "minute":        {name: "minute_prom",        functionType: TRANSFORM_FN, KeepFill: true},
    "hour":          {name: "hour_prom",          functionType: TRANSFORM_FN, KeepFill: true},
    "day_of_week":   {name: "day_of_week_prom",   functionType: TRANSFORM_FN, KeepFill: true},
    "day_of_month":  {name: "day_of_month_prom",  functionType: TRANSFORM_FN, KeepFill: true},
    "day_of_year":   {name: "day_of_year_prom",   functionType: TRANSFORM_FN, KeepFill: true},
    "days_in_month": {name: "days_in_month_prom", functionType: TRANSFORM_FN, KeepFill: true},
    "vector":        {name: "vector_prom",        functionType: TRANSFORM_FN, KeepFill: true},
    "pi":            {name: "pi_prom",            functionType: TRANSFORM_FN, KeepFill: true},
}
```

#### 5.1.6 vectorSortFunctions（排序函数，4 个）

```go
var vectorSortFunctions = map[string]aggregateFn{
    "sort":              {name: "sort_prom",              functionType: SELECTOR_FN},
    "sort_desc":         {name: "sort_desc_prom",         functionType: SELECTOR_FN},
    "sort_by_label":     {name: "sort_by_label_prom",     functionType: SELECTOR_FN, vectorPosition: 0},
    "sort_by_label_desc":{name: "sort_by_label_desc_prom",functionType: SELECTOR_FN, vectorPosition: 0},
}
```

### 5.2 transpileCall() 调度逻辑

```mermaid
graph TD
    A["transpileCall(call)"] --> B["递归转译所有参数 args"]
    B --> C{"按顺序查找函数映射表"}
    C --> D["rangeVectorFunctions<br/>rate, irate, *_over_time..."]
    C --> E["instantVectorFunctions<br/>histogram_quantile, scalar, absent"]
    C --> F["vectorMathFunctions<br/>abs, ceil, floor, sqrt..."]
    C --> G["vectorLabelFunctions<br/>label_replace, label_join"]
    C --> H["vectorTimeFunctions<br/>year, time, timestamp, vector..."]
    C --> I["vectorSortFunctions<br/>sort, sort_desc, sort_by_label..."]
    D --> J["生成 InfluxQL Call 表达式"]
    E --> J
    F --> J
    G --> J
    H --> J
    I --> J
    C -->|"未找到"| K["返回 errno.UnsupportedPromExpr"]
```

**代码位置**：`lib/util/lifted/promql2influxql/call.go:579-627`

```go
// transpileCall 转译 PromQL Call 表达式
func (t *Transpiler) transpileCall(a *parser.Call) (influxql.Node, error) {
    // 步骤1：递归转译所有参数
    args := make([]influxql.Node, len(a.Args))
    for i := range a.Args {
        unwrapParenExpr(&a.Args[i])
        a.Args[i] = unwrapStepInvariantExpr(a.Args[i])
        tArg, err := t.transpileExpr(a.Args[i])
        if err != nil {
            return nil, errno.NewError(errno.TranspileFunctionFail, err.Error())
        }
        args[i] = tArg
    }

    // 步骤2：按优先级在 6 个函数表中查找
    // 范围向量函数（rate, irate, avg_over_time 等）
    if fn, ok := rangeVectorFunctions[a.Func.Name]; ok {
        t.dropMetric = true
        if subExpr, subOk := a.Args[fn.vectorPosition].(*parser.SubqueryExpr); subOk {
            return t.transpilePromSubqueryFunc(subExpr, fn, args)
        }
        return t.transpilePromFunc(fn, args, t.setAggregateFields)
    }
    // 即时向量函数（histogram_quantile, scalar, absent）
    if fn, ok := instantVectorFunctions[a.Func.Name]; ok {
        t.dropMetric = true
        return t.transpilePromFunc(fn, args, t.setAggregateFields)
    }
    // 数学函数（abs, ceil, floor 等）
    if fn, ok := vectorMathFunctions[a.Func.Name]; ok {
        t.dropMetric = true
        return t.transpileVectorMathFunc(fn, args)
    }
    // 标签函数（label_replace, label_join）
    if fn, ok := vectorLabelFunctions[a.Func.Name]; ok {
        return t.transpileVectorLabelFunc(fn, args)
    }
    // 时间函数（year, month, hour 等）
    if fn, ok := vectorTimeFunctions[a.Func.Name]; ok {
        if a.Func.Name != "time" {
            t.dropMetric = true
        }
        return t.transpileVectorTimeFunc(fn, args)
    }
    // 排序函数（sort, sort_desc 等）
    if fn, ok := vectorSortFunctions[a.Func.Name]; ok {
        return t.transpileVectorSortFunc(fn, args)
    }

    return nil, errno.NewError(errno.UnsupportedPromExpr)
}
```

**通俗解释**：

`transpileCall` 是函数调用的转译调度器。它的核心逻辑：

1. **先转译参数**：递归处理所有参数，将 PromQL 参数转译为 InfluxQL 节点
2. **按表查找**：依次在 6 个函数映射表中查找函数名
3. **分发处理**：不同类型的函数使用不同的处理方式
4. **特殊处理子查询**：如果范围向量函数的参数是子查询（如 `rate(metric[5m:1m])`），使用 `transpilePromSubqueryFunc`

### 5.3 transpilePromFunc() 通用转译逻辑

**代码位置**：`lib/util/lifted/promql2influxql/call.go:459-511`

```go
func (t *Transpiler) transpilePromFunc(aggFn aggregateFn, inArgs []influxql.Node, setFieldsFunc SetFieldsFunc) (influxql.Node, error) {
	table, parameter := t.transpileParameter(aggFn.vectorPosition, inArgs)

	node, ok := table.(influxql.Statement)
	if !ok {
		return t.transpilePromFuncWithExprArgs(aggFn, table)
	}
	switch statement := node.(type) {
	case *influxql.SelectStatement:
		field, _ := getSelectFieldIdx(statement)
		switch field.Expr.(type) {
		case *influxql.Call, *influxql.BinaryExpr:
			selectStatement := &influxql.SelectStatement{
				Sources: []influxql.Source{
					&influxql.SubQuery{
						Statement: statement,
					}},
				Step:        t.Step,
				IsPromQuery: true,
			}
			wrappedField := &influxql.Field{
				Expr: &influxql.VarRef{
					Val: field.Name(),
				},
				Alias: DefaultFieldKey,
			}
			setFieldsFunc(selectStatement, wrappedField, parameter, aggFn)
			t.setTimeCondition(selectStatement, false)
			if t.Step > 0 && !aggFn.KeepFill {
				t.setTimeInterval(selectStatement)
				selectStatement.Fill = influxql.NoFill
			}
			if aggFn.name == "histogram_quantile" {
				selectStatement.Dimensions = statement.Dimensions
			}
			return selectStatement, nil
		default:
			setFieldsFunc(statement, field, parameter, aggFn)
			if t.Step > 0 && !aggFn.KeepFill {
				t.setTimeInterval(statement)
				statement.Fill = influxql.NoFill
			}
			if aggFn.name == "absent_over_time_prom" {
				field, _ := getSelectFieldIdx(statement)
				absentFunc := instantVectorFunctions["absent"]
				t.setAggregateFields(statement, field, parameter, absentFunc)
			}
		}
	default:
		return nil, errno.NewError(errno.UnsupportedPromExpr)
	}
	return table, nil
}
```

**通俗解释**：

`transpilePromFunc` 是所有 PromQL 函数转译的通用框架。核心逻辑：

1. **参数分离**：`transpileParameter()` 将参数列表拆分为"表参数"（向量数据）和"函数参数"（标量参数）
   - 例如 `quantile_over_time(0.99, metric[5m])` 中，`metric[5m]` 是表参数，`0.99` 是函数参数

2. **判断是否需要子查询包装**：
   - 如果字段已经是 `Call` 或 `BinaryExpr`（如 `rate(value)`），需要包装为子查询，外层再套聚合
   - 如果字段是简单的 `VarRef`（如 `value`），可以直接在当前语句上设置聚合函数

3. **设置聚合字段**：通过 `setFieldsFunc` 回调设置具体的聚合函数

---

## 6. 聚合表达式

### 6.1 aggregateFns 映射表

**代码位置**：`lib/util/lifted/promql2influxql/aggregate_expr.go:22-35`

```go
var aggregateFns = map[parser.ItemType]aggregateFn{
    parser.SUM:          {name: "sum",              functionType: AGGREGATE_FN},
    parser.AVG:          {name: "mean",             functionType: AGGREGATE_FN},
    parser.MAX:          {name: "max_prom",         functionType: SELECTOR_FN},
    parser.MIN:          {name: "min_prom",         functionType: SELECTOR_FN},
    parser.COUNT:        {name: "count_prom",       functionType: AGGREGATE_FN},
    parser.STDDEV:       {name: "stddev_prom",      functionType: AGGREGATE_FN},
    parser.TOPK:         {name: "top",              functionType: SELECTOR_FN,
                         expectIntegerParameter: true, keepMetric: true, keepAuxLabel: true},
    parser.BOTTOMK:      {name: "bottom",           functionType: SELECTOR_FN,
                         expectIntegerParameter: true, keepMetric: true, keepAuxLabel: true},
    parser.QUANTILE:     {name: "quantile_prom",    functionType: SELECTOR_FN},
    parser.COUNT_VALUES: {name: "count_values_prom",functionType: AGGREGATE_FN},
    parser.STDVAR:       {name: "stdvar_prom",      functionType: AGGREGATE_FN},
    parser.GROUP:        {name: "group_prom",       functionType: AGGREGATE_FN},
}
```

**通俗解释**：

PromQL 聚合操作符与 InfluxQL 函数的映射关系：

| PromQL 操作符 | InfluxQL 函数名 | 说明 |
|---------------|-----------------|------|
| `sum` | `sum` | 直接使用 InfluxQL 的 sum |
| `avg` | `mean` | InfluxQL 用 mean 表示平均值 |
| `max` | `max_prom` | 加 `_prom` 后缀，避免与 InfluxQL 原生 max 冲突 |
| `min` | `min_prom` | 加 `_prom` 后缀 |
| `count` | `count_prom` | 加 `_prom` 后缀 |
| `stddev` | `stddev_prom` | 加 `_prom` 后缀 |
| `topk` | `top` | InfluxQL 使用 top 函数 |
| `bottomk` | `bottom` | InfluxQL 使用 bottom 函数 |
| `quantile` | `quantile_prom` | 加 `_prom` 后缀 |
| `count_values` | `count_values_prom` | 加 `_prom` 后缀 |
| `stdvar` | `stdvar_prom` | 加 `_prom` 后缀 |
| `group` | `group_prom` | 加 `_prom` 后缀 |

**特别注意**：
- `TOPK` 和 `BOTTOMK` 的 `keepMetric: true, keepAuxLabel: true`：这两个函数在聚合后仍需保留原始的指标标签（因为 topk 选择的是完整的时间序列，不是聚合值）
- `expectIntegerParameter: true`：topk/bottomk 的第一个参数（k 值）期望是整数

### 6.2 transpileAggregateExpr() 转译逻辑

**代码位置**：`lib/util/lifted/promql2influxql/aggregate_expr.go:163-261`

```go
// transpileAggregateExpr transpiles PromQL AggregateExpr to InfluxQL SelectStatement
func (t *Transpiler) transpileAggregateExpr(a *parser.AggregateExpr) (influxql.Node, error) {
	// Recursively transpile sub expression
	t.dropMetric = true
	expr, err := t.transpileExpr(a.Expr)
	if err != nil {
		return nil, errno.NewError(errno.TranspileAggFail, err.Error())
	}

	// If the aggregate expression contains the "without" and it is true, reset the dropMetric flag to true
	// Otherwise, if the "__name__" field is found in grouping fields, set the dropMetric flag to false
	if a.Without {
		t.dropMetric = true
	} else {
		for _, field := range a.Grouping {
			if field == DefaultMetricKeyLabel {
				t.dropMetric = false
				break
			}
		}
	}

	// Get Aggregate function parameter
	var parameter []influxql.Expr
	if a.Param != nil {
		unwrapParenExpr(&a.Param)
		a.Param = unwrapStepInvariantExpr(a.Param)
		if !yieldsFloat(a.Param) && !yieldsString(a.Param) {
			return nil, errno.NewError(errno.ErrFloatParamAggExpr)
		}
		param, err := t.transpileExpr(a.Param)
		if err != nil {
			return nil, errno.NewError(errno.TranspileAggFail, err.Error())
		}
		parameter = []influxql.Expr{param.(influxql.Expr)}
	}
	aggFn, ok := aggregateFns[a.Op]
	if !ok {
		return nil, errno.NewError(errno.UnsupportedAggType, a.Op.String())
	}

	node, ok := expr.(influxql.Statement)
	if !ok {
		return nil, errno.NewError(errno.UnsupportedPromExpr)
	}

	switch statement := node.(type) {
	case *influxql.SelectStatement:
		field, _ := getSelectFieldIdx(statement)
		switch field.Expr.(type) {
		case *influxql.Call, *influxql.BinaryExpr, *influxql.ParenExpr:
			if t.canPushDownAggWithFunction(a, statement, field, parameter, aggFn) {
				return statement, nil
			}
			// If the field is a Call expression, we need to wrap the sub expression as InfluxQL SubQuery
			selectStatement := &influxql.SelectStatement{
				Sources: []influxql.Source{
					&influxql.SubQuery{
						Statement: statement,
					}},
				Step:        t.Step,
				IsPromQuery: true,
			}
			// ...
			return selectStatement, nil
		case *influxql.VarRef:
			t.setAggregateFields(statement, field, parameter, aggFn)
			statement.Dimensions = statement.Dimensions[:0]
			t.setAggregateDimension(statement, a.Without, a.Grouping...)
			if t.Step > 0 {
				t.setTimeInterval(statement)
				statement.Fill = influxql.NoFill
			}

			if aggFn.name == "count_values_prom" {
				return t.transpileCountValues(statement, a, aggFn, field, parameter), nil
			}
			return statement, nil
		default:
			return nil, errno.NewError(errno.UnsupportedPromExpr)
		}
	default:
		return expr, nil
	}
}
```

**通俗解释**：

以 `sum by (method)(rate(http_requests_total[5m]))` 为例：

1. **递归转译子表达式**：`rate(http_requests_total[5m])` -> `SelectStatement` with `rate_prom(value)`
2. **处理 grouping**：`by (method)` 设置分组维度；如果 grouping 包含 `__name__`，则保留指标名
3. **查找聚合函数**：`SUM` -> `name: "sum"`
4. **子表达式是函数调用**（`rate_prom`）：
   - 尝试优化下推：`canPushDownAggWithFunction` 用 `getOriCallName("rate_prom")` 得到 `rate`，命中 `rangeVectorFunctions`，且 `statement.Range=5m>0`，因此返回 true
   - 命中时直接把 `sum(rate_prom(value))` 设到原语句上，不创建子查询；只有下推条件不满足时才包装为子查询

最终 InfluxQL：
```sql
SELECT sum(rate_prom(value)) FROM http_requests_total
WHERE time >= ... AND time <= ... AND method = 'GET'
GROUP BY method
```

> 若为 range query 且 `Step>0`，GROUP BY 还会追加 `time(step)` 并设置 `fill(none)`。

### 6.3 by/without 维度处理

**代码位置**：`lib/util/lifted/promql2influxql/aggregate_expr.go:88-124`

```go
// setAggregateDimension 设置 selectStatement 的 GROUP BY 表达式
func (t *Transpiler) setAggregateDimension(statement *influxql.SelectStatement, without bool, grouping ...string) {
    if !without {
        // by (label1, label2)：直接使用 grouping 作为维度
        t.generateDimension(statement, grouping...)
        return
    }
    // without (label1, label2)：从完整维度集中排除 grouping
    if len(statement.Sources) != 1 {
        panic("the number of source should be 1 for promql agg query")
    }
    switch source := statement.Sources[0].(type) {
    case *influxql.Measurement:
        // 从全集中排除
        statement.Without = true
        t.generateDimension(statement, grouping...)
    case *influxql.SubQuery:
        // 从子查询维度中排除
        if source.Statement.Without {
            statement.Without = true
            t.generateDimension(statement, grouping...)
            return
        }
        t.setAggregateDimensionOfSubquery(source.Statement.Dimensions, statement, grouping...)
    case *influxql.BinOp:
        // 从两个子查询的联合维度中排除
        // ...
    }
}
```

**通俗解释**：

- `sum by (method)`：GROUP BY method
- `sum without (instance)`：GROUP BY 除 instance 外的所有标签（通过设置 `Without: true` 实现）
- 子查询场景：需要从子查询的维度列表中排除 `without` 指定的标签

---

## 7. 二元表达式

### 7.1 运算符映射

**代码位置**：`lib/util/lifted/promql2influxql/binary_expr.go:9-27`

```go
// 算术运算符映射
var arithBinOps = map[parser.ItemType]influxql.Token{
    parser.ADD:   influxql.ADD,      // +
    parser.SUB:   influxql.SUB,      // -
    parser.MUL:   influxql.MUL,      // *
    parser.DIV:   influxql.DIV,      // /
    parser.MOD:   influxql.MOD,      // %
    parser.POW:   influxql.POW_OP,   // ^
    parser.ATAN2: influxql.ATAN2_OP, // atan2
}

// 比较运算符映射
var compBinOps = map[parser.ItemType]influxql.Token{
    parser.EQLC: influxql.EQ,   // ==
    parser.NEQ:  influxql.NEQ,  // !=
    parser.GTR:  influxql.GT,   // >
    parser.LSS:  influxql.LT,   // <
    parser.GTE:  influxql.GTE,  // >=
    parser.LTE:  influxql.LTE,  // <=
}
```

### 7.2 transpileBinaryExpr() 三路分发

```mermaid
graph TD
    A["transpileBinaryExpr(BinaryExpr)"] --> B["分别转译左右子表达式 LHS / RHS"]
    B --> C{"判断操作数类型"}
    C -->|"Scalar + Scalar"| D["NewBinaryExpr()<br/>直接构造 InfluxQL BinaryExpr"]
    C -->|"Vector + Scalar"| E["transpileArithBinOps()/transpileCompBinOps()<br/>向量与标量运算"]
    C -->|"Vector + Vector"| F{"两侧都是 SelectStatement?"}
    F -->|"是"| F1["transpileBinOpOfBothVector()<br/>向量间按标签匹配"]
    F -->|"否"| F2["transpileBinOpOfOneVector()<br/>一侧为表达式"]
    E --> G["处理 VectorMatching<br/>on / ignoring / group_left / group_right"]
    F1 --> G
    F2 --> G
    D --> H["返回 InfluxQL 表达式"]
    G --> H
```

**代码位置**：`lib/util/lifted/promql2influxql/binary_expr.go:223-288`

```go
// transpileBinaryExpr 转译 PromQL BinaryExpr
func (t *Transpiler) transpileBinaryExpr(b *parser.BinaryExpr) (influxql.Node, error) {
    // 步骤1：分别转译左右子表达式
    lhs, err := t.transpileExpr(b.LHS)
    if err != nil { return nil, errno.NewError(errno.UnableLeftBinOp, err.Error()) }
    lDropMetric := t.dropMetric
    t.dropMetric = false
    rhs, err := t.transpileExpr(b.RHS)
    if err != nil { return nil, errno.NewError(errno.UnableRightBinOp, err) }
    rDropMetric := t.dropMetric

    // 步骤2：根据操作数类型分发
    switch {
    case yieldsFloat(b.LHS) && yieldsFloat(b.RHS):
        // 情况1：标量 vs 标量（如 1 + 2）
        if op, ok := arithBinOps[b.Op]; ok {
            return t.NewBinaryExpr(op, lhs.(influxql.Expr), rhs.(influxql.Expr), false), nil
        }
        if op, ok := compBinOps[b.Op]; ok {
            return t.NewBinaryExpr(op, lhs.(influxql.Expr), rhs.(influxql.Expr), b.ReturnBool), nil
        }

    case yieldsFloat(b.LHS) && yieldsTable(b.RHS),
         yieldsTable(b.LHS) && yieldsFloat(b.RHS):
        // 情况2：标量 vs 向量（如 metric > 100）
        swap := yieldsFloat(b.LHS) && yieldsVector(b.RHS)
        if op, ok := arithBinOps[b.Op]; ok {
            return t.transpileArithBinOps(b, op, lhs, rhs, swap)
        }
        if op, ok := compBinOps[b.Op]; ok {
            return t.transpileCompBinOps(b, op, lhs, rhs, swap)
        }

    case yieldsVector(b.LHS) && yieldsVector(b.RHS):
        // 情况3：向量 vs 向量（如 metric1 + metric2）
        // ...使用 BinOp 节点实现向量匹配

    default:
        return nil, errno.NewError(errno.UnsupportedBothVS, b.String())
    }
    return nil, errno.NewError(errno.InvalidSVBinOp, b.Op.String())
}
```

**通俗解释**：

二元表达式有三种情况：

1. **标量 vs 标量**（`1 + 2`）：直接映射为 `influxql.BinaryExpr`
2. **标量 vs 向量**（`metric > 100`）：
   - 算术运算：将标量运算嵌入到向量的字段表达式中（如 `SELECT value + 100 FROM ...`）
   - 比较运算：将比较条件添加到 WHERE 子句（如 `WHERE value > 100`）
3. **向量 vs 向量**（`metric1 + metric2`）：使用 `influxql.BinOp` 节点，需要处理向量匹配（on/ignoring、group_left/group_right）

### 7.3 BinOp 节点

**代码位置**：`lib/util/lifted/promql2influxql/binary_expr.go:338-366`

```go
func (t *Transpiler) transpileBinOpOfBothVector(b *parser.BinaryExpr, op influxql.Token, lStmt, rStmt *influxql.SelectStatement) (influxql.Node, error) {
    lSub := &influxql.SubQuery{Statement: lStmt}
    rSub := &influxql.SubQuery{Statement: rStmt}
    binOp := &influxql.BinOp{
        LSrc:        lSub,                              // 左操作数源
        RSrc:        rSub,                              // 右操作数源
        OpType:      int(op),                           // 操作类型
        On:          b.VectorMatching.On,               // on 标签
        MatchKeys:   b.VectorMatching.MatchingLabels,   // 匹配标签
        MatchCard:   influxql.MatchCardinality(b.VectorMatching.Card),  // 基数（one/many）
        IncludeKeys: b.VectorMatching.Include,           // include 标签
        ReturnBool:  b.ReturnBool,                       // bool 修饰符
    }
    newStmt := &influxql.SelectStatement{
        Sources:     influxql.Sources{binOp},
        Fields:      influxql.Fields{&influxql.Field{Expr: &influxql.VarRef{Val: DefaultFieldKey}}},
        IsPromQuery: true,
        Step:        lStmt.Step,
    }
    return newStmt, nil
}
```

**通俗解释**：

`BinOp` 是 openGemini 为 PromQL 向量-向量二元运算专门扩展的 InfluxQL 节点。它封装了：
- 两个子查询源（LSrc/RSrc）
- 操作类型
- 向量匹配规则（on/ignoring/group_left/group_right）

---

## 8. 引擎层实现

### 8.1 PromFunction 接口与注册

**代码位置**：`engine/prom_functions.go:29-77`

```go
func init() {
    // 注册 22 个 PromQL 函数到引擎层
    RegistryPromFunction("rate_prom",              &rateOp{})
    RegistryPromFunction("irate_prom",             &irateOp{})
    RegistryPromFunction("avg_over_time",          &avgOp{})
    RegistryPromFunction("count_over_time",        &countOp{})
    RegistryPromFunction("sum_over_time",          &sumOp{})
    RegistryPromFunction("min_over_time",          &minOp{})
    RegistryPromFunction("max_over_time",          &maxOp{})
    RegistryPromFunction("last_over_time_prom",    &lastOp{})
    RegistryPromFunction("increase",               &increaseOp{})
    RegistryPromFunction("deriv",                  &derivOp{})
    RegistryPromFunction("predict_linear",         &predictLinearOp{})
    RegistryPromFunction("delta_prom",             &deltaOp{})
    RegistryPromFunction("idelta_prom",            &ideltaOp{})
    RegistryPromFunction("stdvar_over_time_prom",  &stdVarOverTime{})
    RegistryPromFunction("stddev_over_time_prom",  &stdDevOverTime{})
    RegistryPromFunction("present_over_time_prom", &intervalExistMark{})
    RegistryPromFunction("holt_winters_prom",      &holtWintersOp{})
    RegistryPromFunction("changes_prom",           &changesOp{})
    RegistryPromFunction("quantile_over_time_prom",&quantileOverTime{})
    RegistryPromFunction("resets_prom",            &resetsOp{})
    RegistryPromFunction("absent_over_time_prom",  &intervalExistMark{})
    RegistryPromFunction("mad_over_time_prom",     &madOverTimeOp{})
}

// PromFunction 接口：每个 PromQL 函数必须实现此接口
type PromFunction interface {
    CreateRoutine(param *PromFuncParam) (Routine, error)
}

// PromFuncParam 函数参数
type PromFuncParam struct {
    inOrdinal, outOrdinal int         // 输入/输出字段的序号
    args                  []influxql.Expr // 函数参数
}

// 全局函数注册表
var factoryInstance = make(map[string]PromFunction)

func GetPromFunction(name string) PromFunction {
    return factoryInstance[name]
}

type PromFunctionFactory map[string]PromFunction

func RegistryPromFunction(name string, aggOp PromFunction) {
    _, ok := factoryInstance[name]
    if ok { return }
    factoryInstance[name] = aggOp
}
```

**通俗解释**：

引擎层的函数注册机制：
1. **init() 注册**：在程序启动时，将 22 个 PromQL 函数注册到全局工厂
2. **PromFunction 接口**：每个函数实现 `CreateRoutine()` 方法，返回一个 `Routine`（计算例程）
3. **Routine/Reducer 接口**：`Routine` 只有 `WorkOnRecord`；具体的 Reduce/Merge 由持有的 `Reducer` 在 `Aggregate` 中通过 `fr`/`fm` 完成

### 8.2 rate 函数实现——外推算法

```mermaid
graph TD
    A["floatPromRateMerge()"] --> B{"pointCount <= 1?"}
    B -->|"是"| C["返回 0, true（无法计算）"]
    B -->|"否"| D["CalcReduceResult()<br/>计算 firstTime, lastTime, firstValue, reduceResult"]
    D --> E["计算 durationToStart = firstTime - rangeStart"]
    D --> F["计算 durationToEnd = rangeEnd - lastTime"]
    E --> G["计算 sampledInterval = lastTime - firstTime"]
    F --> G
    G --> H["计算 averageDurationBetweenSamples"]
    H --> I{"isCounter && reduceResult > 0 && pointCount > 0 && firstValue >= 0?"}
    I -->|"是"| J["durationToZero = sampledInterval * (firstValue / reduceResult)<br/>缩短 durationToStart"]
    I -->|"否"| K["extrapolationThreshold = avgDuration * 1.1"]
    J --> K
    K --> L{"durationToStart >= threshold?"}
    L -->|"是"| M["durationToStart = avgDuration / 2"]
    L -->|"否"| N["extrapolateToInterval += durationToStart"]
    M --> N
    N --> O{"durationToEnd >= threshold?"}
    O -->|"是"| P["durationToEnd = avgDuration / 2"]
    O -->|"否"| Q["extrapolateToInterval += durationToEnd"]
    P --> Q
    Q --> R["resultValue = reduceResult * (extrapolateToInterval / sampledInterval)"]
    R --> S{"isRate?"}
    S -->|"是"| T["resultValue /= rangeDuration(s)"]
    S -->|"否"| U["返回 resultValue"]
    T --> U
```

**代码位置**：`engine/prom_functions.go:100-162`

```go
// rate 函数
type rateOp struct{}

func (o *rateOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
    // 使用 floatSliceReducer，包含 Reduce 和 Merge 两个阶段
    return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(true, true)), p.inOrdinal, p.outOrdinal), nil
}

// Reduce 阶段：提取窗口内的数据切片
func floatPromRateReduce(times []int64, values []float64, start, end int) ([]int64, []float64, bool) {
    if start >= end {
        return []int64{}, []float64{}, true  // 空窗口
    }
    return times[start:end], values[start:end], false
}

// Merge 阶段：外推计算 rate 值
func floatPromRateMerge(isRate, isCounter bool) FloatSliceMergeFunc {
    return func(prevT, currT []int64, prevV, currV []float64, ts int64, pointCount int, param *ReducerParams) (float64, bool) {
        if pointCount <= 1 {
            return 0, true  // 少于 2 个点，无法计算 rate
        }
        // 计算 counter 的增量
        firstTime, lastTime, firstValue, _, reduceResult := executor.CalcReduceResult(prevT, currT, prevV, currV, isCounter)
        if lastTime == firstTime || param.rangeDuration == 0 {
            return 0, true
        }

        rangeStart, rangeEnd := ts-param.rangeDuration, ts

        // 计算首尾样本与范围边界的距离
        durationToStart := float64(firstTime-rangeStart) / 1e9
        durationToEnd := float64(rangeEnd-lastTime) / 1e9

        sampledInterval := float64(lastTime-firstTime) / 1e9
        averageDurationBetweenSamples := sampledInterval / float64(pointCount-1)

        // Counter 类型的特殊处理：如果第一个值接近 0，缩短外推
        if isCounter && reduceResult > 0 && pointCount > 0 && firstValue >= 0 {
            durationToZero := sampledInterval * (firstValue / reduceResult)
            if durationToZero < durationToStart {
                durationToStart = durationToZero
            }
        }

        // 外推阈值：采样间隔的 1.1 倍
        extrapolationThreshold := averageDurationBetweenSamples * 1.1
        extrapolateToInterval := sampledInterval

        if durationToStart >= extrapolationThreshold {
            durationToStart = averageDurationBetweenSamples / 2
        }
        extrapolateToInterval += durationToStart

        if durationToEnd >= extrapolationThreshold {
            durationToEnd = averageDurationBetweenSamples / 2
        }
        extrapolateToInterval += durationToEnd

        // 计算最终值
        resultValue := reduceResult * (extrapolateToInterval / sampledInterval)
        if isRate {
            resultValue = resultValue / float64(param.rangeDuration/1e9)
        }
        return resultValue, false
    }
}
```

**通俗解释**：

rate 的外推算法是 PromQL 最核心也最微妙的计算逻辑之一：

1. **前提**：至少需要 2 个数据点
2. **计算增量**：对 counter 类型，计算 `lastValue - firstValue`（考虑 counter reset）
3. **外推逻辑**：
   - 计算首样本到窗口起点的距离（`durationToStart`）
   - 计算末样本到窗口终点的距离（`durationToEnd`）
   - 如果距离超过"平均采样间隔 * 1.1"的阈值，只外推半个采样间隔
   - 否则，按实际距离外推
4. **Counter 特殊处理**：如果 counter 从 0 开始增长，外推距离不能超过到 0 的时间
5. **最终计算**：`value = delta * (extrapolateInterval / sampledInterval) / rangeDuration`

#### 数值示例：rate 外推算法逐步推演

假设查询 `rate(http_requests_total[5m])`，评估时间 `ts=500s`，窗口为 `[200s, 500s]`，采样到以下数据点（100s 的点在窗口外，不参与本次计算）：

| 时间 (s) | 值 | 说明 |
|----------|-----|------|
| 100 | 100 | 窗口外（不参与） |
| 200 | 200 | 第一个样本（= rangeStart） |
| 300 | 150 | counter reset（值减小） |
| 400 | 250 | |
| 500 | 350 | 最后一个样本（= rangeEnd） |

**第一步：计算增量（考虑 counter reset）**

```
CalcReduceResult 处理 counter reset：
  - reduceResult = lastValue - firstValue = 350 - 200 = 150
  - 扫描窗口内样本：300s 处 150 < 200，检测到 reset，reduceResult += prev(200) → 350
  - firstValue = 200, firstTime = 200s, lastValue = 350, lastTime = 500s
```

**第二步：计算时间距离**

```
rangeStart = 500 - 300 = 200s（窗口起点，ts=500, rangeDuration=300s）
rangeEnd = 500s（窗口终点）

durationToStart = (200s - 200s) / 1e9 = 0s
durationToEnd = (500s - 500s) / 1e9 = 0s
sampledInterval = (500s - 200s) / 1e9 = 300s
pointCount = 4
averageDurationBetweenSamples = 300 / (4-1) = 100s
```

> 注意：代码直接计算 `firstTime - rangeStart`，不会对负值做 `max(0, ...)` 截断；本例首点正好落在窗口起点，因此 `durationToStart = 0`。

**第三步：Counter 从 0 开始的特殊处理**

```
isCounter = true, reduceResult = 350 > 0, firstValue = 200 >= 0
durationToZero = sampledInterval * (firstValue / reduceResult)
  = 300 * (200 / 350) = 171.43s

durationToZero (171.43s) < durationToStart (0s)? → 否
所以 durationToStart 保持为 0
```

**第四步：外推阈值判断**

```
extrapolationThreshold = averageDurationBetweenSamples * 1.1 = 100 * 1.1 = 110s
extrapolateToInterval = sampledInterval = 300s

durationToStart (0s) >= 110s? → 否，extrapolateToInterval += 0 → 300s
durationToEnd (0s) >= 110s? → 否，extrapolateToInterval += 0 → 300s
```

**第五步：最终计算**

```
resultValue = reduceResult * (extrapolateToInterval / sampledInterval)
  = 350 * (300 / 300) = 350

由于 isRate = true：
resultValue = resultValue / rangeDuration(s) = 350 / 300 ≈ 1.1667

最终 rate ≈ 1.1667（每秒增长约 1.17 个请求）
```

> **关键洞察**：当数据点恰好覆盖整个窗口且首尾都在边界时，外推比例为 1:1，rate 等于 `reduceResult / rangeDuration`。外推算法主要在数据点稀疏或窗口边缘缺少数据时发挥作用。

### 8.3 avg_over_time 函数实现——Kahan 求和

**代码位置**：`engine/prom_functions.go:178-213`

```go
// avg_over_time 函数
type avgOp struct{}

func (o *avgOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
    return NewRoutineImpl(newFloatIncReducer(floatAvgReduce, floatAvgMergeFunc), p.inOrdinal, p.outOrdinal), nil
}

// 使用 Kahan 求和的平均值计算
func floatAvgReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
    if start == end {
        return 0, 0, true
    }
    var mean, count, c float64
    for i := start; i < end; i++ {
        count++
        // 处理 Inf 的特殊情况
        if math.IsInf(mean, 0) {
            if math.IsInf(values[i], 0) && (mean > 0) == (values[i] > 0) {
                continue  // 同号 Inf，mean 已正确
            }
            if !math.IsInf(values[i], 0) && !math.IsNaN(values[i]) {
                continue  // mean 是 Inf，值不是 Inf/NaN，保持 mean
            }
        }
        // 使用 Kahan 求和增量更新平均值
        mean, c = executor.KahanSumInc(values[i]/count-mean/count, mean, c)
    }
    if math.IsInf(mean, 0) {
        return times[start], mean, false
    }
    return times[start], mean + c, false  // 加上补偿项
}
```

**通俗解释**：

Kahan 求和（也叫补偿求和）是一种减少浮点数累加误差的算法。在计算平均值时：
- 常规方法：`sum += x; mean = sum / count`，当累加大量数据时，浮点精度损失严重
- Kahan 方法：维护一个补偿变量 `c`，记录每次加法的舍入误差，在下次加法时补偿回来

### 8.4 increase 函数

```go
// increase = rate * rangeDuration
type increaseOp struct{}

func (o *increaseOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
    // isRate=false, isCounter=true：计算绝对增量而非速率
    return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(false, true)), p.inOrdinal, p.outOrdinal), nil
}
```

**通俗解释**：`increase` 和 `rate` 共享相同的 Reduce 和 Merge 逻辑，唯一的区别是 `isRate=false`——rate 会将结果除以 `rangeDuration`，而 increase 不除。

---

## 9. InstantVectorCursor

### 9.1 结构与创建

**代码位置**：`engine/prom_instant_vector_cursor.go:38-73`

```go
// InstantVectorCursor 用于对 PromQL instant_query 或 range_query 的数据进行采样和处理。
// 采样是非聚合查询和聚合查询的原始数据处理第一步。
// 对于非聚合查询，放在 seriesCursor 之后。
// 对于聚合查询，替换 aggregateCursor。
//
// 示例：
// data: (time, value)=> [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (6, 6.0)]
// start=1, end=6, offset=0, step=2, LookUpDelta=3 => startSample=1, endSample=5
// sample data: (time, value)=> [(1, 1.0), (3, 3.0), (5, 4.0)]
type InstantVectorCursor struct {
    aggregateCursor

    lookUpDelta int64  // 回溯窗口（纳秒）
    start       int64  // 查询起始时间
    end         int64  // 查询结束时间
    offset      int64  // 查询偏移量
    step        int64  // 步长
    startSample int64  // 采样起始时间
    endSample   int64  // 采样结束时间
    firstStep   int64  // 当前记录的第一个步长时间
}

func NewInstantVectorCursor(input comm.KeyCursor, schema *executor.QuerySchema,
    globalPool *record.RecordPool, tr util.TimeRange) *InstantVectorCursor {
    c := &InstantVectorCursor{}
    c.aggregateCursor = *NewAggregateCursor(input, schema, globalPool, false)
    c.aggregateCursor.r = c

    c.lookUpDelta = schema.Options().GetPromLookBackDelta().Nanoseconds()
    c.step = schema.Options().GetPromStep().Nanoseconds()
    c.start = schema.Options().GetStartTime()
    c.end = schema.Options().GetEndTime()
    c.offset = schema.Options().GetPromQueryOffset().Nanoseconds()
    c.startSample = c.start + c.lookUpDelta
    if c.step == 0 {
        // instant query：单点查询
        c.endSample = c.startSample
        c.firstStep = c.startSample
        c.reducerParams.lastStep = c.endSample
        c.reducerParams.rangeDuration = schema.Options().GetPromRange().Nanoseconds()
    } else {
        // range query：多点查询
        c.endSample = c.start + c.lookUpDelta + (c.end-(c.start+c.lookUpDelta))/c.step*c.step
        c.firstStep = getCurrStep(c.startSample, c.endSample, c.step, tr.Min)
        c.reducerParams.lastStep = getPrevStep(c.startSample, c.endSample, c.step, tr.Max)
    }
    return c
}
```

**通俗解释**：

`InstantVectorCursor` 是 PromQL 查询的核心采样器。它实现了 Prometheus 的"即时向量选择器"语义：

- **采样逻辑**：对于每个时间步 `t`，在 `[t - lookUpDelta, t]` 范围内查找最近的样本值
- **步长计算**：`startSample = c.start + lookUpDelta`，`endSample = c.start + lookUpDelta + floor((c.end - (c.start + lookUpDelta)) / step) * step`；其中 `c.start/c.end` 来自 `schema.Options().GetStartTime()/GetEndTime()`，是**存储扫描起点/终点**（转译层已按 lookback/range 调整），不是 HTTP 请求里的查询起点
- **示例**：`c.start=1, c.end=6, step=2, lookUpDelta=3`，则 `startSample=4, endSample=6`。若以查询起点 `queryStart=1`、lookback=3 为例，则扫描起点 `c.start=-2`，得到 `startSample=1, endSample=5`，采样点为 `1, 3, 5`

### 9.2 双指针采样算法

```mermaid
sequenceDiagram
    participant Data as 数据点数组
    participant I as i 指针（数据索引）
    participant J as j 指针（步长索引）

    Note over Data: 数据: [t1=1,v1] [t3=3,v2] [t5=5,v3] [t8=8,v4] [t10=10,v5]
    Note over J: 查询: step=2, lookBackDelta=3

    J->>J: j = step1 (t=2)
    I->>I: i=0, data[0].time=t1

    rect rgb(230, 245, 255)
        Note over I,J: 步长 t=2: 窗口 [-1,2]，i 停在 t1(i=0)，j 找到首个 >2 的 t3(j=1) → 区间 [0,1)，取 v1
    end

    J->>J: j = step2 (t=4)
    rect rgb(230, 245, 255)
        Note over I,J: 步长 t=4: 窗口 [1,4]，i 仍为 0（t1=1 >= start=1），j 移到 t5(j=2) → 区间 [0,2)，取 v2
    end

    J->>J: j = step3 (t=6)
    rect rgb(230, 245, 255)
        Note over I,J: 步长 t=6: 窗口 [3,6]，i 移到 t3(i=1)，j 移到 t8(j=3) → 区间 [1,3)，取 v3
    end

    J->>J: j = step4 (t=8)
    rect rgb(255, 245, 230)
        Note over I,J: 步长 t=8: 窗口 [5,8]，i 移到 t5(i=2)，j 移到 t10(j=4) → 区间 [2,4)，取 v4
    end
```

**代码位置**：`engine/prom_instant_vector_cursor.go:129-160`

```go
// computeIntervalIndex 使用双指针算法快速找到目标步长的起止位置
func (c *InstantVectorCursor) computeIntervalIndex(record *record.Record) {
    if c.step == 0 {
        // instant query：整个记录作为一个区间
        c.intervalIndex = append(c.intervalIndex, 0, uint16(record.RowNums()))
        return
    }
    times := record.Times()
    firstStep := getCurrStep(c.startSample, c.endSample, c.step, times[0])
    lastStep := getCurrStep(c.startSample, c.endSample, c.step, times[len(times)-1])
    c.firstStep = firstStep
    var i, j int
    // 对每个步长时间点，找到窗口的起止索引
    for end := firstStep; end <= lastStep; end += c.step {
        start := end - c.lookUpDelta
        // i 指针：找到 >= start 的第一个点
        for i < len(times) {
            if times[i] >= start {
                c.intervalIndex = append(c.intervalIndex, uint16(i))
                break
            }
            i++
        }
        // j 指针：找到 > end 的第一个点
        for j < len(times) {
            if times[j] > end {
                c.intervalIndex = append(c.intervalIndex, uint16(j))
                break
            }
            j++
        }
        // 边界处理
        if len(c.intervalIndex)%2 != 0 {
            c.intervalIndex = append(c.intervalIndex, uint16(record.RowNums()))
        }
    }
}
```

**通俗解释**：

这是整个采样过程的核心算法。`intervalIndex` 存储了一系列 `[start, end)` 区间对，每个区间对应一个步长时间窗口内的数据范围。

**双指针算法**：
- `i` 指针：向右扫描，找到每个窗口的起始位置（`times[i] >= start`）
- `j` 指针：向右扫描，找到每个窗口的结束位置（`times[j] > end`）
- 两个指针都只向右移动，不会回退，因此时间复杂度为 O(n)

### 9.3 floatSampler 采样器

```mermaid
graph TD
    A["floatSampler.Aggregate()"] --> D["遍历当前 record 的每个步长窗口"]
    D --> E{"窗口内取到值?<br/>(isNil == false)"}
    E -->|"是"| F{"IsStaleNaN(value)?"}
    F -->|"是"| J["continue 跳过该步长"]
    F -->|"否"| N["追加 value 到输出 record"]
    E -->|"否"| G{"prevBuf 可用?<br/>prevBuf.time ∈ [ts-lookBackDelta, ts]<br/>且 prevBuf.value 非 stale NaN"}
    G -->|"是"| L["追加 prevBuf.value（跨 record 填充）"]
    G -->|"否"| J
    L --> N
```

**代码位置**：`engine/prom_instant_vector_cursor.go:312-435`

```go
type floatSampler struct {
    fn      float2lFloatReduce  // 约减函数（如 floatLastReduceFunc）
    fv      appendValueFunc     // 值追加函数
    offset  int64               // 时间偏移
    prevBuf *floatColBuf        // 前一个 record 的最后一个值（用于跨 record 填充）
}
```

**核心方法 Aggregate()**：

```go
func (r *floatSampler) Aggregate(p *ReducerEndpoint, param *ReducerParams) {
    r.offset = param.offset
    inRecord, outRecord := p.InputPoint.Record, p.OutputPoint.Record
    inOrdinal, outOrdinal := p.InputPoint.Ordinal, p.OutputPoint.Ordinal
    values := inRecord.ColVals[inOrdinal].FloatValues()
    timeIdx, numStep := outRecord.ColNums()-1, len(param.intervalIndex)/2

    // instant query 特殊处理
    if param.step == 0 && param.rangeDuration > 0 {
        FilterInstantNANPoint(inRecord, outRecord)
        return
    }

    var ts int64
    for i := 0; i < numStep; i++ {
        // 跳过最后一个窗口（跨 record 时）
        if param.sameWindow && i == numStep-1 {
            continue
        }
        // 在窗口内取最后一个值
        idx, value, isNil := r.fn(&inRecord.ColVals[inOrdinal], values,
            int(param.intervalIndex[2*i]), int(param.intervalIndex[2*i+1]))

        if param.step > 0 {
            ts = param.firstStep + int64(i)*param.step
            // 在 record 之间填充（使用前一个 record 的最后一个值）
            if i == 0 && !r.prevBuf.isNil {
                nextStep, lastStep := int64(r.prevBuf.index)+param.step, param.firstStep-param.step
                if nextStep <= lastStep {
                    r.PopulateByPrevious(outRecord, param, nextStep, lastStep, outOrdinal, timeIdx)
                }
            }
        } else {
            ts = inRecord.Time(idx)
        }

        if !isNil {
            if model.IsStaleNaN(value) {
                continue  // 跳过 stale NaN
            }
            r.fv(outRecord.Column(outOrdinal), value)
            if outOrdinal == 0 {
                outRecord.AppendTime(ts + r.offset)
            }
        } else {
            // 窗口内没有数据，使用 lookback 填充
            if r.prevBuf.isNil || r.prevBuf.time < ts-param.lookBackDelta || r.prevBuf.time > ts || model.IsStaleNaN(r.prevBuf.value) {
                continue
            }
            r.fv(outRecord.Column(outOrdinal), r.prevBuf.value)
            if outOrdinal == 0 {
                outRecord.AppendTime(ts + r.offset)
            }
        }
    }

    // get the last row of each record as the latest point and assign the value to prev.
    idx, value, isNil := r.fn(&inRecord.ColVals[inOrdinal], values, 0, inRecord.RowNums())
    if !isNil {
        r.prevBuf.set(int(param.firstStep+int64(numStep-1)*param.step), inRecord.Time(idx), value)
    }
    if param.step > 0 && param.lastRec && !r.prevBuf.isNil {
        if len(param.intervalIndex) == 0 {
            return
        }
        // pad after the last rec.
        nextStep := ts + param.step
        if nextStep <= param.lastStep {
            r.PopulateByPrevious(outRecord, param, nextStep, param.lastStep, outOrdinal, timeIdx)
        }
    }
    if param.lastRec {
        // reset after processing the last rec.
        r.reset()
    }
}
```

**通俗解释**：

`floatSampler` 实现了 Prometheus 的 lookback 采样语义：

1. **正常采样**：在每个步长时间窗口 `[step - lookUpDelta, step]` 内取最后一个值
2. **Lookback 填充**：如果窗口内没有数据，但 `prevBuf`（前一个窗口的值）在 lookback 范围内（`prevBuf.time >= ts - lookBackDelta`），使用 `prevBuf` 的值
3. **Stale NaN 处理**：`model.IsStaleNaN(value)` 检测 stale 标记，遇到则跳过
4. **跨 Record 填充**：当数据分布在多个 record 中时，使用 `prevBuf` 在 record 之间进行填充
5. **record 末尾填充**：最后一个 record 处理完后，需要继续填充到 `lastStep`

### 9.4 Stale NaN 处理

```go
func FilterInstantNANPoint(rec, outRecord *record.Record) *record.Record {
    rowNum := rec.RowNums()
    vals := rec.ColVals[0].FloatValues()
    var startIndex, endIndex int
    for startIndex, endIndex = 0, 0; endIndex < rowNum; {
        if model.IsStaleNaN(vals[endIndex]) {
            // 遇到 stale NaN，将之前的数据段追加到输出，然后跳过 NaN
            outRecord.AppendRec(rec, startIndex, endIndex)
            endIndex++
            startIndex = endIndex
            continue
        }
        endIndex++
    }
    // 追加最后一个数据段
    outRecord.AppendRec(rec, startIndex, endIndex)
    return outRecord
}
```

**通俗解释**：Stale NaN 是 Prometheus 用于标记序列"过期"的特殊值（`math.Float64frombits(0x7FF0000000000002)`）。当遇到 stale NaN 时，需要将其前后的数据分段处理，stale NaN 本身不参与计算。

---

## 10. RangeVectorCursor

### 10.1 结构与创建

```mermaid
sequenceDiagram
    participant Cursor as RangeVectorCursor
    participant Data as 数据点
    participant Window as 滑动窗口
    participant Func as PromFunction

    Note over Data: 原始数据: (t1,v1) (t2,v2) (t3,v3) (t4,v4) (t5,v5)
    Note over Cursor: rangeDuration=5m, step=1m

    Cursor->>Window: step1: 收集 [t1-5m, t1] 范围内的所有点
    Window-->>Func: [points in window] → rate()
    Func-->>Cursor: result1

    Cursor->>Window: step2: 收集 [t2-5m, t2] 范围内的所有点
    Window-->>Func: [points in window] → rate()
    Func-->>Cursor: result2

    Cursor->>Window: step3: 收集 [t3-5m, t3] 范围内的所有点
    Window-->>Func: [points in window] → rate()
    Func-->>Cursor: result3

    Note over Cursor: 每个步长独立计算，窗口随步长滑动
```

**代码位置**：`engine/prom_range_vector_cursor.go:35-71`

```go
// RangeVectorCursor 用于处理带有范围时长的 PromQL 函数计算。
// 这是一种滑动窗口计算。
// 对于聚合查询，替换 aggregateCursor。
//
// 示例：avg_over_time(value[3]) start=1, end=6, step=2
// rangeDuration=3, startSample=1, endSample=5
// original data: (time, value)=> [(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (6, 6.0)]
// interval index: [start, end) => [[0, 1), [0, 3), [1, 4)]
// grouped data:   (time, value)=> [[(1, 1.0)], [(1, 1.0), (2, 2.0), (3, 3.0)], [(2, 2.0), (3, 3.0), (4, 4.0)]]
// aggregated:     (time, value)=> [(1, 1.0), (3, 2.0), (5, 3.0)]
type RangeVectorCursor struct {
    aggregateCursor

    lookUpDelta   int64  // 回溯窗口
    rangeDuration int64  // 范围时长（如 [5m] 的 5m）
    start         int64  // 查询起始时间
    end           int64  // 查询结束时间
    offset        int64  // 查询偏移量
    step          int64  // 步长
    startSample   int64  // 采样起始时间
    endSample     int64  // 采样结束时间
    firstStep     int64  // 当前记录的第一个步长时间
}
```

**通俗解释**：

`RangeVectorCursor` 与 `InstantVectorCursor` 的关键区别：
- **InstantVectorCursor**：每个步长时间点只取一个值（窗口内的最后一个值）
- **RangeVectorCursor**：每个步长时间点取整个窗口内的所有值（用于 rate、avg_over_time 等计算）

### 10.2 滑动窗口索引

```mermaid
graph LR
    subgraph "InstantVector 即时向量"
        A1["数据点: t1 t3 t5 t7 t9"] --> A2["步长: t2 t4 t6 t8"]
        A2 --> A3["每个步长: 1个采样点<br/>lookBackDelta 填补空隙"]
        A3 --> A4["结果: 每步一个值"]
    end

    subgraph "RangeVector 范围向量"
        B1["数据点: t1 t3 t5 t7 t9"] --> B2["步长: t6 t8"]
        B2 --> B3["每个步长: range=5m 窗口<br/>收集窗口内所有点"]
        B3 --> B4["结果: 每步一组值<br/>喂给 rate/avg 等函数"]
    end
```

**代码位置**：`engine/prom_range_vector_cursor.go:118-153`

```go
// getIntervalIndex 使用双指针算法快速找到目标步长的起止位置
// intervalIndex 存储每个分组的区间，左闭右开，例如 [[0, 2), [1, 3)]
func (c *RangeVectorCursor) getIntervalIndex(record *record.Record) {
    if record.RowNums() == 0 {
        return
    }
    if c.step == 0 {
        // instant query：整个记录作为一个区间
        c.firstStep = c.startSample
        c.intervalIndex = append(c.intervalIndex, 0, uint16(record.RowNums()))
        return
    }
    times := record.Times()
    firstStep := getCurrStep(c.startSample, c.endSample, c.step, times[0])
    lastStep := getCurrStep(c.startSample, c.endSample, c.step, times[len(times)-1])
    c.firstStep = firstStep
    var i, j int
    for end := firstStep; end <= lastStep; end += c.step {
        start := end - c.rangeDuration  // 滑动窗口起点
        for i < len(times) {
            if times[i] >= start {
                c.intervalIndex = append(c.intervalIndex, uint16(i))
                break
            }
            i++
        }
        for j < len(times) {
            if times[j] > end {
                c.intervalIndex = append(c.intervalIndex, uint16(j))
                break
            }
            j++
        }
        if len(c.intervalIndex)%2 != 0 {
            c.intervalIndex = append(c.intervalIndex, uint16(record.RowNums()))
        }
    }
}
```

### 10.3 FilterRangeNANPoint

**代码位置**：`engine/prom_range_vector_cursor.go:174-198`

```go
func FilterRangeNANPoint(rec *record.Record) *record.Record {
    rowNum := rec.RowNums()
    var outRecord *record.Record
    vals := rec.ColVals[0].FloatValues()
    var startIndex, endIndex int
    for startIndex, endIndex = 0, 0; endIndex < rowNum; {
        if model.IsStaleNaN(vals[endIndex]) {
            if outRecord == nil {
                outRecord = record.NewRecordBuilder(rec.Schema)
                outRecord.RecMeta = rec.RecMeta
            }
            // 在 stale NaN 处分段
            outRecord.AppendRec(rec, startIndex, endIndex)
            endIndex++
            startIndex = endIndex
            continue
        }
        endIndex++
    }
    if startIndex == 0 {
        return rec  // 没有 stale NaN，直接返回原 record
    }
    outRecord.AppendRec(rec, startIndex, endIndex)
    return outRecord
}
```

**通俗解释**：

`FilterRangeNANPoint` 在 `reduce` 方法的最开始被调用，过滤掉 stale NaN 点。对于 RangeVectorCursor，stale NaN 的处理更为重要——因为在滑动窗口中，一个 stale NaN 可能会污染整个窗口的计算结果。

---

## 11. HTTP Handler 层

### 11.1 路由注册

**代码位置**：`lib/util/lifted/influx/httpd/handler.go:331-454`

```go
		{
			"prometheus-write", // Prometheus remote write
			"POST", "/api/v1/write", false, true, h.servePromWrite,
		},
		{
			"prometheus-read", // Prometheus remote read
			"POST", "/api/v1/read", true, true, h.servePromRead,
		},
		{
			"prometheus-read", // Prometheus remote read
			"GET", "/api/v1/read", true, true, h.servePromRead,
		},
		{
			"prometheus-instant-query", // Prometheus instant query
			"GET", "/api/v1/query", true, true, h.servePromQuery,
		},
		{
			"prometheus-instant-query", // Prometheus instant query
			"POST", "/api/v1/query", true, true, h.servePromQuery,
		},
		...
		{
			"prometheus-range-query", // Prometheus range query
			"GET", "/api/v1/query_range", true, true, h.servePromQueryRange,
		},
		{
			"prometheus-range-query", // Prometheus range query
			"POST", "/api/v1/query_range", true, true, h.servePromQueryRange,
		},
		...
		{
			"prometheus-labels-query", // Prometheus labels query
			"GET", "/api/v1/labels", true, true, h.servePromQueryLabels,
		},
		...
		{
			"prometheus-write-metric-store", // Prometheus remote write
			"POST", "/prometheus/{metric_store}/api/v1/write", false, true, h.servePromWriteWithMetricStore,
		},
		...
		{
			"prometheus-instant-query-metric-store", // Prometheus instant query
			"GET", "/prometheus/{metric_store}/api/v1/query", true, true, h.servePromQueryWithMetricStore,
		},
		...
		{
			"prometheus-range-query-metric-store", // Prometheus range query
			"POST", "/prometheus/{metric_store}/api/v1/query_range", true, true, h.servePromQueryRangeWithMetricStore,
		},
		...
```

**通俗解释**：

openGemini 暴露了完整的 Prometheus HTTP API，包括：
- **查询端点**：`/api/v1/query`（即时查询）、`/api/v1/query_range`（范围查询）
- **元数据端点**：`/api/v1/labels`、`/api/v1/label/{name}/values`、`/api/v1/series`、`/api/v1/metadata`
- **读写端点**：`/api/v1/write`（Remote Write）、`/api/v1/read`（Remote Read）
- **MetricStore 端点**：`/prometheus/{metric_store}/api/v1/...`（使用独立的 measurement 存储）

### 11.2 查询处理流程 servePromBaseQuery()

```mermaid
sequenceDiagram
    participant Client as 客户端
    participant Handler as HTTP Handler
    participant Stats as 统计计数器
    participant Meta as MetaClient
    participant Cache as ResultsCache
    participant Exec as execQuery()
    participant Parser as PromQL Parser
    participant Trans as Transpiler
    participant Engine as 查询引擎
    participant Recv as Receiver

    Client->>Handler: GET /api/v1/query?query=rate(http_requests_total[5m])&time=...
    Handler->>Stats: handlerStat.QueryRequests.Incr()
    Handler->>Meta: getDbRpByProm() 解析数据库名
    Meta-->>Handler: db, rp
    Handler->>Handler: 创建超时 Context
    Note over Handler,Cache: instant query（Evaluation != nil）跳过结果缓存；仅 range query 进入缓存分支
    Handler->>Exec: execQuery()

    rect rgb(230, 245, 255)
        Note over Exec,Recv: execQuery 核心流程
        Exec->>Parser: parser.ParseExpr(query)
        Parser-->>Exec: PromQL AST
        Exec->>Trans: Transpiler.Transpile(expr)
        Trans-->>Exec: InfluxQL SelectStatement
        Exec->>Engine: ExecuteQuery(q, opts, closing, qDuration)
        Engine-->>Exec: query.Result
        Exec->>Recv: getPromResult() → InfluxResultToPromQLValue()
        Recv-->>Exec: PromData
    end

    Exec-->>Handler: PromQueryResponse
    Handler-->>Client: JSON 响应 {status: "success", data: ...}
```

**代码位置**：`lib/util/lifted/influx/httpd/handler_prom.go:562-674`

```go
func (h *Handler) servePromBaseQuery(w http.ResponseWriter, r *http.Request, user meta2.User, p *promQueryParam) {
    handlerStat.QueryRequests.Incr()
    handlerStat.ActiveQueryRequests.Incr()
    start := time.Now()
    defer func() {
        handlerStat.ActiveQueryRequests.Decr()
        handlerStat.QueryRequestDuration.AddSinceNano(start)
    }()

    // 步骤1：获取数据库信息
    db, rp := getDbRpByProm(h, r)

    // 步骤2：超时控制
    ctx := r.Context()
    if to := r.FormValue("timeout"); to != "" {
        var cancel context.CancelFunc
        timeout, err := parseDuration(to)
        if err != nil {
            invalidParamError(w, err, "timeout")
            return
        }
        ctx, cancel = context.WithTimeout(ctx, timeout)
        defer cancel()
    }

    // 步骤3：解析查询参数为 PromCommand
    promCommand, ok := p.getQueryCmd(r, w, p.mst)
    if !ok {
        return
    }
    promCommand.Database, promCommand.RetentionPolicy, promCommand.Measurement = db, rp, p.mst

    // 步骤4：结果缓存（仅 range query）
    if h.Config.ResultCache.Enabled && promCommand.Evaluation == nil && !async && !isExplain {
        cmd := AlignWithStep(promCommand)
        key := generateCacheKey(cmd, p.mst, h.ResultCache.SplitQueriesByInterval)
        // 尝试转发到对应节点
        if doForward(h, r, key, rw, bodyBytes) { return }
        // 查询缓存
        resp, useCache, err := h.ResultCache.Do(reqInfo, cmd, key)
        if useCache {
            // 缓存命中，直接返回
            rw.WritePromResponse(resp)
            return
        }
    }

    // 步骤5：执行查询
    resp, apiErr, isRespond := h.execQuery(rw, r, user, start, promCommand, async, isExplain)
    if apiErr != nil {
        respondError(rw, apiErr)
        return
    }
    rw.WritePromResponse(resp)
}
```

### 11.3 execQuery() 核心执行逻辑

```mermaid
graph TD
    A["execQuery()"] --> B["parser.ParseExpr(query)<br/>解析 PromQL 为 AST"]
    B --> C["创建 Transpiler<br/>设置 PromCommand 参数"]
    C --> D["transpiler.Transpile(expr)<br/>递归转译 AST"]
    D --> E{"转译结果类型?"}
    E -->|"*influxql.SelectStatement"| F["QueryExecutor.ExecuteQuery()<br/>执行 InfluxQL 查询"]
    E -->|"Call/BinaryExpr/字面量"| G["promExprQuery()<br/>纯表达式求值"]
    E -->|"default"| H["返回 errorBadData"]
    F --> I["getPromResult()<br/>Receiver.InfluxResultToPromQLValue()"]
    G --> J["构建 PromQueryResponse"]
    H --> J
    I --> J
    J --> K["返回 JSON 响应"]
```

**代码位置**：`lib/util/lifted/influx/httpd/handler_prom.go:769-929`

```go
func (h *Handler) execQuery(w ResponseWriter, r *http.Request, user meta2.User, start time.Time, promCommand promql2influxql.PromCommand, async, isExplain bool) (resp *promql2influxql.PromQueryResponse, apiErr *apiError, isRespond bool) {
	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	// Get the query parameters db and epoch from the query request.
	db, rp := getDbRpByProm(h, r)

	// Use the native parser of Prometheus to parse and generate AST.
	expr, err := parser.ParseExpr(r.FormValue("query"))
	if err != nil {
		apiErr = &apiError{
			errorBadData, fmt.Errorf("invalid parameter %q: %w", "query", err),
		}
		return
	}

	// Transpiler as the key converter for promql2influxql
	transpiler := &promql2influxql.Transpiler{
		PromCommand: promCommand,
	}
	nodes, err := transpiler.Transpile(expr)
	if err != nil {
		if IsErrWithEmptyResp(err) {
			resp = CreatePromEmptyResp(expr, &promCommand)
			return
		}
		apiErr = &apiError{errorBadData, err}
		return
	}

	var q *influxql.Query
	switch statement := nodes.(type) {
	case *influxql.SelectStatement:
		if !isExplain {
			q = &influxql.Query{Statements: []influxql.Statement{statement}}
		} else {
			q = &influxql.Query{Statements: []influxql.Statement{&influxql.ExplainStatement{Statement: statement, Analyze: true}}}
		}
	case *influxql.Call, *influxql.BinaryExpr, *influxql.IntegerLiteral, *influxql.NumberLiteral, *influxql.StringLiteral, *influxql.ParenExpr:
		resp, apiErr = h.promExprQuery(&promCommand, statement, expr.Type())
		return
	default:
		apiErr = &apiError{errorBadData, fmt.Errorf("invalid the select statement for promql")}
		return
	}

	// ...
	chunked, chunkSize, innerChunkSize, err := h.parseChunkSize(r)
	if err != nil {
		apiErr = &apiError{errorBadData, err}
		return
	}

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: rp,
		ChunkSize:       chunkSize,
		Chunked:         chunked,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		InnerChunkSize:  innerChunkSize,
		ParallelQuery:   atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
		Quiet:           true,
		Authorizer:      h.getAuthorizer(user),
		IsPromQuery:     true,
	}

	// ...
	resultCh := h.QueryExecutor.ExecuteQuery(q, opts, closing, qDuration)

	if async {
		go h.async(q, resultCh)
		h.writeHeader(w, http.StatusNoContent)
		isRespond = true
		return
	}

	// ...
	stmtID2Result := make(map[int]*query.Result)
	for result := range resultCh {
		// Ignore nil results.
		if result == nil {
			continue
		}
		if !h.updateStmtId2Result(result, stmtID2Result) {
			continue
		}
	}

	resp, apiErr = h.getPromResult(stmtID2Result, expr, promCommand, transpiler.DropMetric(), transpiler.DuplicateResult())

	return
}
```

**通俗解释**：

`execQuery()` 是整个查询处理的核心流程：

1. **解析 PromQL**：使用 Prometheus 原生的 `parser.ParseExpr()` 解析查询语句
2. **转译为 InfluxQL**：通过 `Transpiler.Transpile()` 将 PromQL AST 转为 InfluxQL AST
3. **分发执行**：根据转译结果类型，选择不同的执行路径
4. **执行查询**：通过 `QueryExecutor.ExecuteQuery()` 执行 InfluxQL 查询
5. **结果转换**：通过 `Receiver.InfluxResultToPromQLValue()` 将 InfluxQL 结果转为 PromQL 响应

### 11.4 写入处理 servePromWriteBase()

**代码位置**：`lib/util/lifted/influx/httpd/handler_prom.go:116-215`

```go
func (h *Handler) servePromWriteBase(w http.ResponseWriter, r *http.Request, user meta2.User, mst string, tansFunc timeSeries2RowsFunc) {
	// ...
	if syscontrol.DisableWrites {
		h.httpError(w, `disable write!`, http.StatusForbidden)
		h.Logger.Error("write is forbidden!", zap.Bool("DisableWrites", syscontrol.DisableWrites))
		return
	}

	db, rp := getDbRpByProm(h, r)
	// ...

	err := Parser.Parse(body, false, func(tss []prompb2.TimeSeries) error {
		var maxPoints int
		var err error
		inValidTs, partialErr := h.FilterInvalidTimeSeries(mst, tss)
		for i, ts := range tss {
			if inValidTs[i] {
				continue
			}
			maxPoints += len(ts.Samples)
		}
		if maxPoints == 0 {
			if partialErr != nil {
				return partialErr
			}
			return ErrNoSamples
		}
		rs := pool.GetRows(maxPoints)
		*rs = (*rs)[:maxPoints]
		defer pool.PutRows(rs)
		*rs, err = tansFunc(mst, *rs, tss, inValidTs)
		if err != nil {
			h.httpError(w, err.Error(), http.StatusBadRequest)
			return err
		}

		if err = h.PointsWriter.RetryWritePointRows(db, rp, *rs); influxdb.IsClientError(err) {
			h.httpError(w, err.Error(), http.StatusBadRequest)
		} else if influxdb.IsAuthorizationError(err) {
			h.httpError(w, err.Error(), http.StatusForbidden)
		} else if err != nil {
			h.httpError(w, err.Error(), http.StatusInternalServerError)
		} else if partialErr != nil {
			h.Logger.Error("servePromWriteBase partial error:", zap.Error(partialErr))
			h.httpError(w, partialErr.Error(), http.StatusBadRequest)
		}
		return err
	})

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		h.Logger.Error("servePromWriteBase error", zap.Error(err))
		return
	}
	h.writeHeader(w, http.StatusNoContent)
}
```

**通俗解释**：

写入流程的关键点：
- 使用 **VictoriaMetrics 的流式解析器**（`Parser.Parse`）处理 Remote Write 的 Protobuf 数据
- **流式处理**：解析器是流式的，不需要一次性将整个请求体读入内存
- **timeSeries2Rows**：将 Prometheus 的 `TimeSeries` 转换为 InfluxDB 的 `Row` 格式
- **验证**：`FilterInvalidTimeSeries` 进行写入验证（标签数量、值范围等）

### 11.5 结果缓存与一致性哈希

**代码位置**：`lib/util/lifted/influx/httpd/results_cache.go:43-103`

```go
type ResultsCache struct {
	SplitQueriesByInterval time.Duration
	MaxCacheFreshness      time.Duration
	minCacheExtent         int64
	Logger                 *logger.Logger
	cache                  resultcache.ResultCache
}

func NewResultCache(logger *logger.Logger, conf config.ResultCacheConfig) *ResultsCache {
	c := resultcache.NewResultCache(conf)
	return &ResultsCache{
		Logger:                 logger,
		SplitQueriesByInterval: time.Duration(conf.SplitQueriesByInterval),
		MaxCacheFreshness:      time.Duration(conf.MaxCacheFreshness),
		minCacheExtent:         (5 * time.Minute).Milliseconds(),
		cache:                  c,
	}
}

func (rc *ResultsCache) Do(reqInfo *RequestInfo, command *promql2influxql.PromCommand, key string) (*promql2influxql.PromQueryResponse, bool, error) {
	rc.Logger.Debug("result cache key", zap.String("key", key))
	r := reqInfo.r
	if !shouldCache(r) {
		rc.Logger.Info("not enable cache", zap.String(cacheControlHeader, noStoreValue))
		return nil, false, nil
	}

	response := &promql2influxql.PromQueryResponse{}
	var extents []promql2influxql.Extent
	var err error

	maxCacheFreshness := rc.MaxCacheFreshness
	maxCacheTime := model.Now().Add(-maxCacheFreshness).UnixNano()
	if command.Start.UnixNano() > maxCacheTime {
		rc.Logger.Debug("cache miss", zap.Int64("start time", command.Start.UnixNano()))
		return nil, false, nil
	}

	fullHit := false
	cached, ok := rc.get(key)
	if ok {
		rc.Logger.Debug("hit result cache", zap.String("key", key))
		response, extents, err = rc.handleHit(reqInfo, command, cached, maxCacheTime, &fullHit)
	} else {
		rc.Logger.Debug("miss result cache", zap.String("key", key))
		response, extents, err = rc.handleMiss(reqInfo, command, maxCacheTime)
	}
	if err != nil {
		return nil, true, err
	}
	if fullHit {
		return response, true, nil
	}

	extents = rc.filterRecentExtents(extents, command.Step.Nanoseconds(), maxCacheFreshness)
	if len(extents) > 0 {
		rc.put(key, extents)
	}

	return response, true, nil
}
```

**代码位置**：`lib/resultcache/cache_factory.go:30-38`

```go
func NewResultCache(cacheConfig config.ResultCacheConfig) ResultCache {
	if cacheConfig.CacheType == config.MEM_CACHE {
		expiration := time.Duration(cacheConfig.MemCacheExpiration)
		cacheSize := int64(cacheConfig.MemCacheSize)
		statistics.NewResultCache().TotalCacheSize.Store(cacheSize)
		return &MemCacheAdapter{memCache: cache.NewCache(cacheSize, expiration)}
	}

	return nil
}
```

> `config.CacheType` 枚举包含 `MEM_CACHE`（0）与 `FILE_CACHE`（1）；当前 factory 只为 `MEM_CACHE` 创建 `MemCacheAdapter`，`FILE_CACHE` 分支落到 `return nil`，尚不可用。

**缓存转发机制**：

```go
func doForward(h *Handler, r *http.Request, key string, rw ResponseWriter, body []byte) bool {
	// only mem cache do forwarding
	if h.Config.ResultCache.CacheType != 0 {
		return false
	}
	if isForward(r) {
		return false
	}

	nodes, ring, err := buildRing(h)
	if err != nil {
		h.Logger.Error("Failed to build ring", zap.Error(err))
		return false
	}
	// If the key is handled by self
	if isSelf(ring, key) {
		return false
	}

	id := ring.Get(key)
	nodeInfo := getNodeInfo(nodes, id)
	if nodeInfo != nil && nodeInfo.Status == serf.StatusAlive {
		// ...
	}
	return false
}
```

**通俗解释**：

结果缓存的设计：
- **缓存键**：基于查询表达式、时间范围、步长生成唯一的缓存键
- **新鲜度控制**：`MaxCacheFreshness` 确保不缓存太新的数据（因为新数据可能还在写入）
- **缓存分割**：`SplitQueriesByInterval` 将大范围查询分割为多个小范围分别缓存
- **一致性哈希**：多个 ts-sql 节点之间，相同的查询始终路由到同一个节点，提高缓存命中率
- **缓存类型**：配置枚举是 `MEM_CACHE` / `FILE_CACHE`；当前 factory 只有 `MEM_CACHE` 分支，`FILE_CACHE` 不会创建可用实例，文档中不要按外部缓存实现理解

---

## 12. 结果转换 Receiver

### 12.1 Receiver 结构体

**代码位置**：`lib/util/lifted/promql2influxql/receiver.go:34-39`

```go
// Receiver 是查询结果转换器，将 InfluxQL 的执行结果转换为 Prometheus 格式
type Receiver struct {
    PromCommand                // 嵌入查询命令
    DropMetric      bool       // 是否丢弃 __name__ 标签
    DuplicateResult bool       // 是否需要复制结果
}
```

### 12.2 InfluxResultToPromQLValue() 核心转换

```mermaid
graph TD
    A["InfluxResultToPromQLValue(result, expr, cmd)"] --> B["遍历 result.Series"]
    B --> C{"分组策略?"}
    C -->|"有标签列"| D["populatePromSeriesByTag()<br/>按标签值分组"]
    C -->|"无标签列"| E["PopulatePromSeriesByHash()<br/>按完整 label set 的 hash 分组"]
    D --> F{"expr.Type()?"}
    E --> F
    F -->|"ValueTypeMatrix"| G["HandleValueTypeMatrix()<br/>构建 matrix"]
    F -->|"ValueTypeVector"| H{"cmd.DataType == GRAPH_DATA?"}
    F -->|"ValueTypeScalar"| I{"cmd.DataType == GRAPH_DATA?"}
    F -->|"default"| X["返回 errno.UnsupportedValueType"]
    H -->|"是"| G
    H -->|"否"| H1["handleValueTypeVector()"]
    I -->|"是"| G
    I -->|"否"| I1["handleValueTypeScalar()"]
    G --> M["NewPromData 返回"]
    H1 --> M
    I1 --> M
```

**代码位置**：`lib/util/lifted/promql2influxql/receiver.go:236-285`

```go
// InfluxResultToPromQLValue 将 query.Result 切片转换为 Prometheus 的 parser.Value
func (r *Receiver) InfluxResultToPromQLValue(result *query.Result, expr parser.Expr,
    cmd PromCommand) (promRes *PromData, promErr error) {
    defer func() {
        if re := recover(); re != nil {
            promRes, promErr = NewPromData(nil, ""), errno.NewError(errno.PromReceiverErr)
            logger.GetLogger().Error(promErr.Error(), zap.String("InfluxResultToPromQLValue", string(debug.Stack())))
        }
    }()
    if result == nil { return NewPromData(nil, ""), nil }
    if result.Err != nil { return NewPromData(nil, ""), result.Err }

    // 步骤1：将 InfluxQL 的 Row 转换为 Prometheus 的 Series
    var promSeries []*promql.Series
    var preMetric labels.Labels
    for _, item := range result.Series {
        if len(item.Tags) > 0 {
            // 有标签：按标签分组
            if err := r.populatePromSeriesByTag(&preMetric, &promSeries, item); err != nil {
                return NewPromData(nil, ""), errno.NewError(errno.ErrPopulatePromSeries, err.Error())
            }
        } else {
            // 无标签：按哈希分组
            if err := r.PopulatePromSeriesByHash(&promSeries, item); err != nil {
                return NewPromData(nil, ""), errno.NewError(errno.ErrGroupResultBySeries, err.Error())
            }
        }
    }

    // 步骤2：根据返回类型包装结果
    switch expr.Type() {
    case parser.ValueTypeMatrix:
        return NewPromData(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
    case parser.ValueTypeVector:
        switch cmd.DataType {
        case GRAPH_DATA:
            return NewPromData(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
        default:
            value, err := r.handleValueTypeVector(promSeries)
            return NewPromData(value, string(parser.ValueTypeVector)), err
        }
    case parser.ValueTypeScalar:
        switch cmd.DataType {
        case GRAPH_DATA:
            return NewPromData(HandleValueTypeMatrix(promSeries), string(parser.ValueTypeMatrix)), nil
        default:
            value, err := r.handleValueTypeScalar(promSeries)
            return NewPromData(value, string(parser.ValueTypeScalar)), err
        }
    default:
        return NewPromData(nil, ""), errno.NewError(errno.UnsupportedValueType, expr.Type())
    }
}
```

**通俗解释**：

`InfluxResultToPromQLValue()` 是结果转换的核心，它做两件事：

1. **数据转换**：将 InfluxQL 的 `models.Row` 转换为 Prometheus 的 `promql.Series`
   - 每个 Row 包含：columns（列名）、tags（标签）、values（数据行）
   - 每个 Series 包含：metric（标签集）、floats（FPoint 数组）

2. **类型适配**：根据查询类型（matrix/vector/scalar）和 DataType（TABLE/GRAPH）选择不同的包装方式
   - `GRAPH_DATA` + vector：实际返回 matrix 格式（Grafana 需要时间序列）
   - `TABLE_DATA` + vector：返回 vector 格式（只有一个时间点）

### 12.3 Tag 分组 vs Hash 分组

```mermaid
graph LR
    subgraph "Tag 分组（populatePromSeriesByTag）"
        T1["InfluxQL 查询结果<br/>含标签列: host, method"] --> T2["按标签值分组"]
        T2 --> T3["{host=s1, method=GET}: [points...]"]
        T2 --> T4["{host=s1, method=POST}: [points...]"]
        T2 --> T5["{host=s2, method=GET}: [points...]"]
        T3 --> T6["按 DropMetric 决定是否保留 __name__<br/>生成完整 metric 标识"]
        T4 --> T6
        T5 --> T6
    end

    subgraph "Hash 分组（PopulatePromSeriesByHash）"
        H1["InfluxQL 查询结果<br/>无标签列"] --> H2["计算完整 label set 的 metric.Hash()"]
        H2 --> H3["hash(label set): [points...]"]
        H3 --> H4["按 DropMetric 决定是否丢弃 __name__<br/>series 仍保存 Metric labels"]
    end
```

**代码位置**：`lib/util/lifted/promql2influxql/receiver.go:69-153`

```go
func (r *Receiver) populatePromSeriesByTag(pPreMetric *labels.Labels, promSeries *[]*promql.Series, table *models.Row) error {
	var metric labels.Labels
	if !r.DropMetric {
		metric = labels.FromMap(table.Tags)
	} else {
		metric = FromMapWithoutMetric(table.Tags)
	}
	if *pPreMetric != nil && labels.Equal(metric, *pPreMetric) {
		logger.GetLogger().Error(fmt.Sprintf("vector cannot contain metrics with the same labelset: metric=%v", metric.String()))
		return fmt.Errorf("vector cannot contain metrics with the same labelset")
	}
	*pPreMetric = metric

	if len(table.Columns) <= SampleColNum {
		var points []promql.FPoint
		preT := int64(math.MinInt64)
		for _, row := range table.Values {
			point, err := Row2Point(row)
			if err != nil {
				return err
			}
			if preT >= point.T {
				logger.GetLogger().Error(fmt.Sprintf("vector cannot contain metrics with the same labelset: metric=%v point=%v preT=%v", metric.String(), point.String(), preT))
				return fmt.Errorf("vector cannot contain metrics with the same labelset")
			}
			preT = point.T
			points = append(points, point)
		}
		*promSeries = append(*promSeries, &promql.Series{
			Metric: metric,
			Floats: points,
		})
	} else {
		seriesMap := make(map[uint64]*promql.Series)
		metricContainsMap := metricContainsCol(table.Columns, metric)
		for _, row := range table.Values {
			m := metric
			for i := SampleColNum; i < len(table.Columns); i++ {
				if row[i] == nil || (r.DropMetric && DefaultMetricKeyLabel == table.Columns[i]) {
					continue
				}
				if !metricContainsMap[table.Columns[i]] {
					m = append(m, labels.Label{Name: table.Columns[i], Value: row[i].(string)})
				}
			}
			point, err := Row2Point(row)
			if err != nil {
				return err
			}
			sort.Sort(m)
			if series, exists := seriesMap[m.Hash()]; exists {
				if series.Floats[len(series.Floats)-1].T == point.T {
					logger.GetLogger().Error(fmt.Sprintf("vector cannot contain metrics with the same labelset: metric=%v point=%v", metric.String(), point.String()))
					return fmt.Errorf("vector cannot contain metrics with the same labelset")
				}
				series.Floats = append(series.Floats, point)
			} else {
				seriesMap[m.Hash()] = &promql.Series{
					Metric: m,
					Floats: []promql.FPoint{
						point,
					},
				}
			}
		}
		for _, series := range seriesMap {
			*promSeries = append(*promSeries, series)
		}
	}
	if !r.DuplicateResult || r.PromCommand.Step == 0 || len((*promSeries)[len(*promSeries)-1].Floats) > 1 {
		return nil
	}
	// For every evaluation while the value remains same, the timestamp for that
	// value would change for different eval times. Hence we duplicate the result
	// with changed timestamps.
	start, end, interval := r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds()
	series := (*promSeries)[len(*promSeries)-1]
	idx := len(series.Floats)
	ReservePoints(series, int((end-start-interval)/interval)+1)
	for ts := start + interval; ts <= end; ts += interval {
		series.Floats[idx] = promql.FPoint{T: ts, F: series.Floats[0].F}
		idx++
	}
	return nil
}
```

**通俗解释**：

两种分组策略适用于不同的查询场景：

1. **Tag 分组**（`populatePromSeriesByTag`）：当查询带有 `GROUP BY` 时，InfluxQL 会返回带标签的结果。每个标签组合对应一个时间序列。
   - 简单情况：只有 `time` 和 `value` 两列，所有数据属于同一个序列
   - 复杂情况：有多列标签，需要按标签组合分组，使用 `map[uint64]*promql.Series`（标签哈希作为键）

2. **Hash 分组**（`PopulatePromSeriesByHash`）：当查询没有 `GROUP BY` 时，所有数据的标签都在额外的列中。需要遍历所有行，提取标签并按哈希分组。

### 12.4 Row2Point 转换

**代码位置**：`lib/util/lifted/promql2influxql/receiver.go:510-535`

```go
func Row2Point(row []interface{}) (promql.FPoint, error) {
    // 第一列是时间
    ts, ok := row[0].(time.Time)
    if !ok {
        return promql.FPoint{}, errno.NewError(errno.ParseTimeFail)
    }
    point := promql.FPoint{
        T: timestamp.FromTime(ts),  // 转为毫秒时间戳
    }
    // 第二列是值
    switch number := row[1].(type) {
    case json.Number:
        if v, err := number.Float64(); err == nil {
            point.F = v
        } else if v, err := number.Int64(); err == nil {
            point.F = float64(v)
        }
    case float64:
        point.F = number
    case int64:
        point.F = float64(number)
    default:
        return promql.FPoint{}, fmt.Errorf("invalid the data type for prom response")
    }
    return point, nil
}
```

### 12.5 PromData 序列化

**代码位置**：`lib/util/lifted/promql2influxql/models.go:99-115`

```go
type PromQueryResponse struct {
    Status    string    `json:"status"`
    Data      *PromData `json:"data,omitempty"`
    ErrorType string    `json:"errorType,omitempty"`
    Error     string    `json:"error,omitempty"`
}

type PromData struct {
    ResultType string         `json:"resultType,omitempty"`
    Result     PromDataResult `json:"result,omitempty"`
}
```

`PromData` 实现了 `msgp.Encodable` 和 `msgp.Decodable` 接口，支持高效的 MessagePack 序列化，用于结果缓存。

---

## 13. 潜在隐患

### 13.1 AST 转译开销

**风险**：每次查询都需要：
1. Prometheus 原生解析器解析 PromQL -> AST
2. Transpiler 将 PromQL AST -> InfluxQL AST
3. InfluxQL 引擎基于 InfluxQL AST 构建执行计划（不再进行 InfluxQL 文本解析）

**影响**：PromQL 解析 + AST 转译 + 计划构建带来额外的 CPU 和内存开销。对于高频查询场景，这个开销可能不可忽视。注意 execQuery 是直接把 Transpiler 生成的 `*influxql.SelectStatement` 包进 `influxql.Query` 交给 ExecuteQuery，并不存在“第二次文本解析”。

**缓解措施**：
- 结果缓存机制减少了重复查询的转译次数
- Transpiler 实例在使用后被 GC 回收，不会长期占用内存

### 13.2 Stale NaN 边界情况

**风险**：Stale NaN 的处理分布在多个层级：
- `FilterInstantNANPoint`：在 InstantVectorCursor 层过滤
- `FilterRangeNANPoint`：在 RangeVectorCursor 层过滤
- `model.IsStaleNaN`：在采样器中检查

**潜在问题**：
- 在 record 边界处的 stale NaN 可能导致跨 record 的数据段被错误分隔
- `floatSampler.PopulateByPrevious` 中的 stale NaN 检查可能遗漏某些边界情况

### 13.3 结果缓存失效

**风险**：
- 缓存新鲜度（`MaxCacheFreshness`）的配置如果不当，可能导致返回过期数据
- 一致性哈希在节点扩缩容时可能导致缓存命中率下降
- 缓存分割（`SplitQueriesByInterval`）可能导致部分缓存和完整查询的结果不一致

### 13.4 函数名冲突风险

**风险**：`_prom` 后缀约定避免了大部分命名冲突，但仍有隐患：
- `sum_over_time`、`avg_over_time`、`count_over_time` 等没有后缀的函数，如果 InfluxQL 原生也定义了同名函数，会产生冲突
- 新增的 PromQL 函数如果没有正确添加后缀，可能导致行为不一致

### 13.5 向量-向量二元运算的不完整性

**代码位置**：`lib/util/lifted/promql2influxql/binary_expr.go:271-275`

```go
} else if !lok || !rok {
    // todo: vector(with mst)+vector(without mst)
    t.dropMetric = false
    return t.transpileBinOpOfOneVector(b, influxql.Token(b.Op), lhs, rhs)
}
```

**风险**：TODO 注释表明 "向量（有 measurement）+ 向量（无 measurement）" 的复杂二元运算尚未完全实现。也就是说，`vector(with mst)+vector(without mst)` 这类混合来源场景会落入 `transpileBinOpOfOneVector` 的折中路径，标签匹配和结果 metric 语义可能不完整。

### 13.6 panic 风险

**代码位置**：`lib/util/lifted/promql2influxql/aggregate_expr.go:97`

```go
if len(statement.Sources) != 1 {
    panic("the number of source should be 1 for promql agg query")
}
```

**风险**：代码中存在 `panic` 调用，如果 Sources 数量不是 1，整个进程会崩溃。虽然理论上不应该出现这种情况，但在边界条件下可能导致严重问题。

---

## 14. 端到端实战

### 14.1 完整生命周期：rate(http_requests_total[5m])

以下是一个 PromQL 查询从 HTTP 请求到响应的完整生命周期：

#### 阶段 1：HTTP 请求接收

```
POST /api/v1/query
Content-Type: application/x-www-form-urlencoded

query=rate(http_requests_total{method="GET"}[5m])&time=1700000000
```

`Handler.servePromBaseQuery()` 被调用。

#### 阶段 2：解析查询参数

```go
promCommand := PromCommand{
    Cmd:         `rate(http_requests_total{method="GET"}[5m])`,
    Database:    "prom",
    Evaluation:  time.Unix(1700000000, 0),      // instant query（time 参数为秒）
    Step:        0,                            // instant query 无步长
    LookBackDelta: 5 * time.Minute,
    DataType:    TABLE_DATA,
    ValueFieldKey: "value",
}
```

#### 阶段 3：PromQL 解析

```go
expr, _ := parser.ParseExpr(`rate(http_requests_total{method="GET"}[5m])`)
// expr 类型: *parser.Call
// expr.Func.Name = "rate"
// expr.Args[0] = &parser.MatrixSelector{
//     Range: 5 * time.Minute,
//     VectorSelector: &parser.VectorSelector{
//         Name: "http_requests_total",
//         LabelMatchers: [{Name: "method", Value: "GET", Type: MatchEqual}],
//     },
// }
```

#### 阶段 4：AST 预处理

```go
transpiler := &Transpiler{PromCommand: promCommand}
s := transpiler.newEvalStmt(expr)
// s.Start = s.End = Evaluation
// s.Interval = 0 (instant query)
transpiler.rewriteMinMaxTime()
// minT = maxT = 1700000000000 (毫秒)
setOffsetForAtModifier(minT, s.Expr)
```

#### 阶段 5：递归转译

```
transpileExpr(Call{rate, [MatrixSelector]})
  -> transpileCall(rate)
    -> transpileExpr(MatrixSelector{5m, VectorSelector{...}})
      -> transpileRangeVectorSelector(MatrixSelector)
        -> t.timeRange = 5m
        -> transpileExpr(VectorSelector{http_requests_total, method="GET"})
          -> transpileInstantVectorSelector(VectorSelector)
            -> timeCondition: time >= (1700000000000 - 300000) AND time <= 1700000000000
            -> tagCondition: method = 'GET'
            -> SelectStatement{
                Sources: [http_requests_total],
                Fields: [value],
                Dimensions: [*],
                Condition: time >= ... AND method = 'GET',
                IsPromQuery: true,
                LookBackDelta: 5m,
                QueryOffset: 0,
              }
    -> 查 rangeVectorFunctions["rate"]: name="rate_prom"
    -> dropMetric = true
    -> transpilePromFunc(rate_prom, args, setAggregateFields)
      -> table = SelectStatement, parameter = []
      -> field.Expr 是 VarRef(value), 直接设置
      -> setAggregateFields: Fields = [rate_prom(value)]
      -> instant query（step=0）：不设置 GROUP BY time()，也不设置 fill(none)
```

#### 阶段 6：生成的 InfluxQL

```sql
SELECT rate_prom(value)
FROM http_requests_total
WHERE time >= 1699999700000 AND time <= 1700000000000 AND method = 'GET'
GROUP BY *
```

#### 阶段 7：引擎执行

1. 查询规划器创建执行计划
2. 从存储层读取原始数据（record 格式）
3. 创建 `RangeVectorCursor`（range=5m>0；step=0 表示单次评估）
4. 对每个 series，`floatSliceReducer` 在 `[1700000000000 - 5m, 1700000000000]` 窗口内取样本
5. `rate_prom` 函数使用外推算法计算速率

#### 阶段 8：结果转换

```go
receiver := &Receiver{
    PromCommand: promCommand,
    DropMetric: true,  // rate 是 range vector 函数，统一 dropMetric 丢弃 __name__
}
promData, _ := receiver.InfluxResultToPromQLValue(result, expr, promCommand)
```

对于 instant query + TABLE_DATA，结果类型是 vector。

#### 阶段 9：HTTP 响应

```json
{
    "status": "success",
    "data": {
        "resultType": "vector",
        "result": [
            {
                "metric": {"method": "GET", "endpoint": "/api/users"},
                "value": [1700000000, "123.456"]
            },
            {
                "metric": {"method": "GET", "endpoint": "/api/orders"},
                "value": [1700000000, "78.901"]
            }
        ]
    }
}
```

### 14.2 完整生命周期：sum by (method)(rate(http_requests_total[5m]))

这个查询比上一个多了外层的 `sum by (method)` 聚合：

#### 转译过程

```
transpileExpr(AggregateExpr{
    Op: SUM,
    Grouping: ["method"],
    Without: false,
    Expr: Call{rate, [MatrixSelector{...}]},
})
  -> transpileAggregateExpr(AggregateExpr)
    -> dropMetric = true
    -> transpileExpr(Call{rate, ...})
      -> (同上，得到 SelectStatement with rate_prom(value))
    -> 查 aggregateFns[SUM]: name="sum"
    -> 子表达式是 Call(rate_prom)，尝试 canPushDownAggWithFunction
      -> true（getOriCallName("rate_prom")="rate" 命中 rangeVectorFunctions，
         且 statement.Range=5m>0）
    -> 聚合下推到同一条 SELECT：
      SELECT sum(rate_prom(value))
      FROM http_requests_total
      WHERE time >= 1699999700000 AND time <= 1700000000000 AND method = 'GET'
      GROUP BY method
```

#### 最终 InfluxQL

```sql
SELECT sum(rate_prom(value))
FROM http_requests_total
WHERE time >= 1699999700000 AND time <= 1700000000000 AND method = 'GET'
GROUP BY method
```

#### 响应

```json
{
    "status": "success",
    "data": {
        "resultType": "vector",
        "result": [
            {
                "metric": {"method": "GET"},
                "value": [1700000000, "202.357"]
            }
        ]
    }
}
```

### 14.3 端到端 Range Query 示例

以 `query_range` 请求为例，展示多个时间步长的完整计算过程：

#### 请求

```
GET /api/v1/query_range?query=rate(http_requests_total[5m])&start=1700000000&end=1700000300&step=60
```

#### 原始数据（存储层）

| 时间戳 (s) | 值 | 说明 |
|------------|-----|------|
| 1699997100 | 1000 | 50 分钟前 |
| 1699999700 | 1200 | 5 分钟前 |
| 1699999800 | 1250 | 4 分钟前 |
| 1699999900 | 1280 | 3 分钟前 |
| 1700000000 | 1350 | 起始时间 |
| 1700000060 | 1400 | |
| 1700000120 | 1420 | |
| 1700000180 | 1500 | |
| 1700000240 | 1550 | |
| 1700000300 | 1600 | 结束时间 |

#### Step 1: Transpiler 转译

```
输入: rate(http_requests_total[5m])
转译结果:
  SELECT rate_prom(value)
  FROM http_requests_total
  WHERE time >= 1699999700000 AND time <= 1700000300000
  GROUP BY *, time(60s, 0s) fill(none)
```

#### Step 2: 引擎执行（RangeVectorCursor 滑动窗口）

```
rangeDuration = 300s (5m), step = 60s
窗口边界: startSample=1700000000, endSample=1700000300

步长 T=1700000000 (start):
  窗口: [1700000000-300, 1700000000] = [1699999700, 1700000000]
  数据点: (1699999700, 1200), (1699999800, 1250), (1699999900, 1280), (1700000000, 1350)
  → rate_prom 计算: delta=150, sampled=300s, extrapolate=300s → rate = 0.5/s

步长 T=1700000060:
  窗口: [1699999760, 1700000060]
  数据点: (1699999800, 1250), (1699999900, 1280), (1700000000, 1350), (1700000060, 1400)
  → rate_prom 计算: delta=150, sampled=260s, extrapolate=300s → rate = 150*(300/260)/300 = 0.5769/s

步长 T=1700000120:
  窗口: [1699999820, 1700000120]
  数据点: (1699999900, 1280), (1700000000, 1350), (1700000060, 1400), (1700000120, 1420)
  → rate_prom 计算: delta=140, sampled=220s, extrapolate=300s → rate = 140*(300/220)/300 = 0.6364/s

步长 T=1700000180:
  窗口: [1699999880, 1700000180]
  数据点: (1700000000, 1350), (1700000060, 1400), (1700000120, 1420), (1700000180, 1500)
  → rate_prom 计算: delta=150, sampled=180s, extrapolate=210s → rate = 150*(210/180)/300 = 0.5833/s

步长 T=1700000240:
  窗口: [1699999940, 1700000240]
  数据点: (1700000000, 1350), (1700000060, 1400), (1700000120, 1420), (1700000180, 1500), (1700000240, 1550)
  → rate_prom 计算: delta=200, sampled=240s, extrapolate=300s → rate = 200*(300/240)/300 = 0.8333/s

步长 T=1700000300:
  窗口: [1700000000, 1700000300]
  数据点: (1700000000, 1350), (1700000060, 1400), (1700000120, 1420), (1700000180, 1500), (1700000240, 1550), (1700000300, 1600)
  → rate_prom 计算: delta=250, sampled=300s, extrapolate=300s → rate = 250*(300/300)/300 = 0.8333/s
```

#### Step 3: Receiver 转换

```
InfluxQL 结果 → Receiver.InfluxResultToPromQLValue()
  → resultType = "matrix"（range query + GRAPH_DATA）
  → rate 属于 range vector 函数，DropMetric=true，构建时间序列: metric={}（丢弃 __name__）
  → values = [[1700000000, "0.5"], [1700000060, "0.5769230769230769"], ...]
```

#### 最终响应

```json
{
    "status": "success",
    "data": {
        "resultType": "matrix",
        "result": [
            {
                "metric": {},
                "values": [
                    [1700000000, "0.5"],
                    [1700000060, "0.5769230769230769"],
                    [1700000120, "0.6363636363636364"],
                    [1700000180, "0.5833333333333334"],
                    [1700000240, "0.8333333333333334"],
                    [1700000300, "0.8333333333333334"]
                ]
            }
        ]
    }
}
```

> **观察**：6 个步长的 rate 依次为 0.5、0.577、0.636、0.583、0.833、0.833，前几步受窗口内首点距边界不同的外推影响而波动，最后两步持平。每个步长独立计算 5 分钟窗口内的增长率，步长之间的重叠使得趋势变化更平滑。

---

## 附录 A：PromQL 常用函数映射表

> 本表只列常用函数；6 张映射表的完整 70 项见附录 K.2。

| PromQL 函数 | 引擎函数名 | 函数类型 | 说明 |
|-------------|-----------|----------|------|
| `rate` | `rate_prom` | TRANSFORM_FN | 计算每秒增长率（外推） |
| `irate` | `irate_prom` | TRANSFORM_FN | 计算瞬时增长率 |
| `increase` | `increase` | TRANSFORM_FN | 计算增量（外推） |
| `delta` | `delta_prom` | TRANSFORM_FN | 计算差值 |
| `idelta` | `idelta_prom` | TRANSFORM_FN | 计算瞬时差值 |
| `deriv` | `deriv` | TRANSFORM_FN | 计算导数（线性回归） |
| `predict_linear` | `predict_linear` | TRANSFORM_FN | 线性预测 |
| `avg_over_time` | `avg_over_time` | AGGREGATE_FN | 窗口内平均值 |
| `sum_over_time` | `sum_over_time` | AGGREGATE_FN | 窗口内求和 |
| `min_over_time` | `min_over_time` | SELECTOR_FN | 窗口内最小值 |
| `max_over_time` | `max_over_time` | SELECTOR_FN | 窗口内最大值 |
| `count_over_time` | `count_over_time` | AGGREGATE_FN | 窗口内计数 |
| `last_over_time` | `last_over_time_prom` | AGGREGATE_FN | 窗口内最后一个值 |
| `stddev_over_time` | `stddev_over_time_prom` | AGGREGATE_FN | 窗口内标准差 |
| `stdvar_over_time` | `stdvar_over_time_prom` | TRANSFORM_FN | 窗口内方差 |
| `quantile_over_time` | `quantile_over_time_prom` | SELECTOR_FN | 窗口内分位数 |
| `changes` | `changes_prom` | TRANSFORM_FN | 计算变化次数 |
| `resets` | `resets_prom` | TRANSFORM_FN | 计算重置次数 |
| `holt_winters` | `holt_winters_prom` | SELECTOR_FN | Holt-Winters 平滑 |
| `absent` | `absent_prom` | AGGREGATE_FN | 检测序列缺失 |
| `absent_over_time` | `absent_over_time_prom` | AGGREGATE_FN | 检测窗口内缺失 |
| `mad_over_time` | `mad_over_time_prom` | SELECTOR_FN | 窗口内 MAD |
| `present_over_time` | `present_over_time_prom` | AGGREGATE_FN | 窗口内存在标记 |
| `histogram_quantile` | `histogram_quantile` | AGGREGATE_FN | 直方图分位数 |
| `scalar` | `scalar_prom` | AGGREGATE_FN | 向量转标量 |

## 附录 B：关键代码文件索引

| 文件路径 | 核心职责 | 行数 |
|---------|---------|------|
| `lib/util/lifted/promql2influxql/transpiler.go` | Transpiler 核心结构、递归转译入口、子查询处理 | ~619 |
| `lib/util/lifted/promql2influxql/selector.go` | VectorSelector/MatrixSelector 转译、条件生成 | ~376 |
| `lib/util/lifted/promql2influxql/call.go` | 函数调用转译、6 大函数映射表 | ~633 |
| `lib/util/lifted/promql2influxql/aggregate_expr.go` | 聚合表达式转译、by/without 维度处理 | ~373 |
| `lib/util/lifted/promql2influxql/binary_expr.go` | 二元表达式转译、向量匹配 | ~417 |
| `lib/util/lifted/promql2influxql/receiver.go` | 结果转换、PromQL 值构建 | ~572 |
| `lib/util/lifted/promql2influxql/models.go` | PromCommand、PromResponse 数据模型、msgp 序列化 | ~664 |
| `lib/util/lifted/promql2influxql/constant.go` | 常量定义 | ~39 |
| `engine/prom_functions.go` | 引擎层函数注册、rate/irate/avg 等实现 | ~731 |
| `engine/prom_instant_vector_cursor.go` | 即时向量采样器、双指针算法、lookback | ~436 |
| `engine/prom_range_vector_cursor.go` | 范围向量滑动窗口、stale NaN 过滤 | ~199 |
| `lib/util/lifted/influx/httpd/handler_prom.go` | HTTP 路由、查询执行、写入处理、结果缓存 | ~1336 |
| `lib/util/lifted/influx/httpd/results_cache.go` | 结果缓存、缓存分割、新鲜度控制 | ~442 |

## 附录 C：引擎层 PromQL 函数深入实现

### C.1 irate 函数——瞬时增长率

**代码位置**：`engine/prom_functions.go:165-169`

```go
// irate
type irateOp struct{}

func (o *irateOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatRateReducer(floatIRateReduce, floatIRateMerge(true), floatIRateUpdate), p.inOrdinal, p.outOrdinal), nil
}
```

**通俗解释**：

irate 与 rate 的核心区别：
- **rate**：使用窗口内所有数据点进行外推计算
- **irate**：只使用最后两个数据点计算瞬时速率

irate 不做外推，但仍处理 counter reset：设最后两点为 `(prevTime, prevValue)`、`(lastTime, lastValue)`，则
`resultValue = lastValue - prevValue`；若 `lastValue < prevValue`（发生 reset），`resultValue = lastValue`；最后 `rate = resultValue / ((lastTime-prevTime)/1e9)`。

irate 适合观察快速变化的指标（如请求速率的瞬时波动），但不适合用于告警规则（因为容易被毛刺影响）。

### C.2 deriv 函数——导数计算

**代码位置**：`engine/prom_functions.go:356-437`

```go
// deriv 函数：使用简单线性回归计算导数
type derivOp struct{}

func (o *derivOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
    return NewRoutineImpl(newFloatSliceReducer(floatPromDerivReduce, linearMergeFunc(true, 0)), p.inOrdinal, p.outOrdinal), nil
}

func linearMergeFunc(isDeriv bool, scalar float64) FloatSliceMergeFunc {
    return func(t1, t2 []int64, v1, v2 []float64, ts int64, pointCount int, param *ReducerParams) (float64, bool) {
        if pointCount <= 1 { return 0, true }

        var (
            n          float64
            sumX, cX   float64   // X 的 Kahan 求和
            sumY, cY   float64   // Y 的 Kahan 求和
            sumXY, cXY float64   // XY 的 Kahan 求和
            sumX2, cX2 float64   // X^2 的 Kahan 求和
            constY     bool      // Y 是否为常量
            index      int
        )
        constY = true
        interceptTime := ts + param.offset

        // 对两个数据段分别进行累加
        reduce := func(times []int64, values []float64) {
            for i, v := range values {
                index++
                if constY && index > 0 && v != fv { constY = false }
                n += 1.0
                x := float64(times[i]-interceptTime) / 1e9
                sumX, cX = executor.KahanSumInc(x, sumX, cX)
                sumY, cY = executor.KahanSumInc(v, sumY, cY)
                sumXY, cXY = executor.KahanSumInc(x*v, sumXY, cXY)
                sumX2, cX2 = executor.KahanSumInc(x*x, sumX2, cX2)
            }
        }
        reduce(t1, v1)
        reduce(t2, v2)

        if constY {
            if isDeriv { return 0, false }    // 常量的导数为 0
            return fv, false                   // predict_linear 返回常量值
        }

        // 最小二乘法计算斜率
        covXY := sumXY - sumX*sumY/n
        varX := sumX2 - sumX*sumX/n
        deriv := covXY / varX

        if isDeriv { return deriv, false }

        // predict_linear: 斜率 * t + 截距
        intercept := sumY/n - deriv*sumX/n
        return (deriv*scalar + intercept), false
    }
}
```

**通俗解释**：

deriv 使用简单线性回归（最小二乘法）计算数据的趋势斜率。核心数学：

```
斜率 = Cov(X, Y) / Var(X)
其中 X = 时间, Y = 值
```

predict_linear 复用了相同的线性回归逻辑，只是额外计算截距并进行预测：`predicted_value = slope * t + intercept`。

两者都使用 Kahan 求和来减少浮点精度损失——这在计算 X^2 等大数累加时尤为重要。

### C.3 delta / idelta 函数

**代码位置**：`engine/prom_functions.go`

```go
// delta 函数：计算窗口内首尾值的差值
type deltaOp struct{}

func (o *deltaOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
    return NewRoutineImpl(newFloatSliceReducer(floatPromRateReduce, floatPromRateMerge(false, false)), p.inOrdinal, p.outOrdinal), nil
}

// idelta 函数：计算最后两个点的差值
type ideltaOp struct{}

func (o *ideltaOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
    return NewRoutineImpl(newFloatRateReducer(floatIRateReduce, floatIRateMerge(false), floatIRateUpdate), p.inOrdinal, p.outOrdinal), nil
}
```

**通俗解释**：

- **delta**：与 rate 共享相同的 Reduce/Merge 框架，但 `isRate=false, isCounter=false`。即：进行外推，但不除以时间范围，不做 counter 处理
- **idelta**：与 irate 共享框架，但 `isCounter=false`。即：取最后两点差值，不做 counter 处理

### C.4 changes / resets 函数

```go
// changes
type changesOp struct{}

func (r *changesOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatChangesReducer, floatChangesMerger()), param.inOrdinal, param.outOrdinal), nil
}

func floatChangesReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatChangesMerger() FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c < 1 {
			return math.NaN(), true
		}
		return executor.CalcChange(prevV, curV), false
	}
}

// resets
type resetsOp struct{}

func (r *resetsOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatResetsReducer, floatResetsMerger()), param.inOrdinal, param.outOrdinal), nil
}

func floatResetsReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatResetsMerger() FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		return executor.CalcResets(prevV, curV), false
	}
}
```

**通俗解释**：

- **changes**：Reduce 只收集窗口内的数据切片，实际计数由 `executor.CalcChange(prevV, curV)` 完成（遍历相邻点对，统计值变化次数，忽略 NaN）
- **resets**：Reduce 同样只收集切片，实际计数由 `executor.CalcResets(prevV, curV)` 完成（统计 counter 重置次数，值变小，忽略 NaN）

### C.5 stddev_over_time / stdvar_over_time 函数

```go
// stdvar_over_time
type stdVarOverTime struct{}

func (r *stdVarOverTime) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatStdVarOverTimeReducer, floatStdVarOverTimeMerger(false)), param.inOrdinal, param.outOrdinal), nil
}

func floatStdVarOverTimeReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatStdVarOverTimeMerger(isStdDev bool) FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c < 1 {
			return 0, true
		}
		var count float64
		var mean, cMean float64
		var aux, cAux float64

		if len(prevV) == 0 && len(curV) == 0 {
			return 0.0, true
		}

		if len(prevV) != 0 {
			for _, f := range prevV {
				count++
				delta := f - (mean + cMean)
				mean, cMean = executor.KahanSumInc(delta/count, mean, cMean)
				aux, cAux = executor.KahanSumInc(delta*(f-(mean+cMean)), aux, cAux)
			}
		}

		if len(curV) != 0 {
			for _, f := range curV {
				count++
				delta := f - (mean + cMean)
				mean, cMean = executor.KahanSumInc(delta/count, mean, cMean)
				aux, cAux = executor.KahanSumInc(delta*(f-(mean+cMean)), aux, cAux)
			}
		}
		stdVar := (aux + cAux) / count
		if isStdDev {
			return math.Sqrt(stdVar), false
		}
		return stdVar, false
	}
}

// stddev_over_time
type stdDevOverTime struct{}

func (r *stdDevOverTime) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatStdVarOverTimeReducer, floatStdVarOverTimeMerger(true)), param.inOrdinal, param.outOrdinal), nil
}
```

**通俗解释**：

stddev/stdvar 复用同一个 merger，内部用 Welford 在线算法 + Kahan 补偿求和：
- **单遍扫描**：不需要先计算均值再扫描第二遍
- **数值稳定**：通过辅助变量 `aux` 避免大数减小数的精度损失
- **方差分母是 `count`**（总体方差，不是样本方差 `count-1`）；`isStdDev=true` 时再取 `math.Sqrt(stdVar)`
- **stddev_over_time 没有单独的 Reduce**：与 stdvar 一样使用 `floatStdVarOverTimeReducer`（只收集窗口切片），仅 merger 参数不同

### C.6 quantile_over_time 函数

```go
// quantile_over_time
type quantileOverTime struct{}

func (r *quantileOverTime) CreateRoutine(param *PromFuncParam) (Routine, error) {
	var percentile float64
	switch arg := param.args[1].(type) {
	case *influxql.NumberLiteral:
		percentile = arg.Val
	case *influxql.IntegerLiteral:
		percentile = float64(arg.Val)
	default:
		return nil, errors.New("the type of input args of quantile_prom iterator is unsupported")
	}
	return NewRoutineImpl(newFloatSliceReducer(floatStdVarOverTimeReducer, floatQuantileOverTimeMerger(percentile)), param.inOrdinal, param.outOrdinal), nil
}

func floatQuantileOverTimeMerger(percentile float64) FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c == 0 || len(prevT)+len(currT) == 0 || len(prevV)+len(curV) == 0 {
			return math.NaN(), true
		}
		return executor.CalcQuantile2(percentile, prevV, curV), false
	}
}
```

**通俗解释**：分位数值从 `param.args[1]` 解析；Reduce 复用 `floatStdVarOverTimeReducer` 收集窗口切片，Merge 直接调用 `executor.CalcQuantile2(percentile, prevV, curV)` 完成排序与插值，文档不再自行实现分位数算法。

### C.7 holt_winters 函数

```go
// holt_winters
type holtWintersOp struct{}

func (r *holtWintersOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	// The smoothing factor argument.
	var sf float64
	// The trend factor argument.
	switch arg := param.args[1].(type) {
	case *influxql.NumberLiteral:
		sf = arg.Val
	case *influxql.IntegerLiteral:
		sf = float64(arg.Val)
	default:
		return nil, errors.New("the type of input args of holtWinters sf is unsupported")
	}
	var tf float64
	switch arg2 := param.args[2].(type) {
	case *influxql.NumberLiteral:
		tf = arg2.Val
	case *influxql.IntegerLiteral:
		tf = float64(arg2.Val)
	default:
		return nil, errors.New("the type of input args of holtWinters tf is unsupported")
	}

	return NewRoutineImpl(newFloatSliceReducer(floatHoltWintersReducer, floatHoltWintersMerger(sf, tf)), param.inOrdinal, param.outOrdinal), nil
}

func floatHoltWintersReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatHoltWintersMerger(sf float64, tf float64) FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c < 2 {
			return math.NaN(), true
		}
		return executor.CalcHoltWinters(prevV, curV, sf, tf), false
	}
}
```

**通俗解释**：

Prometheus 的 `holt_winters` 本身就是双指数平滑（level + trend），没有季节性参数；openGemini 的实现与之一致：
- `args[1]` 是平滑因子 `sf`，`args[2]` 是趋势因子 `tf`
- Reduce 只收集窗口切片，Merge 调用 `executor.CalcHoltWinters(prevV, curV, sf, tf)`
- 样本数少于 2 时返回 `NaN`

### C.8 mad_over_time 函数

```go
// mad_over_time
type madOverTimeOp struct{}

func (r *madOverTimeOp) CreateRoutine(param *PromFuncParam) (Routine, error) {
	return NewRoutineImpl(newFloatSliceReducer(floatMadOverTimeReducer, floatMadOverTimeMerger()), param.inOrdinal, param.outOrdinal), nil
}

func floatMadOverTimeReducer(times []int64, values []float64, start int, end int) ([]int64, []float64, bool) {
	if start >= end {
		return []int64{}, []float64{}, true
	}
	return times[start:end], values[start:end], false
}

func floatMadOverTimeMerger() FloatSliceMergeFunc {
	return func(prevT []int64, currT []int64, prevV []float64, curV []float64, ts int64, c int, param *ReducerParams) (float64, bool) {
		if c == 0 || len(prevT)+len(currT) == 0 || len(prevV)+len(curV) == 0 {
			return math.NaN(), true
		}
		return executor.CalcMad2(prevV, curV), false
	}
}
```

**通俗解释**：

MAD（Median Absolute Deviation）是一种鲁棒的离散度量：
1. 计算窗口内所有值的中位数 `median`
2. 计算每个值与中位数的绝对偏差 `|value - median|`
3. 取这些偏差的中位数作为 MAD 值

MAD 比标准差更鲁棒，因为它不受极端值（outlier）的影响。

## 附录 D：Transpiler 辅助函数深入分析

### D.1 setTimeCondition() 时间条件设置

**代码位置**：`lib/util/lifted/promql2influxql/transpiler.go:124-156`

```go
// setTimeCondition 在 InfluxQL WHERE 子句中设置时间范围和时区条件
func (t *Transpiler) setTimeCondition(node influxql.Statement, ignoreLookBack bool) {
    if t.timeCondition == nil {
        // 从查询中获取偏移量
        var offset time.Duration
        if n, ok := node.(*influxql.SelectStatement); ok {
            offset = n.QueryOffset
        }
        // 计算起止时间
        var start, end time.Time
        if ignoreLookBack {
            // 忽略回溯：直接使用 minT
            start = timestamp.Time(t.minT - durationMilliseconds(offset))
        } else {
            if t.timeRange == 0 {
                // 即时查询：回溯 lookBackDelta
                start = timestamp.Time(t.minT - t.LookBackDelta.Milliseconds() - durationMilliseconds(offset))
            } else {
                // 范围查询：回溯整个 timeRange
                start = timestamp.Time(t.minT - t.timeRange.Milliseconds() - durationMilliseconds(offset))
            }
        }
        end = timestamp.Time(t.maxT - durationMilliseconds(offset))
        t.timeCondition = GetTimeCondition(&start, &end)
    }
    // 将时间条件应用到语句
    switch statement := node.(type) {
    case *influxql.SelectStatement:
        statement.Condition = CombineConditionAnd(statement.Condition, t.timeCondition)
        statement.LookBackDelta = t.LookBackDelta
        statement.Location = t.Timezone
    case *influxql.ShowTagValuesStatement:
        statement.Condition = CombineConditionAnd(statement.Condition, t.timeCondition)
    }
}
```

**通俗解释**：

`setTimeCondition` 计算查询的实际时间范围：

- **即时查询**（step=0, timeRange=0）：`start = minT - lookBackDelta - offset`
- **范围查询**（timeRange > 0）：`start = minT - timeRange - offset`
- **ignoreLookBack=true**：用于二元表达式场景，不额外回溯

**为什么需要回溯？** Prometheus 的即时查询在每个时间步都需要在 `[t - lookBackDelta, t]` 范围内查找最近的样本。如果查询时间是 T，lookBackDelta 是 5m，那么实际需要读取 `[T-5m, T]` 范围的数据。

### D.2 setTimeInterval() 时间间隔设置

**代码位置**：`lib/util/lifted/promql2influxql/transpiler.go:99-121`

```go
// setTimeInterval 在 InfluxQL GROUP BY 子句中设置时间间隔和偏移
func (t *Transpiler) setTimeInterval(statement *influxql.SelectStatement) {
    interval := &influxql.Dimension{
        Expr: &influxql.Call{
            Name: TimeField,  // "time"
            Args: []influxql.Expr{
                &influxql.DurationLiteral{Val: t.Step},  // GROUP BY time(step)
            },
        },
    }
    defer func() {
        statement.Dimensions = append(statement.Dimensions, interval)
    }()
    if t.Start == nil || t.Step == 0 {
        return
    }
    // 计算时间偏移量：start % step
    remain := t.Start.UnixNano() % t.Step.Nanoseconds()
    offset := time.Duration(remain) * time.Nanosecond
    // GROUP BY time(step, offset)
    interval.Expr.(*influxql.Call).Args = append(
        interval.Expr.(*influxql.Call).Args,
        &influxql.DurationLiteral{Val: offset},
    )
}
```

**通俗解释**：

PromQL 的步长对齐与 InfluxQL 不同：
- Prometheus range query 的评估时间点是 `start, start+step, ..., end`，从查询 `start` 起算，并非从 epoch 对齐
- InfluxQL 的 `GROUP BY time(interval, offset)` 支持自定义偏移量

`setTimeInterval` 计算 `offset = start % step`，把 InfluxQL 的时间分桶对齐到查询 `start`。

### D.3 PreprocessExpr() 步长无关预处理

**代码位置**：`lib/util/lifted/promql2influxql/transpiler.go:184-191`

```go
// PreprocessExpr 包装所有可能的步长无关部分为 StepInvariantExpr。
// 还解析预处理器。
func (t *Transpiler) PreprocessExpr(expr parser.Expr, start, end time.Time) parser.Expr {
    isStepInvariant := preprocessExprHelper(expr, start, end)
    if isStepInvariant {
        t.isStepVariantExpr = true
        return newStepInvariantExpr(expr)
    }
    return expr
}
```

**通俗解释**：

步长无关表达式是指在不同时间步长上返回相同值的表达式，如：
- `1 + 2`：常量表达式
- `vector(1)`：常量向量
- `abs(metric)` 中 `abs` 是步长无关的（如果参数是步长无关的）

对于步长无关表达式，只需要计算一次，然后在每个时间步复用结果（通过 `DuplicateResult` 标志）。

### D.4 转译结果缓存优化——canPushDownAggWithFunction

**代码位置**：`lib/util/lifted/promql2influxql/aggregate_expr.go:264-287`

```go
// canPushDownAggWithFunction 用于优化函数和聚合操作符的嵌套下推
func (t *Transpiler) canPushDownAggWithFunction(agg *parser.AggregateExpr,
    statement *influxql.SelectStatement, field *influxql.Field,
    parameter []influxql.Expr, aggFn aggregateFn) bool {
    // mean 不能下推（因为 InfluxQL 的 mean 语义与嵌套聚合不兼容）
    if aggFn.name == "mean" {
        return false
    }
    call, ok := field.Expr.(*influxql.Call)
    if !ok { return false }
    // 检查是否是范围向量函数
    _, ok = rangeVectorFunctions[getOriCallName(call.Name)]
    if !ok { return false }
    if statement.Range <= 0 { return false }
    // 直接在现有语句上叠加聚合
    t.setAggregateFields(statement, field, parameter, aggFn)
    statement.Dimensions = statement.Dimensions[:0]
    t.setAggregateDimension(statement, agg.Without, agg.Grouping...)
    if t.Step > 0 {
        t.setTimeInterval(statement)
        statement.Fill = influxql.NoFill
    }
    return true
}
```

**通俗解释**：

这是一个查询优化技巧。当聚合表达式嵌套在范围向量函数上时（如 `sum(rate(metric[5m]))`），可以将聚合直接下推到 rate 的计算中，避免创建子查询。

**优化前**：
```sql
SELECT sum(rate_prom(value)) FROM (
    SELECT rate_prom(value) FROM metric GROUP BY *, time(step) fill(none)
) GROUP BY method
```

**优化后**：
```sql
SELECT sum(rate_prom(value)) FROM metric
GROUP BY method, time(step) fill(none)
```

省去了一层子查询，减少了中间结果的序列化/反序列化开销。

### D.5 transpileSubqueryExpr() 子查询处理

**代码位置**：`lib/util/lifted/promql2influxql/transpiler.go:299-335`

```go
func (t *Transpiler) transpileSubqueryExpr(e *parser.SubqueryExpr) (influxql.Node, error) {
    // 保存父查询的时间参数
    preMinT := t.minT
    preMaxT := t.maxT
    preInterval := t.Step

    // 计算子查询的时间范围
    offsetMills := durationMilliseconds(e.Offset)
    rangeMillis := durationMilliseconds(e.Range)
    newEndTime := t.maxT - offsetMills
    var newInterval int64
    if e.Step != 0 {
        newInterval = durationMilliseconds(e.Step)
    } else {
        newInterval = rangeMillis  // 子查询默认步长 = 范围
    }
    // 对齐起始时间
    newStartTime := newInterval * ((t.minT - offsetMills - rangeMillis) / newInterval)
    if newStartTime < (t.minT - offsetMills - rangeMillis) {
        newStartTime += newInterval
    }
    // 处理 @ 修饰符
    if newStartTime != t.minT {
        setOffsetForAtModifier(newStartTime, e.Expr)
    }

    // 设置子查询的时间参数
    t.minT, t.maxT, t.Step = newStartTime, newEndTime, time.Duration(newInterval*int64(time.Millisecond/time.Nanosecond))
    t.upperSubquery++

    // 递归转译子查询表达式
    node, err := t.transpileExpr(e.Expr)

    // 重新初始化时间窗口偏移
    if stmt, ok := node.(*influxql.SelectStatement); ok {
        ReinitTimeWindowOffset(stmt, t.minT, rangeMillis, t.Step)
    }

    // 记录子查询时间范围
    t.subStartT = newStartTime
    t.subEndT = newEndTime

    // 恢复父查询的时间参数
    t.minT, t.maxT, t.Step = preMinT, preMaxT, preInterval
    t.upperSubquery--
    return node, err
}
```

**通俗解释**：

子查询（如 `metric[5m:1m]`）是 PromQL 的高级特性，含义是"在过去 5 分钟内，每 1 分钟评估一次内层表达式"。

处理步骤：
1. **保存父查询参数**：minT, maxT, Step
2. **计算子查询参数**：新的起止时间、步长
3. **时间对齐**：将子查询的起始时间对齐到步长的整数倍
4. **递归转译**：使用子查询的时间参数递归处理内层表达式
5. **重新初始化时间窗口**：`ReinitTimeWindowOffset` 确保子查询内的 GROUP BY time() 正确
6. **恢复父查询参数**

## 附录 E：引擎层采样算法详解

### E.1 双指针算法时间复杂度分析

`computeIntervalIndex`（InstantVectorCursor）和 `getIntervalIndex`（RangeVectorCursor）都使用双指针算法：

```
给定 N 个数据点和 M 个步长时间点：
- i 指针从左到右扫描 N 个点
- j 指针从左到右扫描 N 个点
- 外层循环 M 次
- 总体时间复杂度：O(N + M)
- 空间复杂度：O(M)（intervalIndex 存储 M 个区间对）
```

**为什么不用二分查找？** 因为数据是按时间有序的，双指针只需要单次遍历，比 M 次二分查找（O(M * log N)）更高效。

### E.2 getCurrStep / getPrevStep 步长计算

**代码位置**：`engine/prom_instant_vector_cursor.go:206-226`

```go
// getPrevStep 返回 t 所在的上一个步长时间点
func getPrevStep(startSample, endSample, step, t int64) int64 {
    if t <= startSample { return startSample }
    if t == endSample { return t }
    n := (t - startSample) / step
    return hybridqp.MinInt64(startSample+n*step, endSample)
}

// getCurrStep 返回 t 所在的当前或下一个步长时间点
func getCurrStep(startSample, endSample, step, t int64) int64 {
    if t <= startSample { return startSample }
    n, r := (t-startSample)/step, (t-startSample)%step
    if r > 0 {
        // t 不在步长边界上，返回下一个步长
        return hybridqp.MinInt64(startSample+(n+1)*step, endSample)
    }
    // t 在步长边界上
    return hybridqp.MinInt64(t, endSample)
}
```

**具体例子**：

```
startSample=1000, endSample=6000, step=2000

getCurrStep(1000) = 1000   (恰好在边界)
getCurrStep(1500) = 3000   (下一个步长)
getCurrStep(2000) = 2000   (恰好在边界)
getCurrStep(3500) = 4000   (下一个步长)

getPrevStep(1000) = 1000   (起始)
getPrevStep(1500) = 1000   (上一个步长)
getPrevStep(2000) = 2000   (当前步长)
getPrevStep(3500) = 3000   (上一个步长)
```

### E.3 isSameWindow 跨窗口判断

**代码位置**：`engine/prom_instant_vector_cursor.go:180-204`

```go
func isSameWindow(
    currRecord, nextRecord *record.Record, currInfo, nextInfo *comm.FileInfo,
    schema hybridqp.Catalog, startSample, endSample, step, rangeDuration int64,
) bool {
    if nextRecord == nil || currRecord.RowNums() == 0 {
        return false  // 没有下一个 record
    }
    if nextRecord.RowNums() == 0 {
        return true   // 下一个 record 为空，可能继续属于同一窗口
    }
    // 文件信息不同，说明是不同的数据源
    if currInfo != nil && nextInfo != currInfo {
        return false
    }
    // instant query（step=0）总是同一窗口
    if schema.Options().GetPromStep() == 0 {
        return true
    }
    // 步长大于范围时，使用 IsSameStep
    if step > rangeDuration {
        return IsSameStep(startSample, endSample, step, rangeDuration,
            currRecord.Times()[currRecord.RowNums()-1], nextRecord.Times()[0])
    }
    // 检查两个 record 的最后一个/第一个时间点是否在同一窗口
    prevStep := getCurrStep(startSample, endSample, step, currRecord.Times()[currRecord.RowNums()-1])
    nextStep := getCurrStep(startSample, endSample, step, nextRecord.Times()[0])
    return prevStep == nextStep
}
```

**通俗解释**：

`isSameWindow` 决定两个相邻的 record 是否属于同一个查询窗口。这在数据量大、需要分批处理 record 时至关重要：
- 如果属于同一窗口：当前 record 的处理结果需要与下一个 record 合并
- 如果不属于同一窗口：当前 record 的处理可以独立完成

### E.4 Lookback 填充机制

```mermaid
sequenceDiagram
    participant Step as 步长时间 T
    participant Data as 数据数组
    participant Sampler as floatSampler

    Note over Step: 当前步长时间 T = 100s<br/>lookBackDelta = 300s (5m)

    Step->>Data: 检查 data[i].time == T?
    alt 精确匹配
        Data-->>Sampler: 使用 data[i].value
    else 不精确匹配
        Step->>Data: 检查 data[i].time > T - lookBackDelta?
        alt 在 lookback 窗口内 (data[i].time > 70s)
            Data-->>Sampler: 使用 data[i].value（lookback 填充）
        else 超出 lookback 窗口
            Step->>Data: 检查 data[i] 是否为 stale NaN?
            alt stale NaN
                Data-->>Sampler: 跳过（stale 标记不参与填充）
            else 非 stale
                Sampler->>Sampler: 使用 prevBuf 的前值填充
            end
        end
    end
```

**代码位置**：`engine/prom_instant_vector_cursor.go:327-342`

```go
// PopulateByPrevious 使用前一个 record 的最后一个值填充当前 record 之前的步长
func (r *floatSampler) PopulateByPrevious(outRecord *record.Record, param *ReducerParams,
    nextStep, lastStep int64, outOrdinal, timeIdx int) {
    if model.IsStaleNaN(r.prevBuf.value) {
        return  // stale NaN 不参与 lookback 填充
    }
    for t := nextStep; t <= lastStep; t += param.step {
        if r.prevBuf.time < t-param.lookBackDelta {
            break  // 超出 lookback 窗口，停止填充
        }
        r.fv(outRecord.Column(outOrdinal), r.prevBuf.value)
        if outOrdinal == 0 {
            outRecord.AppendTime(t + r.offset)
        }
    }
}
```

**具体例子**：

```
prevBuf = {time: 5000, value: 42.0}
lookBackDelta = 3000 (3秒)
nextStep = 6000, lastStep = 12000, step = 2000

t=6000: prevBuf.time(5000) >= 6000-3000(3000) -> 填充 42.0
t=8000: prevBuf.time(5000) >= 8000-3000(5000) -> 填充 42.0 (恰好在边界)
t=10000: prevBuf.time(5000) < 10000-3000(7000) -> 停止填充
```

## 附录 F：HTTP Handler 层深入分析

### F.1 Instant Query 与 Range Query 的参数解析

**代码位置**：`lib/util/lifted/influx/httpd/handler_prom_util.go:458-537`

```go
func getInstantQueryCmd(r *http.Request, w http.ResponseWriter, mst string) (cmd promql2influxql.PromCommand, ok bool) {
	// Instant_query Query time. if this parameter is not specified, the current time is used by default.
	ts, err := parseTimeParam(r, "time", time.Now())
	if err != nil {
		invalidParamError(w, err, "time")
		return
	}
	lookBackDelta := promql2influxql.DefaultLookBackDelta
	if r.FormValue("lookback-delta") != "" {
		lookBackDelta, err = parseDuration(r.FormValue("lookback-delta"))
		if err != nil {
			invalidParamError(w, err, "lookback-delta")
			return
		}
	}
	promCommand := promql2influxql.PromCommand{
		Cmd:           r.FormValue("query"),
		Evaluation:    &ts,
		LookBackDelta: lookBackDelta,
	}
	return promCommand, true
}

func getRangeQueryCmd(r *http.Request, w http.ResponseWriter, mst string) (cmd promql2influxql.PromCommand, ok bool) {
	// Range_query query time. if this parameter is not specified, the current time is used by default.
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		invalidParamError(w, err, "start")
		return
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		invalidParamError(w, err, "end")
		return
	}
	if end.Before(start) {
		invalidParamError(w, fmt.Errorf("end timestamp must not be before start time"), "end")
		return
	}

	if err := validation.ValidateQueryTimeRange(mst, start, end); err != nil {
		invalidParamError(w, err, "start_end")
		return
	}

	step, err := parseDuration(r.FormValue("step"))
	if err != nil {
		invalidParamError(w, err, "step")
		return
	}

	if step <= 0 {
		invalidParamError(w, fmt.Errorf("zero or negative query resolution step widths are not accepted. Try a positive integer"), "step")
		return
	}

	// ...
	lookBackDelta := promql2influxql.DefaultLookBackDelta
	if r.FormValue("lookback-delta") != "" {
		lookBackDelta, err = parseDuration(r.FormValue("lookback-delta"))
		if err != nil {
			invalidParamError(w, err, "lookback-delta")
			return
		}
	}
	promCommand := promql2influxql.PromCommand{
		Cmd:           r.FormValue("query"),
		Start:         &start,
		End:           &end,
		Step:          step,
		LookBackDelta: lookBackDelta,
		DataType:      promql2influxql.GRAPH_DATA,
	}
	return promCommand, true
}
```

**通俗解释**：

| 参数 | Instant Query | Range Query |
|------|--------------|-------------|
| `query` | 必需 | 必需 |
| `time` | 可选（默认 now） | 不使用 |
| `start` | 不使用 | 必需 |
| `end` | 不使用 | 必需 |
| `step` | 不使用 | 必需（无默认值，必须 > 0） |
| `lookback-delta` | 可选（默认 5m） | 可选（默认 5m） |
| `timeout` | 可选（在 `servePromBaseQuery` 中解析） | 可选（在 `servePromBaseQuery` 中解析） |

### F.2 MetricStore 模式

**代码位置**：`lib/util/lifted/influx/httpd/handler_prom.go:91-96`

```go
func (h *Handler) servePromWriteWithMetricStore(w http.ResponseWriter, r *http.Request, user meta2.User) {
    mst, ok := getMstByProm(h, w, r)
    if !ok { return }
    h.servePromWriteBase(w, r, user, mst, timeSeries2RowsV2)
}
```

**通俗解释**：

MetricStore 模式允许将不同的指标存储到不同的 measurement 中，而不是全部存储在 `__name__` 对应的 measurement 中。

URL 模式：`/prometheus/{metric_store}/api/v1/query`

当使用 MetricStore 模式时：
- 写入时：`metric_store` 作为 measurement 名称
- 查询时：`metric_store` 作为 `FROM` 子句中的 measurement

这在多租户场景中特别有用——不同租户可以使用不同的 metric_store，实现数据隔离。

### F.3 异步查询支持

```go
// 解析是否为异步命令
async := r.FormValue("async") == "true"

// 异步执行：go h.async 排空结果，HTTP 立即返回 204 No Content
if async {
	go h.async(q, resultCh)
	h.writeHeader(w, http.StatusNoContent)
	isRespond = true
	return
}
```

> 说明：异步分支不会在前台执行查询，`h.async` 只是消费 `resultCh` 并在出错时记录日志；状态码是 `204 No Content`，不是 `202 Accepted`。

### F.4 Explain 查询分析

```go
// explain 参数用于输出查询延迟分析
isExplain := false
explain := r.FormValue("explain")
if len(explain) > 0 {
	isExplain, err = strconv.ParseBool(explain)
	if err != nil {
		apiErr := &apiError{errorBadData, err}
		respondError(w, apiErr)
		return
	}
}

// 在 execQuery 中使用 ExplainStatement（Analyze=true）
if isExplain {
	q = &influxql.Query{Statements: []influxql.Statement{
		&influxql.ExplainStatement{Statement: statement, Analyze: true},
	}}
}
```

**通俗解释**：

Prom 路径使用的是 `Analyze: true` 的 EXPLAIN ANALYZE：coordinator 会走 `executeExplainAnalyzeStatement`，创建 pipeline executor 并**真实执行查询**、采集各阶段耗时，最后返回带统计信息的执行结果；它不是只返回执行计划。若需要“不执行”的 EXPLAIN，应使用 `Analyze: false`，但 Prom 路径没有使用该分支。

### F.5 慢查询统计

```go
var qDuration *statistics.SQLSlowQueryStatistics
if !isInternalDatabase(db) {
    qDuration = statistics.NewSqlSlowQueryStatistics(db)
    defer func() {
        d := time.Now().Sub(start)
        if d.Nanoseconds() > time.Second.Nanoseconds()*10 {
            // 超过 10 秒的查询记录为慢查询
            qDuration.AddDuration("TotalDuration", d.Nanoseconds())
            statistics.AppendSqlQueryDuration(qDuration)
            h.Logger.Info("slow query",
                zap.Duration("duration", d),
                zap.String("db", qDuration.DB),
                zap.String("query", qDuration.Query),
            )
        }
    }()
}
```

**通俗解释**：

所有超过 10 秒的 PromQL 查询都会被记录为慢查询，包括：
- 执行时间
- 数据库名称
- 查询语句

这些统计数据会被推送到监控系统，用于告警和性能分析。

## 附录 G：配置参数参考

### G.1 LookBackDelta 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DefaultLookBackDelta` | 5 分钟 | PromQL 查询的最大回溯窗口 |

**影响**：
- 增大 LookBackDelta 可以确保在数据点稀疏时仍能找到样本
- 减小 LookBackDelta 可以减少查询的数据读取量
- Prometheus 的默认值也是 5 分钟，保持一致可以确保语义兼容

### G.2 ResultCache 配置

| 参数 | 说明 |
|------|------|
| `ResultCache.Enabled` | 是否启用结果缓存 |
| `ResultCache.CacheType` | 缓存类型枚举：`MEM_CACHE` / `FILE_CACHE`；当前 factory 只有 `MEM_CACHE` 实现 |
| `ResultCache.SplitQueriesByInterval` | 查询分割间隔 |
| `ResultCache.MaxCacheFreshness` | 最大缓存新鲜度 |

### G.3 默认值常量

**代码位置**：`lib/util/lifted/promql2influxql/constant.go:16-29`

```go
const (
    ArgNameOfTimeFunc          = "prom_time"   // time() 函数的参数名
    DefaultFieldKey            = "value"       // 默认字段名
    DefaultMetricKeyLabel      = "__name__"    // 指标名标签
    DefaultMeasurementName     = "prom_metric_not_specified"  // 未指定指标时的默认 measurement
    DefaultDatabaseName        = "prom"        // 默认数据库
    DefaultRetentionPolicyName = "autogen"     // 默认保留策略
    TimeField                  = "time"
    LastCall                   = "last"
    PromSuffix                 = "_prom"       // PromQL 函数后缀
)

const DefaultLookBackDelta = 5 * time.Minute  // 默认回溯窗口
const FieldCountForKeepMetric = 2             // keepMetric 模式下的字段数
```

## 附录 H：错误处理与异常场景

### H.1 错误码分类

PromQL 兼容层定义了丰富的错误码，覆盖各类异常场景：

| 错误码 | 含义 | 触发场景 |
|--------|------|----------|
| `TranspileExprFail` | 表达式转译失败 | 顶层转译错误 |
| `TranspileIVSFail` | 即时向量选择器转译失败 | VectorSelector 处理错误 |
| `TranspileFunctionFail` | 函数转译失败 | Call 参数转译错误 |
| `TranspileAggFail` | 聚合表达式转译失败 | AggregateExpr 处理错误 |
| `UnsupportedNodeType` | 不支持的 AST 节点类型 | 遇到未知的 PromQL 节点 |
| `UnsupportedPromExpr` | 不支持的 PromQL 表达式 | 无法转换为 InfluxQL |
| `UnsupportedAggType` | 不支持的聚合类型 | 不在 aggregateFns 表中 |
| `UnsupportedBothVS` | 不支持的双向量操作 | 两边都是 vector 的复杂情况 |
| `InvalidSVBinOp` | 无效的标量-向量二元操作 | 运算符不支持的操作数组合 |
| `InvalidExprType` | 无效的表达式类型 | 返回类型不是 vector 或 scalar |
| `PromReceiverErr` | 结果转换错误 | Receiver 中的 panic 恢复 |
| `ErrPopulatePromSeries` | 序列构建错误 | 标签分组时的错误 |
| `ErrGroupResultBySeries` | 序列分组错误 | 哈希分组时的错误 |
| `ParseTimeFail` | 时间解析错误 | Row 中的时间类型不正确 |

### H.2 panic 恢复机制

**代码位置**：`lib/util/lifted/promql2influxql/receiver.go:237-241`

```go
defer func() {
    if re := recover(); re != nil {
        promRes, promErr = NewPromData(nil, ""), errno.NewError(errno.PromReceiverErr)
        logger.GetLogger().Error(promErr.Error(),
            zap.String("InfluxResultToPromQLValue", string(debug.Stack())))
    }
}()
```

**通俗解释**：

所有 Receiver 方法都有 `defer recover()` 保护，确保即使发生 panic（如空指针、类型断言失败），也不会导致整个进程崩溃。panic 会被捕获并转换为错误返回值，同时记录完整的堆栈信息便于排查。

### H.3 空结果处理

```go
// AbsentNoMstResult 处理 absent() 函数在没有匹配 measurement 时的结果
func (r *Receiver) AbsentNoMstResult(expr parser.Expr) (*PromData, error) {
    if r.Evaluation != nil {
        // instant query：返回一个值为 1 的样本
        metric := getAbsentLabelsFromExpr(expr)
        res := &promql.Vector{promql.Sample{T: r.Evaluation.UnixMilli(), F: 1, Metric: metric}}
        return NewPromData(&PromDataVector{res}, string(parser.ValueTypeVector)), nil
    }
    // range query：返回多个时间步的值为 1 的样本
    start, end, interval := r.Start.UnixMilli(), r.End.UnixMilli(), r.Step.Milliseconds()
    // ... 生成完整的时间序列
}
```

**通俗解释**：

`absent()` 函数的语义是"如果序列不存在则返回 1"。当查询的 measurement 本身就不存在时（即完全没有数据），需要特殊处理：不查询存储层，直接构造返回值为 1 的结果。

### H.4 错误包装示例

```go
// 转译错误包装
influxNode, err := t.transpile(expr)
if err != nil {
    return nil, errno.NewError(errno.TranspileExprFail, err.Error())
}

// 二元表达式左操作数错误包装
lhs, err := t.transpileExpr(b.LHS)
if err != nil {
    return nil, errno.NewError(errno.UnableLeftBinOp, err.Error())
}

// 二元表达式右操作数错误包装
rhs, err := t.transpileExpr(b.RHS)
if err != nil {
    return nil, errno.NewError(errno.UnableRightBinOp, err)
}
```

**通俗解释**：

每一层转译都对错误进行了包装，添加了上下文信息。当用户看到 `TranspileExprFail: UnableLeftBinOp: ...` 时，可以快速定位是二元表达式的左操作数转译出了问题。

## 附录 I：引擎层 Routine 接口与 Reducer 模式

### I.1 Routine 接口体系

```mermaid
classDiagram
    class Reducer {
        <<interface>>
        +Aggregate(p, param)
    }

    class Routine {
        <<interface>>
        +WorkOnRecord(in, out, params)
    }

    class RoutineImpl {
        -reducer Reducer
        -inOrdinal int
        -outOrdinal int
        -endPoint *ReducerEndpoint
        +WorkOnRecord(in, out, params)
    }

    class floatIncAggReducer {
        -fr FloatIncAggReduceFunc
        -fm FloatIncAggMergeFunc
        +Aggregate(p, param) 增量聚合
    }

    class floatSliceReducer {
        -fr FloatSliceReduceFunc
        -fm FloatSliceMergeFunc
        +Aggregate(p, param) 收集切片
    }

    class floatRateReducer {
        -fr FloatRateReduceFunc
        -fm FloatRateMergeFunc
        -fu FloatRateUpdateFunc
        +Aggregate(p, param) 速率计算
    }

    Routine <|.. RoutineImpl : 实现
    RoutineImpl o-- Reducer : 持有
    Reducer <|.. floatIncAggReducer : 实现
    Reducer <|.. floatSliceReducer : 实现
    Reducer <|.. floatRateReducer : 实现
```

引擎层的函数执行基于 Routine/Reducer 接口模式：

```go
type Reducer interface {
	Aggregate(*ReducerEndpoint, *ReducerParams)
}

type Routine interface {
	WorkOnRecord(*record.Record, *record.Record, *ReducerParams)
}

type RoutineImpl struct {
	reducer    Reducer
	inOrdinal  int
	outOrdinal int
	endPoint   *ReducerEndpoint
}

func NewRoutineImpl(reducer Reducer, inOrdinal int, outOrdinal int) *RoutineImpl {
	return &RoutineImpl{
		reducer:    reducer,
		inOrdinal:  inOrdinal,
		outOrdinal: outOrdinal,
		endPoint: &ReducerEndpoint{
			InputPoint:  EndPointPair{},
			OutputPoint: EndPointPair{},
		},
	}
}

func (r *RoutineImpl) WorkOnRecord(in *record.Record, out *record.Record, params *ReducerParams) {
	r.endPoint.InputPoint.Record = in
	r.endPoint.InputPoint.Ordinal = r.inOrdinal
	r.endPoint.OutputPoint.Record = out
	r.endPoint.OutputPoint.Ordinal = r.outOrdinal
	r.reducer.Aggregate(r.endPoint, params)
}
```

`Routine` 接口只有 `WorkOnRecord`，`Reduce/Merge` 并不是接口方法：具体计算由 `RoutineImpl` 持有的 `Reducer.Aggregate` 完成，各 reducer 内部再调用自己的 `fr`（单窗口归约）与 `fm`（跨 record 合并）。`NewRoutineImpl` 把 reducer、输入/输出列序号组装成 `RoutineImpl`。每个 PromQL 函数都通过 `CreateRoutine()` 工厂方法创建对应的 Routine 实例。Routine 有三种主要的 Reducer 模式：

#### I.1.1 floatIncAggReducer（增量 Reducer）

适用于聚合类函数（avg、sum、min、max、count、last 等）：

```go
type floatIncAggReducer struct {
	fr        FloatIncAggReduceFunc
	fm        FloatIncAggMergeFunc
	prevStep  int64
	offset    int64
	prevPoint *floatPoint
	ringBuf   *floatBuffer
}

type FloatIncAggReduceFunc func(times []int64, values []float64, start, end int) (int64, float64, bool)
type FloatIncAggMergeFunc func(prevValue float64, currValue float64, prevCount, currCount int) (float64, int)
```

**工作流程**：
1. **Reduce（`fr`）**：在每个时间窗口内，计算一个中间结果（时间戳、值、计数）
2. **Merge（`fm`）**：跨 record 时，合并两个中间结果

**示例**（sum_over_time）：
```
Record 1 窗口 [0, 3): values=[1.0, 2.0, 3.0]
  Reduce: time=0, value=6.0, count=3

Record 2 窗口 [0, 3) 续: values=[4.0]
  Reduce: time=0, value=4.0, count=1
  Merge: value=6.0+4.0=10.0, count=3+1=4
```

#### I.1.2 floatSliceReducer（切片 Reducer）

适用于需要整个窗口数据的函数（rate、increase、deriv、quantile 等）：

```go
type floatSliceReducer struct {
	fr       FloatSliceReduceFunc
	fm       FloatSliceMergeFunc
	prevStep int64
	ringBuf  *floatBuffer
}

type FloatSliceReduceFunc func(times []int64, values []float64, start, end int) ([]int64, []float64, bool)
type FloatSliceMergeFunc func(prevT, currT []int64, prevV, currV []float64, ts int64, count int, param *ReducerParams) (float64, bool)
```

**工作流程**：
1. **Reduce（`fr`）**：提取窗口内的原始数据切片（不做计算）
2. **Merge（`fm`）**：跨 record 合并数据切片，然后在最终的完整数据上计算结果

**为什么 rate 需要 floatSliceReducer？** 因为 rate 的外推算法需要知道窗口内所有数据点的分布情况（首尾点的位置、数据点的数量等），无法通过增量方式计算。

#### I.1.3 floatRateReducer（速率 Reducer）

适用于速率类函数（rate/irate/delta/idelta/increase）：

```go
type floatRateReducer struct {
	fr         FloatRateReduceFunc
	fm         FloatRateMergeFunc
	fu         FloatRateUpdateFunc
	prevStep   int64
	prevPoints [2]*floatPoint
	ringBuf    *floatBuffer
}

type FloatRateReduceFunc func(times []int64, values []float64, start, end int) (int64, int64, float64, float64, bool)
type FloatRateMergeFunc func(prevTime int64, currTime int64, prevValue float64, currValue float64, ts int64, pointCount int, param *ReducerParams) (float64, bool)
type FloatRateUpdateFunc func(ft1, ft2, lt1, lt2 int64, fv1, fv2, lv1, lv2 float64) (int64, int64, float64, float64)
```

**工作流程**：
1. **Reduce（`fr`）**：提取窗口内的首尾时间/值
2. **Merge（`fm`）**：基于首尾点与外推参数计算 rate/delta
3. **Update（`fu`）**：跨 record 时更新保存的首尾点状态

### I.2 CoProcessor 协处理器

```mermaid
sequenceDiagram
    participant Transform as StreamAggregateTransform
    participant CP as CoProcessorImpl
    participant R0 as RoutineImpl[0]
    participant D0 as floatIncAggReducer

    Transform->>CP: WorkOnRecord(inRecord, outRecord, params)
    CP->>R0: WorkOnRecord(inRecord, outRecord, params)
    R0->>D0: Aggregate(endPoint, params)
    D0-->>R0: 写入 outRecord
    R0-->>CP: 返回
    CP-->>Transform: 处理完成
```

```go
type CoProcessor interface {
	WorkOnRecord(*record.Record, *record.Record, *ReducerParams)
}

type CoProcessorImpl struct {
	Routines []Routine
}

func (p *CoProcessorImpl) AppendRoutine(routines ...Routine) {
	p.Routines = append(p.Routines, routines...)
}

func (p *CoProcessorImpl) WorkOnRecord(in *record.Record, out *record.Record, params *ReducerParams) {
	for _, r := range p.Routines {
		r.WorkOnRecord(in, out, params)
	}
}
```

**通俗解释**：

当查询包含多个 PromQL 函数时（如 `rate(value), sum(value)`），CoProcessor 会同时管理多个 Routine，对同一个 record 进行多次处理，每个 Routine 负责一个输出字段。

### I.3 ReducerParams 参数传递

```go
type ReducerParams struct {
	multiCall     bool
	sameWindow    bool
	lastRec       bool
	firstStep     int64
	lastStep      int64
	step          int64
	offset        int64
	rangeDuration int64
	lookBackDelta int64
	intervalIndex []uint16
}
```

**通俗解释**：

`ReducerParams` 是 InstantVectorCursor/RangeVectorCursor 向 Routine 传递参数的桥梁。它包含了计算所需的所有上下文信息：
- `intervalIndex`：窗口的起止索引（双指针算法的输出）
- `step`、`firstStep`：步长信息
- `lookBackDelta`、`rangeDuration`：窗口参数
- `sameWindow`、`lastRec`：跨 record 状态

## 附录 J：PromQL 远程读写协议

### J.1 Remote Write 协议

Prometheus Remote Write 协议使用 Protobuf + Snappy 压缩：

```
POST /api/v1/write
Content-Type: application/x-protobuf
Content-Encoding: snappy
X-Prometheus-Remote-Write-Version: 0.1.0

[Snappy 压缩的 prompb.WriteRequest]
```

**WriteRequest 结构**：
```protobuf
message WriteRequest {
    repeated TimeSeries timeseries = 1;
    repeated MetricMetadata metadata = 3;
}

message TimeSeries {
    repeated Label labels = 1;
    repeated Sample samples = 2;
}

message Label {
    string name = 1;
    string value = 2;
}

message Sample {
    double value = 1;
    int64 timestamp = 2;
}
```

### J.2 timeSeries2Rows 转换

```go
func timeSeries2Rows(mst string, dst []influx.Row, tss []prompb2.TimeSeries, invalidTss map[int]bool) ([]influx.Row, error) {
	var i int
	for tsIdx, ts := range tss {
		if invalidTss[tsIdx] {
			continue
		}
		dst[i].ResizeTags(len(ts.Labels))
		tags := dst[i].Tags
		tags, mst = unmarshalPromTags(tags, ts)
		for j, s := range ts.Samples {
			// convert and append
			dst[i].Timestamp = s.Timestamp * int64(time.Millisecond)
			dst[i].Name = mst
			if j == 0 {
				dst[i].Tags = tags
			} else {
				dst[i].CloneTags(tags)
			}
			if cap(dst[i].Fields) > 0 {
				dst[i].Fields = dst[i].Fields[:1]
				dst[i].Fields[0].Type = influx.Field_Type_Float
				dst[i].Fields[0].Key = promql2influxql.DefaultFieldKey
				dst[i].Fields[0].NumValue = s.Value
			} else {
				dst[i].Fields = []influx.Field{
					influx.Field{
						Type:     influx.Field_Type_Float,
						Key:      promql2influxql.DefaultFieldKey,
						NumValue: s.Value,
					},
				}
			}
			i++
		}
	}
	return dst, nil
}
```

> 说明：`unmarshalPromTags`（`handler_prom_util.go:353-364`）在遍历 labels 时把 `__name__` 的值取作 measurement 名；若没有 `__name__`，measurement 使用 `DefaultMeasurementName`。

### J.3 Remote Read 协议

```
POST /api/v1/read
Content-Type: application/x-protobuf
Content-Encoding: snappy

[Snappy 压缩的 prompb.ReadRequest]
```

**ReadRequest 结构**：
```protobuf
message ReadRequest {
    repeated Query queries = 1;
}

message Query {
    int64 start_timestamp_ms = 1;
    int64 end_timestamp_ms = 2;
    repeated LabelMatcher matchers = 3;
    ReadHints hints = 4;
}
```

**ReadResponse 结构**：
```protobuf
message ReadResponse {
    repeated QueryResult results = 1;
}

message QueryResult {
    repeated TimeSeries timeseries = 1;
}
```

### J.4 ReadRequestToInfluxQuery 转换

```go
func ReadRequestToInfluxQuery(req *prompb.ReadRequest, mst string) (string, error) {
	var readRequest string
	for _, q := range req.Queries {
		if err := validation.ValidateQueryTimeRange(mst, time.UnixMilli(q.StartTimestampMs), time.UnixMilli(q.EndTimestampMs)); err != nil {
			return "", err
		}

		if s, err := buildCommand(q, mst); err != nil {
			return "", err
		} else {
			readRequest += ";" + s
		}
	}
	if len(readRequest) != 0 {
		return readRequest[1:], nil
	}
	return "", nil
}

func buildCommand(q *prompb.Query, mst string) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	// If we don't find a metric name matcher, query all metrics
	// (InfluxDB measurements) by default.
	from := "FROM /.+/"
	haveMetricStore := len(mst) > 0
	if haveMetricStore {
		from = fmt.Sprintf("FROM %s", mst)
	}
	for _, m := range q.Matchers {
		if !haveMetricStore && m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				from = fmt.Sprintf("FROM %q", m.Value)
			case prompb.LabelMatcher_RE:
				from = fmt.Sprintf("FROM /%s/", escapeSlashes(m.Value))
			default:
				// TODO: Figure out how to support these efficiently.
				return "", errors.New("non-equal or regex-non-equal matchers are not supported on the metric name yet")
			}
			continue
		}

		switch m.Type {
		case prompb.LabelMatcher_EQ:
			matchers = append(matchers, fmt.Sprintf("%q = '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_NEQ:
			matchers = append(matchers, fmt.Sprintf("%q != '%s'", m.Name, escapeSingleQuotes(m.Value)))
		case prompb.LabelMatcher_RE:
			matchers = append(matchers, fmt.Sprintf("%q =~ /%s/", m.Name, escapeSlashes(m.Value)))
		case prompb.LabelMatcher_NRE:
			matchers = append(matchers, fmt.Sprintf("%q !~ /%s/", m.Name, escapeSlashes(m.Value)))
		default:
			return "", errors.New("unknown match type")
		}
	}
	matchers = append(matchers, fmt.Sprintf("time >= %v", q.StartTimestampMs*1e6))
	matchers = append(matchers, fmt.Sprintf("time <= %v", q.EndTimestampMs*1e6))

	return fmt.Sprintf("SELECT value %s WHERE %v GROUP BY *", from, strings.Join(matchers, " AND ")), nil
}
```

> 两段代码分别位于 `lib/util/lifted/influx/httpd/handler.go:2415-2432` 与 `handler.go:2365-2405`。时间是 `StartTimestampMs*1e6` 的纳秒整数，不是 RFC3339；`__name__` 匹配器用于生成 `FROM`，不会写成普通标签条件；生成结果固定带 `GROUP BY *`。

## 附录 K：PromQL 兼容性矩阵

### K.1 已支持的 PromQL 特性

> 说明：下表中的“完全支持”表示对应代码路径已实现；本次审计未逐项验证其与 Prometheus 的语义等价性。

| 特性 | 支持状态 | 说明 |
|------|---------|------|
| 即时查询（/api/v1/query） | 完全支持 | |
| 范围查询（/api/v1/query_range） | 完全支持 | |
| 标签匹配器（=, !=, =~, !~） | 完全支持 | |
| 范围向量（[5m]） | 完全支持 | |
| 子查询（[5m:1m]） | 完全支持 | |
| 偏移修饰符（offset 5m） | 完全支持 | |
| @ 修饰符（@ 1234567890） | 完全支持 | |
| 聚合操作符（sum, avg, max 等） | 完全支持 | |
| by/without 维度控制 | 完全支持 | |
| 二元运算（+，-，*，/） | 部分支持 | 常见标量-标量、标量-向量和部分向量-向量可用；matrix/vector 双侧和有/无 measurement 混合等复杂组合仍有 TODO/限制 |
| 比较运算（==, !=, >, <） | 部分支持 | 常见比较可用，复杂向量匹配限制同二元运算 |
| bool 修饰符 | 完全支持 | |
| 向量匹配（on/ignoring） | 部分支持 | `BinOp` 路径实现常见匹配，但不是完整 Prometheus vector matching 语义覆盖 |
| group_left/group_right | 部分支持 | |
| Remote Write | 支持基础写入 | Protobuf + Snappy 端点存在，写入路径使用 VictoriaMetrics parser |
| Remote Read | 支持基础查询转换 | `/api/v1/read` 会转换为 InfluxQL 查询，不等同于 Prometheus Remote Read 全语义覆盖 |
| 标签查询（/api/v1/labels） | 完全支持 | |
| 标签值查询 | 完全支持 | |
| 序列查询（/api/v1/series） | 完全支持 | |
| 元数据查询（/api/v1/metadata） | 完全支持 | |
| 结果缓存 | 部分支持 | `MEM_CACHE` 已实现；`FILE_CACHE` 仅有枚举，factory 尚未创建文件缓存实例；`@` modifier 超出安全时间或负 offset 时不缓存 |

### K.2 已支持的 PromQL 函数

| 类别 | 函数 | 数量 |
|------|------|------|
| 范围向量函数 | rate, irate, increase, delta, idelta, deriv, predict_linear, avg_over_time, sum_over_time, min_over_time, max_over_time, count_over_time, last_over_time, stddev_over_time, stdvar_over_time, quantile_over_time, changes, resets, holt_winters, absent_over_time, present_over_time, mad_over_time | 22 |
| 即时向量函数 | histogram_quantile, scalar, absent | 3 |
| 数学函数 | abs, ceil, floor, exp, sqrt, ln, log2, log10, round, acos, asin, atan, cos, sin, tan, clamp, clamp_max, clamp_min, rad, deg, sinh, cosh, tanh, asinh, atanh, acosh, sgn | 27 |
| 标签函数 | label_replace, label_join | 2 |
| 时间函数 | year, time, timestamp, month, minute, hour, day_of_week, day_of_month, day_of_year, days_in_month, vector, pi | 12 |
| 排序函数 | sort, sort_desc, sort_by_label, sort_by_label_desc | 4 |

### K.3 已知限制

| 限制 | 说明 | 影响 |
|------|------|------|
| 向量-向量复杂匹配 | 一个有 measurement、一个没有时的二元运算不完整 | 少数复杂查询可能失败 |
| 解释器差异 | 部分函数的浮点精度可能与原生 Prometheus 有微小差异 | 精度敏感场景需注意 |
| 嵌套子查询深度 | 未设置明确的深度限制 | 极深嵌套可能影响性能 |
| 正则表达式 | 使用 Go 标准 regexp 库，与 Prometheus 的 OptimizedRegexMatcher 可能有差异 | 复杂正则的性能可能不同 |

## 附录 L：PromQL 兼容层扩展指南

### L.1 添加新的 PromQL 函数

当需要支持新的 PromQL 函数时，需要在三个层级进行修改：

#### 步骤 1：在 Transpiler 层添加函数映射

```go
// 在 call.go 中，根据函数类型添加到对应的 map
var rangeVectorFunctions = map[string]aggregateFn{
    // ... 已有函数
    "new_function": {name: "new_function_prom", functionType: TRANSFORM_FN},
}
```

#### 步骤 2：在 Engine 层实现函数逻辑

```go
// 在 prom_functions.go 中添加实现
type newFunctionOp struct{}

func (o *newFunctionOp) CreateRoutine(p *PromFuncParam) (Routine, error) {
    // 选择合适的 Reducer 模式
    return NewRoutineImpl(newFloatIncReducer(newFunctionReduce, newFunctionMerge), p.inOrdinal, p.outOrdinal), nil
}

func newFunctionReduce(times []int64, values []float64, start, end int) (int64, float64, bool) {
    // 实现归约逻辑
}

func newFunctionMerge(prevValue, currValue float64, prevCount, currCount int) (float64, int) {
    // 实现合并逻辑
}

func init() {
    RegistryPromFunction("new_function_prom", &newFunctionOp{})
}
```

#### 步骤 3：在 Receiver 层处理结果（通常不需要修改）

除非新函数的返回类型与现有函数不同，否则 Receiver 层不需要修改。

### L.2 添加新的聚合操作符

```go
// 在 aggregate_expr.go 中添加映射
var aggregateFns = map[parser.ItemType]aggregateFn{
    // ... 已有操作符
    parser.NEW_AGG: {name: "new_agg_prom", functionType: AGGREGATE_FN},
}
```

### L.3 添加新的二元运算符

```go
// 在 binary_expr.go 中添加映射
var arithBinOps = map[parser.ItemType]influxql.Token{
    // ... 已有运算符
    parser.NEW_OP: influxql.NEW_OP,
}
```

### L.4 测试建议

1. **单元测试**：在 `lib/util/lifted/promql2influxql/` 目录下编写转译测试
2. **集成测试**：使用 Prometheus 的测试框架验证查询结果
3. **端到端测试**：通过 HTTP API 发送查询，验证响应格式
4. **对比测试**：与原生 Prometheus 对比相同查询的结果，确保一致性

## 附录 M：核心数据结构流转详解

### M.1 从 PromQL 到 InfluxQL 的 AST 映射

下表详细列出了每种 PromQL AST 节点到 InfluxQL AST 节点的映射关系：

| PromQL AST 节点 | InfluxQL AST 节点 | 映射方式 |
|----------------|-------------------|----------|
| `parser.NumberLiteral` | `influxql.NumberLiteral` | 直接映射 |
| `parser.StringLiteral` | `influxql.StringLiteral` | 直接映射 |
| `parser.VectorSelector` | `influxql.SelectStatement` | 生成 SELECT + WHERE + GROUP BY |
| `parser.MatrixSelector` | `influxql.SelectStatement` | 设置 timeRange 后递归 VectorSelector |
| `parser.AggregateExpr` | `influxql.SelectStatement` | 外层 SELECT + GROUP BY 维度 |
| `parser.BinaryExpr`（标量-标量） | `influxql.BinaryExpr` | 直接映射运算符 |
| `parser.BinaryExpr`（标量-向量） | `influxql.SelectStatement` + 字段表达式 | 嵌入到字段或条件中 |
| `parser.BinaryExpr`（向量-向量） | `influxql.SelectStatement` + `influxql.BinOp` | 使用 BinOp 源 |
| `parser.Call` | `influxql.SelectStatement` + `influxql.Call` | 函数名映射 + 参数传递 |
| `parser.SubqueryExpr` | `influxql.SelectStatement` | 递归 + 时间参数调整 |
| `parser.ParenExpr` | `influxql.ParenExpr` | 直接映射 |
| `parser.UnaryExpr` | `influxql.BinaryExpr` | SUB 取负（字面量取负，或 `-1 * expr`）；ADD 原样返回 |
| `parser.StepInvariantExpr` | 透传内部表达式 | 设置 duplicateResult |

### M.2 InfluxQL SelectStatement 字段详解

Transpiler 生成的 `influxql.SelectStatement` 包含以下关键字段：

```go
type SelectStatement struct {
    // 基本字段
    Fields      Fields           // SELECT 子句的字段列表
    Sources     Sources          // FROM 子句的数据源
    Condition   Expr             // WHERE 子句的条件表达式
    Dimensions  Dimensions       // GROUP BY 子句的维度

    // PromQL 扩展字段
    IsPromQuery bool             // 标记为 PromQL 查询
    Step        time.Duration    // 步长（对应 PromQL 的 step）
    Range       time.Duration    // 范围（对应 [5m] 的 5m）
    LookBackDelta time.Duration  // 回溯窗口
    QueryOffset time.Duration    // 查询偏移
    RemoveMetric bool            // 是否移除 __name__ 标签
    Without     bool             // 是否使用 without 模式
    Fill        FillOption       // 填充选项（通常为 NoFill）
    SortFields  SortFields       // 排序字段（sort 函数使用）

    // 子查询相关
    PromSubCalls []*PromSubCall  // PromQL 子查询调用

    // 优化相关
    SelectAllTags bool           // 是否选择所有标签
    SelectTagToAux bool          // 是否将选择的标签移到辅助位置
}
```

### M.3 BinOp 结构详解

`influxql.BinOp` 是 openGemini 为 PromQL 向量运算专门扩展的节点：

```go
type BinOp struct {
    LSrc        Source      // 左操作数源（SubQuery）
    RSrc        Source      // 右操作数源（SubQuery）
    LExpr       Expr        // 左操作数表达式（标量场景）
    RExpr       Expr        // 右操作数表达式（标量场景）
    OpType      int         // 操作类型（对应 PromQL 运算符）
    On          bool        // 是否使用 on 模式
    MatchKeys   []string    // 匹配的标签列表
    MatchCard   MatchCardinality  // 基数类型（one/many）
    IncludeKeys []string    // 包含的标签列表
    ReturnBool  bool        // 是否返回 bool 值
}
```

**通俗解释**：

BinOp 的核心是向量匹配逻辑：
- `On: true, MatchKeys: ["method"]`：表示 `on (method)` —— 只在 method 标签上匹配
- `MatchCard: CARD_ONE`：表示 `group_left` —— 左边是 "多" 端，右边是 "一" 端
- `IncludeKeys: ["endpoint"]`：表示 `group_left (endpoint)` —— 将右边的 endpoint 标签包含到结果中

### M.4 PromSubCall 子查询调用

```go
type PromSubCall struct {
    Name      string           // 函数名（如 "rate_prom"）
    StartTime int64            // 查询起始时间（纳秒）
    EndTime   int64            // 查询结束时间（纳秒）
    Range     time.Duration    // 范围时长
    Offset    time.Duration    // 偏移量
    InArgs    []Expr           // 函数参数
    Interval  int64            // 评估间隔（纳秒）
    SubStartT int64            // 子查询起始时间
    SubEndT   int64            // 子查询结束时间
    SubStep   int64            // 子查询步长
    LowerStepInvariant bool   // 下层是否步长无关
}
```

**通俗解释**：

`PromSubCall` 用于处理带子查询的函数调用，如 `rate(metric[5m:1m])`。它封装了子查询的所有时间参数，传递给引擎层的子查询执行器。

## 附录 N：性能优化策略

### N.1 查询计划优化

Transpiler 在转译过程中实现了多种查询计划优化：

1. **聚合下推**（`canPushDownAggWithFunction`）：将聚合函数直接下推到范围向量函数中，避免子查询
2. **条件合并**（`CombineConditionAnd`）：将时间条件和标签条件合并为单个 WHERE 子句
3. **步长无关优化**（`StepInvariantExpr`）：对步长无关表达式只计算一次，复用结果
4. **批量处理**：CoProcessor 在单个 record 上执行所有 Routine，减少 I/O 次数

### N.2 内存优化

1. **对象池**：使用 `pool.GetRows` / `pool.PutRows` 复用行对象
2. **流式解析**：Remote Write 使用 VictoriaMetrics 的流式解析器，不需要一次性读入整个请求体
3. **record 分批**：数据按 record 分批处理，单个 record 的大小有限，内存可控
4. **BufferPool**：使用 `bufferpool.Get()` / `bufferpool.Put()` 复用缓冲区

### N.3 缓存策略

1. **结果缓存**：对范围查询的结果进行缓存，避免重复计算
2. **一致性哈希**：确保相同查询路由到同一节点，提高缓存命中率
3. **缓存分割**：大范围查询被分割为多个小范围，分别缓存和合并
4. **新鲜度控制**：`MaxCacheFreshness` 防止返回过期数据

### N.4 并发控制

```go
opts := query.ExecutionOptions{
    ParallelQuery: atomic.LoadInt32(&syscontrol.ParallelQueryInBatch) == 1,
    // ...
}
```

当 `ParallelQuery` 启用时，多个查询可以并行执行。这在高并发场景下可以提高吞吐量，但也需要注意资源竞争。

## 附录 O：PromQL 语义兼容性保证

### O.1 时间戳处理

PromQL 使用毫秒时间戳，而 InfluxQL 使用纳秒时间戳。转译层在两个单位之间进行转换：

```go
// 毫秒 -> 纳秒
func timeMilliseconds(t time.Time) int64 {
    return t.UnixNano() / int64(time.Millisecond/time.Nanosecond)
}

// 纳秒 -> 毫秒
func durationMilliseconds(d time.Duration) int64 {
    return int64(d / (time.Millisecond / time.Nanosecond))
}
```

### O.2 StaleNaN 语义

Prometheus 的 StaleNaN 有特殊语义：
- 值为 `math.Float64frombits(0x7FF0000000000002)`
- 表示序列"过期"——当一个序列在某个时间步没有新数据时，Prometheus 会插入 StaleNaN
- StaleNaN 不参与任何计算（rate、avg 等都跳过 StaleNaN）
- StaleNaN 会中断 lookback 填充（遇到 StaleNaN 后，之前的数据不再用于填充后续时间步）

openGemini 的实现严格遵循这些语义：
- `FilterInstantNANPoint` / `FilterRangeNANPoint` 在采样前过滤 StaleNaN
- `floatSampler.Aggregate` 在采样时检查 `model.IsStaleNaN`
- `floatSampler.PopulateByPrevious` 在 lookback 填充时检查 StaleNaN

### O.3 Counter Reset 处理

PromQL 的 rate/increase 函数需要处理 counter reset（计数器重置为 0）：
- 当检测到当前值小于前一个值时，认为发生了 counter reset
- 计算方式：先令 `reduceResult = lastValue - firstValue`，再扫描窗口内相邻点，每次 `cur < prev` 就把 `prev` 加回 `reduceResult`（把 reset 前的累计量补上）。例如 `100, 200, 150, 250, 350`：`350-100=250`，在 150<200 处再加 200，得到 450

```go
// CalcReduceResult 中的 counter reset 处理
reduceResult := lastValue - firstValue
if isCounter {
	prev := firstValue
	for _, cur := range prevV {
		if cur < prev {
			reduceResult += prev
		}
		prev = cur
	}
	for _, cur := range currV {
		if cur < prev {
			reduceResult += prev
		}
		prev = cur
	}
}
```

### O.4 外推语义

rate/increase 的外推算法是 Prometheus 最核心的计算逻辑之一，openGemini 的实现与原生 Prometheus 保持一致：

1. **首尾点外推**：根据首尾样本与窗口边界的距离，按比例外推
2. **外推阈值**：距离超过平均采样间隔的 1.1 倍时，只外推半个采样间隔
3. **Counter 从 0 开始**：如果 counter 从 0 开始增长，外推距离不超过到 0 的时间
4. **单点不计算**：窗口内只有一个点时，返回 0（无法计算速率）
