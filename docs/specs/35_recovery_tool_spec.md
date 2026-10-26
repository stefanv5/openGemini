# Module 35: 数据恢复工具（ts-recover）深度审计报告

> 代码基线：openGemini `origin/docs/spec-md-chinese` @ `9717d662`（2026-04-11）；本文行号与代码摘录均以 `_spec_verify_20260616/snapshots/spec-md-chinese` 为准。

> ts-recover 是 openGemini 的离线数据恢复 CLI 工具。当数据库因硬件故障、误操作或备份恢复场景需要从备份文件中还原数据时，ts-recover 负责将全量备份和增量备份的数据文件正确地恢复到 openGemini 数据目录中，并通过 HTTP API 通知 meta 节点同步元数据。

---

## 1. ts-recover 是什么？为什么需要它？

```mermaid
sequenceDiagram
    participant Admin as 运维人员
    participant CLI as ts-recover CLI
    participant FS as 文件系统
    participant Backup as 备份目录
    participant Meta as Meta 节点

    Admin->>CLI: 执行恢复命令
    CLI->>Backup: 读取备份日志
    Backup-->>CLI: 备份文件列表 + 元数据

    CLI->>FS: 清理目标数据目录
    CLI->>FS: 移动全量备份文件（rename）
    CLI->>FS: 应用增量备份（新增/删除）
    CLI->>FS: 恢复索引文件

    CLI->>Meta: HTTP POST /recoverMeta
    Meta-->>CLI: HTTP 200（空 body）

    CLI-->>Admin: recover success !
```

**通俗解释**：
ts-recover 就像一个"数据搬运工"。当数据库出问题时，它把备份目录中的文件按照正确的顺序搬回数据目录：
1. **先看清单**：读取备份日志，知道有哪些文件需要恢复
2. **清空场地**：清理目标数据目录，确保干净
3. **搬全量**：把全量备份的文件移动（rename）过来
4. **补增量**：把增量备份中新增的文件加上，删除的文件去掉
5. **通知总部**：告诉 meta 节点"数据恢复好了，请同步元数据"

**核心设计目标**：
1. **两种恢复模式**：全量恢复、全量 + 增量恢复
2. **基于文件清单的批量搬迁**：备份日志记录 measurement → 文件路径组清单；无事务/原子性保证，中断后状态不确定
3. **安全保护**：默认拒绝覆盖已有数据，需 `--force` 显式确认
4. **元数据同步**：通过 HTTP API 通知 meta 节点恢复元数据

---

## 2. CLI 参数与入口

### 2.1 命令行参数

**代码位置**：`app/ts-recover/recover/recover.go:37-47`（`RecoverConfig` 定义；`main.go:49` 只是引用 `recover.RecoverConfig`）

> 注：以下行内注释为解读所加，源码仅有 `Host` 上方的 `// leader meta address`。

```go
type RecoverConfig struct {
    RecoverMode        string  // 恢复模式："1"=全量+增量，"2"=仅全量
    DataDir            string  // openGemini 数据目录路径
    FullBackupDataPath string  // 全量备份文件路径
    IncBackupDataPath  string  // 增量备份文件路径
    SSL                bool    // 是否使用 HTTPS 连接 meta 节点
    InsecureTLS        bool    // 是否跳过 TLS 证书验证
    Force              bool    // 强制覆盖已有数据
    Host               string  // meta 节点地址（默认 127.0.0.1:8091）
}
```

**完整参数说明**：

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `-dataDir` | string | "" (必填) | openGemini 数据目录路径；必须与备份时的原始数据目录一致（它只用于拼出备份内检索根 `<backup>/data_backup/<DataDir>/data` 与 `--force` 删除对象，落地路径来自备份中记录的原始绝对路径） |
| `-recoverMode` | string | "1" | 恢复模式：1=全量+增量，2=仅全量 |
| `-fullBackupDataPath` | string | "" (必填) | 全量备份文件路径 |
| `-incBackupDataPath` | string | "" (模式1必填) | 增量备份文件路径；一次只支持一个增量备份路径 |
| `-ssl` | bool | false | 是否使用 HTTPS |
| `-insecure-tls` | bool | false | 是否跳过 TLS 验证 |
| `-force` | bool | false | 强制覆盖已有数据 |
| `-host` | string | "127.0.0.1:8091" | meta 节点地址 |

**使用示例**：

```bash
# 仅全量恢复
ts-recover \
  -dataDir /var/lib/openGemini \
  -recoverMode 2 \
  -fullBackupDataPath /backup/full_20240101 \
  -host 127.0.0.1:8091

# 全量 + 增量恢复
ts-recover \
  -dataDir /var/lib/openGemini \
  -recoverMode 1 \
  -fullBackupDataPath /backup/full_20240101 \
  -incBackupDataPath /backup/inc_20240102 \
  -host 127.0.0.1:8091

# 强制恢复（覆盖已有数据）
ts-recover \
  -dataDir /var/lib/openGemini \
  -recoverMode 2 \
  -fullBackupDataPath /backup/full_20240101 \
  -force
```

### 2.2 入口函数

**代码位置**：`app/ts-recover/main.go:28-46`

```go
func main() {
    if err := doRun(os.Args[1:]...); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}

func doRun(args ...string) error {
    options, err := ParseFlags(args...)   // 解析命令行参数
    if err != nil {
        return err
    }
    if err := recover.BackupRecover(&options); err != nil {
        return err
    }
    fmt.Println("recover success !")
    return nil
}
```

**逐行解释**：
- **第 29 行**：`main` 调用 `doRun`；`doRun` 的定义在第 35 行（`func doRun(args ...string) error`）
- **第 36 行**：`ParseFlags` 解析命令行参数；当前只校验 `dataDir`，不会在这里校验备份路径
- **第 40 行**：`BackupRecover` 是恢复的核心入口
- **第 43 行**：成功后打印 "recover success !"

---

## 3. 恢复模式选择

```mermaid
sequenceDiagram
    participant Main as BackupRecover()
    participant Mode as 恢复模式判断
    participant Run as runRecover()
    participant Data as recoverData()
    participant Meta as recoverMeta()

    Main->>Mode: 检查 RecoverMode

    alt RecoverMode = "1" (全量+增量)
        Mode->>Run: runRecover(opt, isInc=true)
        Run->>Data: recoverData(rc, dbs, isInc=true)
        Note over Data: 1. 全量备份恢复<br/>2. 增量备份恢复
        Run->>Meta: recoverMeta(rc, isInc=true, dbs)
        Note over Meta: 从增量备份读取元数据
    else RecoverMode = "2" (仅全量)
        Mode->>Run: runRecover(opt, isInc=false)
        Run->>Data: recoverData(rc, dbs, isInc=false)
        Note over Data: 仅全量备份恢复
        Run->>Meta: recoverMeta(rc, isInc=false, dbs)
        Note over Meta: 从全量备份读取元数据
    else 其他值
        Mode-->>Main: error: invalid recovermode
    end
```

**核心代码**：`app/ts-recover/recover/recover.go:51-72`

```go
func BackupRecover(opt *RecoverConfig) error {
    if opt.FullBackupDataPath == "" {
        return fmt.Errorf("`missing required parameter: fullBackupDataPath")
    }
    if opt.RecoverMode == "1" && opt.IncBackupDataPath == "" {
        return fmt.Errorf("`missing required parameter: incBackupDataPath")
    }
    var err error
    switch opt.RecoverMode {
    case FullAndIncRecoverMode:
        err = runRecover(opt, true)
    case FullRecoverMode:
        err = runRecover(opt, false)
    default:
        return fmt.Errorf("invalid recovermode")
    }
    if err != nil {
        return err
    }

    return nil
}
```

**逐行解释**：
- **第 52-54 行**：在 `BackupRecover` 中验证全量备份路径必填
- **第 55-57 行**：模式 1（全量+增量）需要增量备份路径
- **第 60-61 行**：模式 1 调用 `runRecover(opt, true)`
- **第 62-63 行**：模式 2 调用 `runRecover(opt, false)`

**具体例子**：

```
场景：数据库因磁盘损坏需要恢复

已有备份：
  /backup/full_20240101/  ← 1月1日的全量备份
  /backup/inc_20240102/   ← 1月2日的增量备份

使用模式 1（全量+增量）恢复到 1月2日的状态：
  步骤 1：恢复全量备份（1月1日的完整数据）
  步骤 2：应用一个增量备份路径（1月2日的变更）
  步骤 3：同步元数据

使用模式 2（仅全量）恢复到 1月1日的状态：
  步骤 1：恢复全量备份（1月1日的完整数据）
  步骤 2：同步元数据
```

---

## 4. 数据恢复流程

### 4.1 总体流程

```mermaid
sequenceDiagram
    participant Run as runRecover()
    participant DB as getDatabases()
    participant Data as recoverData()
    participant Meta as recoverMeta()

    Run->>DB: 获取需要恢复的数据库列表
    DB-->>Run: dbs = ["db1", "db2", ...]

    Run->>Data: recoverData(rc, dbs, isInc)

    Run->>Meta: recoverMeta(rc, isInc, dbs)
```

**核心代码**：`app/ts-recover/recover/recover.go:74-84`

```go
func runRecover(rc *RecoverConfig, isInc bool) error {
    dbs, err := getDatabases(rc)        // 步骤 1: 获取数据库列表
    if err != nil {
        return err
    }
    if err := recoverData(rc, dbs, isInc); err != nil {  // 步骤 2: 恢复数据文件
        return err
    }
    return recoverMeta(rc, isInc, dbs)  // 步骤 3: 恢复元数据
}
```

### 4.2 获取数据库列表

**代码位置**：`app/ts-recover/recover/recover.go:374-417`

```go
func getDatabases(rc *RecoverConfig) ([]string, error) {
    path := fileops.Join(rc.FullBackupDataPath, backup.BackupLogPath, backup.ResultLog)
    if _, err := os.Stat(path); err != nil {
        return nil, err
    }
    fullRes := &backup.BackupResult{}
    if err := backup.ReadBackupLogFile(path, fullRes); err != nil {
        return nil, err
    }
    databases := make([]string, 0, len(fullRes.DataBases))

    if rc.RecoverMode == FullRecoverMode {
        for db := range fullRes.DataBases {
            databases = append(databases, db)
        }
        return databases, nil
    }

    path = fileops.Join(rc.IncBackupDataPath, backup.BackupLogPath, backup.ResultLog)
    if _, err := os.Stat(path); err != nil {
        return nil, err
    }
    incRes := &backup.BackupResult{}
    if err := backup.ReadBackupLogFile(path, incRes); err != nil {
        return nil, err
    }

    if fullRes.Time > incRes.Time {
        return nil, errors.New("fullBackup time should earlier than incBackup")
    }

    if len(fullRes.DataBases) != len(incRes.DataBases) {
        return nil, errors.New("databases not equal in full Backup and inc Backup")
    }

    for db := range fullRes.DataBases {
        if _, ok := incRes.DataBases[db]; !ok {
            return nil, errors.New("databases not equal in full Backup and inc Backup")
        }
        databases = append(databases, db)
    }

    return databases, nil
}
```

**逐行解释**：
- 读取备份结果日志获取数据库列表；两处 `os.Stat(result)` 在文件缺失时直接返回错误
- 全量模式直接返回
- 增量模式需要验证：全量时间不得晚于增量时间（只有 `fullRes.Time > incRes.Time` 才报错，允许相等），数据库列表一致

### 4.3 数据文件恢复

```mermaid
sequenceDiagram
    participant RD as recoverData()
    participant Check as 目录检查
    participant Clean as 清理数据目录
    participant Full as 全量恢复
    participant Inc as 增量恢复

    RD->>Check: 检查目标数据目录

    alt 有指定数据库
        loop 每个数据库
            Check->>Check: os.Stat(dbPath)
            alt 数据库已存在且未 --force
                Check-->>RD: error: target database file exist
            else 数据库不存在或 --force
                Check->>Clean: os.RemoveAll(dbPath)
            end
        end
    else dbs 为空（整库备份恢复的常态路径）
        Check->>Check: os.Stat(dataPath)
        Note over Clean: RemoveAll 会删除整个 <DataDir>/data
        alt 数据目录存在且未 --force
            Check-->>RD: error: data file exist
        else --force
            Check->>Clean: os.RemoveAll(dataPath)
        end
    end

    RD->>Full: 遍历全量备份日志
    Full->>Full: copyWithFull() 或 copyWithFullAndInc()

    alt isInc = true
        RD->>Inc: 遍历增量备份日志
        Inc->>Inc: copyWithInc()
    end
```

**核心代码**：`app/ts-recover/recover/recover.go:86-139`

```go
func recoverData(rc *RecoverConfig, dbs []string, isInc bool) error {
    dataPath := filepath.Join(rc.DataDir, config.DataDirectory)
    // check clear data path
    if len(dbs) > 0 {
        for _, db := range dbs {
            p := filepath.Join(dataPath, db)
            _, err := os.Stat(p)
            if !rc.Force && err == nil {
                return fmt.Errorf("target database file exist,db : %s.if you still recover,please use --force", db)
            }
            if err := os.RemoveAll(p); err != nil {
                return err
            }
        }
    } else {
        _, err := os.Stat(dataPath)
        if !rc.Force && err == nil {
            return fmt.Errorf("data file exist.if you still recover,please use --force")
        }
        if err := os.RemoveAll(filepath.Join(dataPath)); err != nil {
            return err
        }
    }

    copyFunc := copyWithFull
    if isInc {
        copyFunc = copyWithFullAndInc
    }

    // recover full_backup
    fullBackupDataPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir, dataPath)
    if _, err := os.Stat(fullBackupDataPath); err != nil {
        return errors.New("backupDataPath empty")
    }
    if err := traversalBackupLogFile(rc, fullBackupDataPath, copyFunc, isInc); err != nil {
        return err
    }

    if !isInc {
        return nil
    }

    // recover inc_backup
    incBackupDataPath := filepath.Join(rc.IncBackupDataPath, backup.DataBackupDir, dataPath)
    if _, err := os.Stat(incBackupDataPath); err != nil {
        fmt.Println("incBackupDataPath empty !")
        return nil
    }
    if err := traversalIncBackupLogFile(rc, incBackupDataPath); err != nil {
        return err
    }

    return nil
}
```

**逐行解释**：
- **第 89-108 行**：安全检查 — 如果目标目录已存在且未指定 `--force`，拒绝覆盖
- **第 110-113 行**：根据是否需要增量恢复选择复制函数
- **第 116-122 行**：遍历全量备份日志，执行文件复制
- **第 129-136 行**：如果是增量模式，遍历增量备份日志

**通俗解释**：
数据恢复就像"搬家"：
1. **检查新房**：看看目标目录是否已有数据（安全保护）
2. **清空新房**：如果指定了 `--force`，先清空目标目录
3. **搬大家具**：把全量备份的文件移动（rename）过来
4. **搬小物件**：如果是增量模式，把增量备份的变更也搬过来

---

## 5. 全量备份恢复

### 5.1 copyWithFull — 全量复制

**代码位置**：`app/ts-recover/recover/recover.go:280-299`

```go
func copyWithFull(rc *RecoverConfig, path string) error {
    backupLog := &backup.BackupLogInfo{}
    if err := backup.ReadBackupLogFile(path, backupLog); err != nil {
        return err
    }

    basicPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir)
    for _, fileList := range backupLog.FileListMap {
        for _, files := range fileList {
            srcPath := filepath.Join(basicPath, files[0])
            for _, f := range files {
                if err := backup.FileMove(srcPath, f); err != nil {
                    return err
                }
            }
        }
    }
    return nil
}
```

**逐行解释**：
- **第 282 行**：读取备份日志文件，获取文件列表
- **第 287 行**：`FileListMap` 是 `map[string][][]string`，key 是 measurement 名，value 是文件路径组
- **第 288-291 行**：`files[0]` 是数据文件的**原始绝对路径**（既用于拼出备份内源路径 `basicPath + files[0]`，也是第一个恢复目标）；`files[1..]` 是同一节点上对端分片目录的目标路径。由于 `FileMove` 是 rename，同一源被依次移动到多个目标时，第二个目标会因源已不存在而失败（潜在缺陷）
- **第 291 行**：`backup.FileMove` 执行文件移动（内部为 `fileops.RenameFile → os.Rename`，不是复制）

**具体例子**：

```
备份日志内容（落盘键名带 order 前缀，对应 Go 字段 FileListMap）：
{
  "orderFileListMap": {
    "cpu": [
      ["/var/lib/openGemini/data/db0/0/cpu/00000001-0000-00000001.tssp"]
    ],
    "mem": [
      ["/var/lib/openGemini/data/db0/0/mem/00000001-0000-00000001.tssp"]
    ]
  }
}

恢复过程：
  源路径 = <FullBackupDataPath>/data_backup + files[0]
  cpu 文件：/backup/full_20240101/data_backup/var/lib/openGemini/data/db0/0/cpu/00000001-0000-00000001.tssp
         → /var/lib/openGemini/data/db0/0/cpu/00000001-0000-00000001.tssp（rename，备份侧源文件被搬走）
```

---

## 6. 增量备份恢复

### 6.1 copyWithFullAndInc — 合并全量与增量

**代码位置**：`app/ts-recover/recover/recover.go:301-325`

```go
func copyWithFullAndInc(rc *RecoverConfig, fullPath string) error {
    p, _ := filepath.Split(strings.Replace(fullPath, rc.FullBackupDataPath, rc.IncBackupDataPath, -1))
    incPath := filepath.Join(p, backup.IncBackupLog)
    incBackupLog := &backup.IncBackupLogInfo{}
    _, err := fileops.Stat(incPath)
    if err != nil {
        err = copyWithFull(rc, fullPath)
        return err
    }
    if err := backup.ReadBackupLogFile(incPath, incBackupLog); err != nil {
        return err
    }

    backupLog := &backup.BackupLogInfo{}
    if err := backup.ReadBackupLogFile(fullPath, backupLog); err != nil {
        return err
    }

    err = mergeFileList(rc, backupLog.FileListMap, incBackupLog.DelFileListMap)
    if err != nil {
        return err
    }

    return nil
}
```

**逐行解释**：
- **第 302 行**：将全量备份路径替换为增量备份路径，找到对应的增量日志
- **第 305-309 行**：如果增量日志不存在（`fileops.Stat` 失败），退回纯全量复制
- **第 319 行**：`mergeFileList` 合并文件列表，排除增量中标记为删除的文件

### 6.2 mergeFileList — 文件列表合并

```mermaid
sequenceDiagram
    participant Merge as mergeFileList()
    participant Full as 全量 FileListMap
    participant Del as 增量 DelFileListMap
    participant Copy as FileMove()

    loop 遍历全量 FileListMap 的每个 measurement
        Merge->>Del: 检查该 measurement 的删除列表
        Merge->>Merge: 构建 dListSeen map（标记删除的文件）

        loop 遍历该 measurement 的每个文件
            alt 文件在 dListSeen 中
                Note over Merge: 跳过（增量中标记为删除）
            else 文件不在 dListSeen 中
                Merge->>Copy: FileMove(srcPath, dstPath)
            end
        end
    end
```

**核心代码**：`app/ts-recover/recover/recover.go:348-372`

```go
func mergeFileList(rc *RecoverConfig, listMap, delListMap map[string][][]string) error {
    fullBasicPath := filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir)
    for name, fileList := range listMap {
        // 构建删除列表的查找表
        dListSeen := make(map[string]bool)
        if dLists, ok := delListMap[name]; ok {
            for _, files := range dLists {
                dListSeen[files[0]] = true
            }
        }
        // 复制文件，跳过删除列表中的文件
        for _, files := range fileList {
            if dListSeen[files[0]] {
                continue  // 增量中标记为删除，跳过
            }
            srcPath := filepath.Join(fullBasicPath, files[0])
            for _, f := range files {
                if err := backup.FileMove(srcPath, f); err != nil {
                    return err
                }
            }
        }
    }
    return nil
}
```

**具体例子**：

```
落盘 JSON 键名：orderFileListMap / addOrderFileListMap / delOrderFileListMap
（对应 Go 字段 FileListMap / AddFileListMap / DelFileListMap）

全量备份（1月1日）：
  orderFileListMap = {
    "cpu": [
      ["/var/lib/openGemini/data/db0/0/cpu/00000001-0000-00000001.tssp"],
      ["/var/lib/openGemini/data/db0/0/cpu/00000002-0000-00000001.tssp"],
      ["/var/lib/openGemini/data/db0/0/cpu/00000003-0000-00000001.tssp"]
    ]
  }

增量备份（1月2日）：
  delOrderFileListMap = {
    "cpu": [
      ["/var/lib/openGemini/data/db0/0/cpu/00000002-0000-00000001.tssp"]    ← 1月2日删除了该文件
    ]
  }

合并结果：
  恢复文件：00000001-0000-00000001.tssp、00000003-0000-00000001.tssp
  跳过文件：00000002-0000-00000001.tssp（在增量删除列表中）
```

### 6.3 copyWithInc — 增量文件复制

**代码位置**：`app/ts-recover/recover/recover.go:327-346`

```go
func copyWithInc(rc *RecoverConfig, incPath string) error {
    backupLog := &backup.IncBackupLogInfo{}
    if err := backup.ReadBackupLogFile(incPath, backupLog); err != nil {
        return err
    }

    basicPath := filepath.Join(rc.IncBackupDataPath, backup.DataBackupDir)
    for _, fileList := range backupLog.AddFileListMap {
        for _, files := range fileList {
            srcPath := filepath.Join(basicPath, files[0])
            for _, f := range files {
                if err := backup.FileMove(srcPath, f); err != nil {
                    return err
                }
            }
        }
    }
    return nil
}
```

**逐行解释**：
- `AddFileListMap`（落盘键名 `addOrderFileListMap`）记录增量备份中新增的文件
- 与 `copyWithFull` 类似，但只复制新增文件

---

## 7. 元数据恢复

### 7.1 元数据恢复流程

```mermaid
sequenceDiagram
    participant RD as recoverMeta()
    participant Path as 备份路径选择
    participant File as 读取元数据文件
    participant HTTP as sendRequestToMeta()
    participant Meta as Meta 节点

    RD->>Path: 根据 isInc 选择备份路径
    alt isInc = true
        Path-->>RD: 使用增量备份路径
    else isInc = false
        Path-->>RD: 使用全量备份路径
    end

    RD->>File: 读取 <backupPath>/meta_backup/meta_info.json
    File-->>RD: metaData (JSON 字符串)

    RD->>HTTP: sendRequestToMeta(rc, dbs, metaData)
    HTTP->>Meta: POST /recoverMeta
    Note over HTTP: 参数：<br/>dbs = "db1,db2,..."<br/>metaData = "{...}"
    Meta-->>HTTP: HTTP 200 OK
    HTTP-->>RD: nil (成功)
```

**核心代码**：`app/ts-recover/recover/recover.go:141-155`

```go
func recoverMeta(rc *RecoverConfig, isInc bool, dbs []string) error {
    var backupPath string
    if isInc {
        backupPath = rc.IncBackupDataPath
    } else {
        backupPath = rc.FullBackupDataPath
    }
    backupMetaPath := filepath.Join(backupPath, backup.MetaBackupDir)
    buf, err := os.ReadFile(filepath.Join(backupMetaPath, backup.MetaInfo))
    if err != nil {
        return err
    }
    return sendRequestToMeta(rc, dbs, string(buf))
}
```

### 7.2 HTTP 元数据同步

**代码位置**：`app/ts-recover/recover/recover.go:157-185`

```go
func sendRequestToMeta(rc *RecoverConfig, dbs []string, metaData string) error {
    protocol := "http"
    if rc.SSL {
        protocol = "https"
    }
    urlValues := url.Values{}
    urlValues.Add(backup.DataBases, strings.Join(dbs, ","))
    urlValues.Add(backup.MetaData, metaData)

    Url, err := url.Parse(fmt.Sprintf("%s://%s/recoverMeta", protocol, rc.Host))
    if err != nil {
        return err
    }
    Url.RawQuery = urlValues.Encode()

    transport := &http.Transport{}
    client := &http.Client{
        Transport: transport,
    }
    if rc.InsecureTLS {
        transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
    }
    res, err := client.PostForm(Url.String(), urlValues)
    if err != nil {
        return err
    }
    defer res.Body.Close()
    return resolveResponseError(res)
}
```

**逐行解释**：
- **第 159-161 行**：根据 SSL 配置选择 HTTP/HTTPS 协议
- **第 162-164 行**：构造请求参数：数据库列表 + 元数据 JSON；实际参数 key 是 `dbs` 和 `metaData`
- **第 170 行**：`Url.RawQuery = urlValues.Encode()` 会把参数放进 URL query
- **第 179 行**：`client.PostForm(Url.String(), urlValues)` 又会把同一组参数放进 form body，因此 URL query 和 PostForm 都携带参数；服务端 `recoverMeta` 只解析 `r.URL.Query()`，form body 实际被忽略（这也是参数必须放在 query 的原因）
- **第 176-178 行**：如果配置了 `InsecureTLS`，跳过证书验证
- **第 179-182 行**：发送 POST 请求到 meta 节点的 `/recoverMeta` 接口

**具体例子**：

```
HTTP 请求：
  POST http://127.0.0.1:8091/recoverMeta
  Content-Type: application/x-www-form-urlencoded

  Body:
    dbs=prom,db0,db1
    metaData={"MetaNodes":[...],"DataNodes":[...],"Databases":{...}}   ← meta.Data 的 JSON 快照

  URL query:
    ?dbs=prom%2Cdb0%2Cdb1&metaData=...

成功响应：HTTP 200 OK
失败响应：HTTP 4xx/5xx + 错误信息
```

---

## 8. 备份日志遍历

### 8.1 全量备份日志遍历

```mermaid
sequenceDiagram
    participant Trav as traversalBackupLogFile()
    participant Dir as 目录遍历
    participant Log as 备份日志文件
    participant Copy as 复制函数
    participant Move as 文件移动

    Trav->>Dir: os.Stat(path)（不存在即失败）
    Trav->>Dir: ReadDir(path)

    loop 遍历每个文件/目录
        alt 是目录
            alt 目录名 = "index" 且 isInc=false
                Trav->>Move: FolderMove(src, dst)
                Note over Move: 仅纯全量恢复移动 full index
            else 其他目录
                Trav->>Trav: 递归遍历
            end
        else 是文件
            alt 文件名 = FullBackupLog
                Trav->>Log: 读取备份日志
                Trav->>Copy: copyWithFull() 或 copyWithFullAndInc()
                Trav->>Move: FileMove(src, dst)
                Note over Move: 移动日志文件到目标目录
            end
        end
    end
```

**核心代码**：`app/ts-recover/recover/recover.go:199-237`

```go
func traversalBackupLogFile(rc *RecoverConfig, path string, fn RecoverFunc, isInc bool) error {
    var err error
    var fds []fs.FileInfo

    if _, err = os.Stat(path); err != nil {
        return err
    }
    if fds, err = fileops.ReadDir(path); err != nil {
        return err
    }

    for _, fd := range fds {
        srcfp := filepath.Join(path, fd.Name())
        if fd.IsDir() {
            if fd.Name() == "index" && !isInc {
                // 索引目录直接移动（仅纯全量模式）
                outPath := strings.Replace(srcfp, filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir), "", -1)
                if err := backup.FolderMove(srcfp, outPath); err != nil {
                    return err
                }
                continue
            }
            // 递归遍历子目录
            if err = traversalBackupLogFile(rc, srcfp, fn, isInc); err != nil {
                return err
            }
        } else {
            if fd.Name() == backup.FullBackupLog {
                // 找到备份日志文件，执行复制
                if err := fn(rc, srcfp); err != nil {
                    return err
                }
                // 移动日志文件
                outPath := strings.Replace(srcfp, filepath.Join(rc.FullBackupDataPath, backup.DataBackupDir), "", -1)
                if err := backup.FileMove(srcfp, outPath); err != nil {
                    return err
                }
            }
        }
    }
    return nil
}
```

全量+增量恢复时，遍历全量备份日志传入的 `isInc=true`，所以 full backup 里的 `index/` 不会在这一步移动；随后遍历增量备份时才会移动 inc backup 中的 index 相关内容。**后果**：模式 1 下全量备份的索引在整个流程中没有任何一步会搬回——一旦增量备份里没有对应的 index 目录，恢复后就会「数据文件在、索引缺失」，这是该工具最需要提示的风险之一。

---

## 9. 端到端实战：一次完整恢复

### 9.1 场景设定

```
数据库：prom, db0
全量备份：/backup/full_20240101/
增量备份：/backup/inc_20240102/
数据目录：/var/lib/openGemini/

全量备份内容：
  /backup/full_20240101/
    data_backup/var/lib/openGemini/data/prom/
      0/cpu/00000001-0000-00000001.tssp
      0/cpu/00000002-0000-00000001.tssp
      0/backup_log/full_backup_log.json  ← 该 shard 的文件清单
      0/<rp>/index/...                   ← 索引目录（随数据一起备份）
    data_backup/var/lib/openGemini/data/db0/
      0/mem/00000001-0000-00000001.tssp
      0/backup_log/full_backup_log.json
    backup_log/result  ← 备份结果（BackupLogPath=/backup_log，ResultLog=result，无 .json 后缀）

增量备份内容：
  /backup/inc_20240102/
    data_backup/var/lib/openGemini/data/prom/
      0/cpu/00000003-0000-00000001.tssp  ← 新增
      0/backup_log/inc_backup_log.json  ← 增量日志
        addOrderFileListMap: { "cpu": [["/var/lib/openGemini/data/prom/0/cpu/00000003-0000-00000001.tssp"]] }
        delOrderFileListMap: { "cpu": [["/var/lib/openGemini/data/prom/0/cpu/00000002-0000-00000001.tssp"]] }  ← 删除 cpu_002
```

### 9.2 恢复时序图

```mermaid
sequenceDiagram
    participant CLI as ts-recover
    participant DB as getDatabases()
    participant Data as recoverData()
    participant Meta as recoverMeta()

    CLI->>DB: 读取全量+增量 backup_log/result
    DB-->>CLI: dbs = ["prom", "db0"]

    CLI->>Data: recoverData(rc, dbs, isInc=true)

    Note over Data: ===== 步骤 1: 清理目标目录 =====
    Data->>Data: os.RemoveAll(/var/lib/.../data/prom)
    Data->>Data: os.RemoveAll(/var/lib/.../data/db0)

    Note over Data: ===== 步骤 2: 恢复全量备份 =====
    Data->>Data: 遍历 full_backup_log.json
    Data->>Data: copyWithFullAndInc(fullPath)
    Note over Data: 合并全量文件列表与增量删除列表
    Data->>Data: FileMove: 00000001-0000-00000001.tssp ✓
    Data->>Data: 跳过: 00000002-0000-00000001.tssp（在删除列表中）
    Data->>Data: FileMove: 00000001-0000-00000001.tssp ✓
    Data->>Data: full index/ 不移动（full+inc 模式）

    Note over Data: ===== 步骤 3: 恢复增量备份 =====
    Data->>Data: 遍历 inc_backup_log.json
    Data->>Data: copyWithInc(incPath)
    Data->>Data: FileMove: 00000003-0000-00000001.tssp ✓（新增文件）
    Data->>Data: inc index/ 移动

    Note over Data: ===== 步骤 4: 移动日志文件 =====
    Data->>Data: FileMove: full_backup_log.json
    Data->>Data: FileMove: inc_backup_log.json

    CLI->>Meta: POST /recoverMeta
    Note over Meta: dbs=prom,db0<br/>metaData=...
    Meta-->>CLI: HTTP 200 OK

    CLI-->>CLI: recover success !
```

### 9.3 恢复结果

```
恢复前：
  /var/lib/openGemini/data/  ← 空或已有数据

恢复后：
  /var/lib/openGemini/data/
    prom/
      0/cpu/00000001-0000-00000001.tssp  ← 全量备份
      0/cpu/00000003-0000-00000001.tssp  ← 增量新增
      # 00000002-0000-00000001.tssp 被增量删除，未恢复
      0/<rp>/index/...                   ← 索引（模式 1 下来自增量备份）
    db0/
      0/mem/00000001-0000-00000001.tssp  ← 全量备份
```

---

## 10. 安全保护机制

### 10.1 数据覆盖保护

```mermaid
flowchart TD
    A[开始恢复] --> B{目标数据库存在?}
    B -->|是| C{--force 参数?}
    C -->|否| D[报错: target database file exist<br/>please use --force]
    C -->|是| E[os.RemoveAll 清理]
    B -->|否| E
    E --> F[复制备份文件]
```

**代码位置**：`app/ts-recover/recover/recover.go:89-108`

```go
// 安全检查：防止误覆盖
if !rc.Force && err == nil {
    return fmt.Errorf("target database file exist,db : %s.if you still recover,please use --force", db)
}
```

### 10.2 备份完整性验证

**代码位置**：`app/ts-recover/recover/recover.go:401-407`

```go
// 验证全量备份时间早于增量备份
if fullRes.Time > incRes.Time {
    return nil, errors.New("fullBackup time should earlier than incBackup")
}

// 验证数据库列表一致
if len(fullRes.DataBases) != len(incRes.DataBases) {
    return nil, errors.New("databases not equal in full Backup and inc Backup")
}
```

### 10.3 TLS 支持

| 配置 | 说明 |
|------|------|
| `--ssl` | 使用 HTTPS 连接 meta 节点 |
| `--insecure-tls` | 跳过 TLS 证书验证（测试环境用） |

---

## 11. 潜在隐患

### 11.1 恢复中断无断点续传

```mermaid
sequenceDiagram
    participant CLI as ts-recover
    participant FS as 文件系统

    CLI->>FS: 开始移动文件（rename）...
    Note over CLI: 移动了 100 个文件后崩溃

    CLI-->>FS: 重启恢复
    Note over CLI: 备份目录已被搬空<br/>既无断点，也无法简单重跑
```

**隐患**：
- `FileMove` 是 rename：每搬一个文件，备份目录里的源文件就消失了。中断后既没有断点记录，也不能简单「从头重跑」——已搬走的源文件会让重跑报 `no such file`
- 备份与数据目录不在同一文件系统时，`os.Rename` 会返回 EXDEV，恢复直接失败
- 对于大型数据库（TB 级），恢复时间可能很长
- 建议：改用「复制 + 校验 + 删除」并记录进度，实现断点续传

### 11.2 增量恢复的删除列表可能不完整

```
场景：
  全量备份：file1, file2, file3
  增量备份：删除 file2，新增 file4

  如果增量备份的 DelFileListMap 丢失：
    → file2 仍会被恢复（不应该恢复）
    → 数据不一致
```

### 11.3 元数据恢复无回滚机制

```mermaid
sequenceDiagram
    participant CLI as ts-recover
    participant Meta as Meta 节点

    CLI->>Meta: POST /recoverMeta

    alt 元数据恢复成功
        Meta-->>CLI: HTTP 200
    else 元数据恢复失败
        Note over CLI: 数据文件已恢复<br/>但元数据未同步<br/>数据不一致！
    end
```

**隐患**：
- 如果数据文件恢复成功但元数据恢复失败，会导致数据不一致
- 建议：实现事务性恢复，支持回滚

---

## 12. 总结：ts-recover 的关键设计

| 设计 | 目的 | 效果 |
|------|------|------|
| 两种恢复模式 | 灵活的恢复策略 | 支持仅全量和全量+增量 |
| 备份日志清单 | 记录 measurement → 文件路径组 | 支持增量删除和新增（无事务/断点保证） |
| --force 保护 | 防止误覆盖 | 默认拒绝覆盖已有数据 |
| HTTP 元数据同步 | 恢复 meta 节点数据 | 数据和元数据一致 |
| 递归目录遍历 | 处理复杂的目录结构 | 支持多数据库、多 shard |
| 文件列表合并 | 处理增量删除 | 正确恢复到目标时间点 |
