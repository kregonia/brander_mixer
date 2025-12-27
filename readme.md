# brander mixer

brander mixer 是一个任务系统，用于在复杂字符分割后在边缘设备上执行分布式计算。

![GitHub](https://img.shields.io/github/license/kregonia/brander_mixer)
![GitHub release](https://img.shields.io/github/v/release/kregonia/brander_mixer)

## 功能特性

- ✨ 高性能信息传输
- 🚀 动态设备扩缩容
- 💡 智能任务拆分
- 🔧 异常自动修复

## 快速开始

### 前置要求

在开始之前，请确保你的环境安装了以下软件：
- go 1.21+
- make
- protoc-gen-go
- protoc-gen-go-grpc
- protobuf@3
- protoc-gen-grpc-gateway

### 安装步骤

1. 克隆项目
```bash
git clone https://github.com/kregonia/brander_mixer.git
go mod init github.com/kregonia/brander_mixer
go mod tidy
```

2. 安装依赖
```bash
// 必要工具链
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest

cd brander_mixer
make prepare
make build
```
3. 配置环境
```bash
# 测试环境
export APP_ENV=dev
# 生产环境
export APP_ENV=production
```
4. 运行项目
```bash
./bin/disk_status_reader
```

## 使用方法

### 基本使用
```bash
./bin/disk_status_reader -h : 打印帮助信息
./bin/disk_status_reader -in file1 -out file2 : 解序列化file2为可读文件
```

### 高级配置
如果需要更高级的功能，可以这样配置：
```bash
```

## 项目结构
```bash
./
├── bootstrap
├── cmd
├── go.mod
├── LICENSE
├── log
├── main.go
├── model
│   ├── _const
│   └── task
├── readme.md
├── resource
└── script
```

## 贡献指南
我们欢迎任何形式的贡献！请阅读以下指南：
1. Fork 本仓库
2. 创建特性分支 (git checkout -b feature/AmazingFeature)
3. 提交更改 (git commit -m 'Add some AmazingFeature')
4. 推送到分支 (git push origin feature/AmazingFeature)
5. 开启 Pull Request

## 许可证
本项目采用 Apache 2.0 License - 查看 LICENSE 文件了解详情。

## 致谢
- [AtlasBegonia](https://https://github.com/AtlasBegonia)
---
如果这个项目对你有帮助，请给个 ⭐️ 支持一下！