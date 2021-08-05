# 介绍
- 一个基于sarama的kafka简易命令行工具

# 用法
```shell
$ ./kafkacli 
kafka简易命令行工具(v0.0.1)

Usage:
  kafkacli [command]

Available Commands:
  brokers        列出所有broker信息
  completion     generate the autocompletion script for the specified shell
  consume        同步消费一条消息
  consumergroups 列出所有消费者组（0.8版本存储在zookeeper中的消费者组信息将不再展示）
  help           Help about any command
  lag            列出某个消费者组的lag情况（需要指定topic）
  leader         列出某个topic的leader副本情况
  offset         列出某个topic当前最老、最新offset
  partitions     列出某个topic所有partition
  produce        同步生产一条消息
  replicas       列出某个topic所有partition的副本，以及ISR
  topics         列出所有topic

Flags:
      --broker-addresses strings   broker地址 (default [127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093])
      --broker-version string      broker版本号 (default "0.10.2.1")
      --compress string            消息压缩算法，包括gz、snappy、none（无压缩） (default "none")
      --consume-offset int         你想消费的具体开始的offset，-1是最新，-2是最老，非负数为具体offset (default -1)
      --consume-partition int32    你想消费的具体的partition (default -1)
      --consumer-group string      具体消费者组名称
  -h, --help                       help for kafkacli
      --produce-message string     你想发送的消息
      --topic string               具体topic名称
      --verbose                    是否打印debug日志

Use "kafkacli [command] --help" for more information about a command.
```