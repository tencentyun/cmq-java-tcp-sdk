
# 腾讯云消息队列CMQ JAVA TCP SDK

CMQ tcp协议的java sdk

# 使用方式

## 方式一：直接maven依赖

```xml
<dependency>
    <groupId>com.qcloud</groupId>
    <artifactId>cmq-client</artifactId>
    <version>1.1.0</version>
</dependency>
```
## 方式二：获取源码本地依赖
 - 获取master代码：
    `git clone https://github.com/tencentyun/cmq-java-tcp-sdk.git`
 - 获取指定tag代码，如v1.1.1：
    `git clone --branch v1.1.1 https://github.com/tencentyun/cmq-java-tcp-sdk.git`

# 特性说明
包含所有数据流特性。数据流包括消息相关的能力，如：收发消息、事务消息等等。
包含部分管理流特性，如：队列的创建、订阅主题等。管理流的能力会在后续的版本中逐步补全。

接口的使用示例可以参考demo：https://github.com/tencentyun/cmq-java-tcp-sdk/tree/master/example/src/main/java/demo


其他信息可参考：
1. HTTP 版本sdk（包含管理流）：https://github.com/tencentyun/cmq-java-sdk
2. JSON API文档： https://cloud.tencent.com/document/api/406/5853

