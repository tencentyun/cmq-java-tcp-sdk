
# 腾讯云消息队列CMQ JAVA TCP SDK

CMQ tcp协议的java sdk

# 使用方式

## 方式一：直接maven依赖
新版的TCP SDK的`artifactId`从`cmq-client`变更为`cmq-tcp-client`
### 新版SDK
```xml
<dependency>
    <groupId>com.qcloud</groupId>
    <artifactId>cmq-tcp-client</artifactId>
    <version>1.1.4</version>
</dependency>
```


### 老版本SDK
老版本的artifactId是cmq-client，建议切换到新的版本
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
 - 获取指定tag代码，如v1.1.8：
 
    `git clone --branch v1.1.8 https://github.com/tencentyun/cmq-java-tcp-sdk.git`

# 特性说明
包含所有数据流特性。数据流包括消息相关的能力，如：收发消息、删除消息、事务消息等等。

包含部分管理流特性，如：队列的创建、订阅主题等。管理流的能力会在后续的版本中逐步补全。

接口的使用示例可以参考demo：https://github.com/tencentyun/cmq-java-tcp-sdk/tree/master/example/src/main/java/demo


其他信息可参考：
1. HTTP 版本sdk（包含管理流）：https://github.com/tencentyun/cmq-java-sdk
2. JSON API文档： https://cloud.tencent.com/document/api/406/5853

