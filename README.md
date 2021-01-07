# miniRedis
此项目是使用Python实现的类Redis数据库服务器。该数据库服务器通过实现Redis通信协议来处理客户端请求并发送响应；可以响应GET/SET/DELETE/FLUSH/MGET/MSET等多种命令；支持字符串和二进制数据、数值、NULL、数组、字典、错误信息等数据类型；使用gevent异步处理多个客户端请求；并实现了持久化和恢复功能。
