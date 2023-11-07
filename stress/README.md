步骤：
1：进入kafka
2：脚本使用方法 ./sndmsg -c xxx.toml

配置文件说明：

[required]
>eip="10.253.31.238" #对应通过dataproxy发送数据
>
>brokerips=["127.0.0.1:9094"] #对应通过kafka发送数据
>
>topics=["mpp_bus_pro","mpp_bus_pro2"] 支持多个topic
>
>schemaname=1 # 切换schema：1--300B无随机数;2--300B随机数
>
>threadsnum=3 # 线程数
>
>recordnum=1 # 每条消息行数
>
>sndnum=20 # 总消息数
>
>runtostop=0 # 总运行时长：单位min
>
[optional]
>flow=false # 是否流量控制,开启后,同时设置令牌间隔时间
>
>flowinterval=0 # 令牌间隔时间:单位ms （计算公式：flowinterval=1000*每条消息大小(1M/s 20M/s)*topic数量/目标流量(200M/s)）
>
[test]
>usemethod=1 # 默认为1,通过dataproxy发送数据;2为通过kafka发送数据 	
