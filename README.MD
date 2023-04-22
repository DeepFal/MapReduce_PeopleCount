# PeopleCount



## 这是什么？

这是一个用于 Hadoop 框架的 MapReduce 程序。

用于统计泰坦尼克号中，幸存者的信息：

包括 男性/女性 的 幸存数/死亡数 ，以及总的幸存者数量和未存活者数量。



**使用方法：**

下载原始数据 [Titanic+Data.csv](https://github.com/DeepFal/MapReduce_PeopleCount/blob/main/data/Titanic%2BData.csv) 上传至HDFS中：

```shell
# hdfs dfs -mkdir -p /user/hadoop/input
# hdfs dfs -put ./Titanic+Data.csv /user/hadoop/input
# hdfs dfs -rm -r /user/hadoop/output
```

使用集群模式hadoop运行该MapReduce程序：

```shell
# hadoop jar ./PeopleCount.jar input output
```

使用命令查看统计运行结果：

```shell
# hdfs dfs -cat /user/hadoop/output/*
```

![](https://cc.deepfal.cn/2023/04/23/64441e87524b8.png)





兰州城市学院-大数据201-大三下 作业

课程：大数据开发实践

教师：叶祥飞
