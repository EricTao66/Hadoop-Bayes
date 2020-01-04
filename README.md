# Hadoop-Bayes


## 开发环境

Hadoop软件的版本是2.7.7，Java开发环境jdk版本为1.8.0_231，搭建节点的虚拟机为CentOS，版本为6.5。使用maven管理jar包，maven版本为3.6.2。本工程使用IntelliJ IDEA IDE进行编译和调试，工程项目文件结构如下所示：
```shell
java
├─docCount
│   DocCountDriver.java
│   DocCountMapper.java
│   DocCountReducer.java
│   WholeFileInputFormat.java
│   WholeRecordReader.java
│
├─prediction
│   Evaluation.java
│   PredictDriver.java
│   Prediction.java
│   PredictMapper.java
│   PredictReducer.java
│   PredictTestInputFormat.java
│   PredictTestRecordReader.java
│   TextPair.java
│   Util.java
│
└─wordCount
    WordCountDriver.java
    WordCountMapper.java
    WordCountReducer.java
```


## 数据集


采用训练集80%，测试集20%的原则手动对数据集进行分割。为了预测的准确度，选择数据集较多的类别进行训练和测试，因此选用如下文件：
1. NBCorpus\Country\ALB下包含的81个文件，训练集65个，测试集16个。

2. NBCorpus\Country\ARG下包含的108个文件，训练集87个，测试集21个。

3. NBCorpus\Country\AUSTR下包含的305个文件，训练集244个，测试集61个。

4. NBCorpus\Country\BELG下包含的154个文件，训练集124个，测试集30个。

5. NBCorpus\Country\BRAZ下包含的200个文件，训练集160个，测试集40个。

6. NBCorpus\Country\CANA下包含的263个文件，训练集211个，测试集52个。


一共有6种类别，将分割后的数据传入用三台Linux虚拟机搭建好的分布式集群HDFS中。
```shell 
[hadoop@master ~]$ hadoop fs -put ~/Desktop/input / 
```


## 单机模式，伪分布式模式，分布式模式的切换


在Prediction包的Util类里统一管理文件路径，便于实现程序在单机模式，伪分布式模式和分布式模式之间的切换。
```java
public class Util {

public static final String[] CLASS_NAMES = {"ALB", "ARG", "AUSTR", "BELG", "BRAZ", "CANA"};//训练及测试选取的类别

//    public static final String BASE_PATH = "src/main/resources/";//单机模式
//    public static final String BASE_PATH = "hdfs://localhost:9000/user/erictao/";//伪分布式
    public static final String BASE_PATH = "hdfs://master:9000/";//分布式

    public static final String INPUT_PATH = BASE_PATH + "input/";//输入目录
    public static final String OUTPUT_PATH = BASE_PATH + "output/";//输出目录

    // 获取path路径下所有子文件夹路径
    public static Path[] getPaths(Path path, FileSystem fs)
            throws IOException {
        FileStatus[] stat = fs.listStatus(path);
        Path[] listPath = FileUtil.stat2Paths(stat);
        return listPath;
    }
}
```


## 运行结果

|        | Precision | Recall   | F1       |
| ------ | --------- | -------- | -------- |
| ALB    | 0.516129  | 1.000000 | 0.680851 |
| ARG    | 0.384615  | 0.952381 | 0.547945 |
| AUSTR  | 1.000000  | 0.442623 | 0.613636 |
| BELG   | 0.812500  | 0.866667 | 0.838710 |
| BRAZ   | 0.894737  | 0.850000 | 0.871795 |
| CANA   | 0.950000  | 0.730769 | 0.826087 |
| 微平均 | 0.759664  | 0.807073 | 0.729837 |
| 宏平均 | 0.731818  | 0.731818 | 0.731818 |