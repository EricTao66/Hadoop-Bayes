package docCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeRecordReader extends RecordReader<NullWritable, BytesWritable> {

    BytesWritable value = new BytesWritable();
    boolean isProcess = false;
    FileSplit split;
    Configuration configuration;

    // 初始化
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    // 逐个读取文件
    @Override
    public boolean nextKeyValue() {
        if (!isProcess) {
            // 0.缓存区
            byte[] buf = new byte[(int) split.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;
            try {
                // 1.获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);
                // 2.打开文件输入流
                fis = fs.open(path);
                // 3.流的拷贝
                IOUtils.readFully(fis, buf, 0, buf.length);
                // 4.拷贝缓存区的数据到最终输出
                value.set(buf, 0, buf.length);
            } catch (Exception e) {
            } finally {
                IOUtils.closeStream(fis);
                IOUtils.closeStream(fs);
            }
            isProcess = true;
            return true;
        }
        return false;
    }

    // 获取当前键
    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    // 获取当前值
    @Override
    public BytesWritable getCurrentValue() {
        return value;
    }

    // 获取当前进度
    @Override
    public float getProgress() {
        return isProcess ? 1 : 0;
    }

    @Override
    public void close() {
    }
}