package prediction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class PredictTestRecordReader extends RecordReader<NullWritable, Text> {

    FileSplit split;
    Configuration conf;
    Text value = new Text();
    boolean isProcess = false;
    LineRecordReader reader = new LineRecordReader();

    // 初始化
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException {
        this.split = (FileSplit) split;
        conf = context.getConfiguration();
        reader.initialize(split, context);
    }

    // 逐个读取文件
    @Override
    public boolean nextKeyValue() throws IOException {
        if (!isProcess) {
            String result = "";
            while (reader.nextKeyValue()) {
                result += reader.getCurrentValue() + "\n";
            }
            value.set(result);
            isProcess = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    // 获取当前进度
    @Override
    public float getProgress() throws IOException {
        return reader.getProgress();
    }

    @Override
    public void close() {
    }
}