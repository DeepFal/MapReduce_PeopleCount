import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

//统计人数,继承Reducer类,设置输入类型<Text, IntWritable>和输出类型<Text, IntWritable>
public class PeopleCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //设置输出变量result,类型为IntWritable,初始值为0,用于存储最终结果
    private IntWritable result = new IntWritable();

    public PeopleCountReducer() {
    }


    //这个函数接收三个参数：一个Text类型的key，一个IterableWritable>类型的values和一个Reducer, IntWritable, Text, IntWritable>.Context类型的context。
    //其中，key是输入的键，values是与该键相关的值的集合，context是Reducer的上下文对象，用于输出结果。
    //在函数体内，使用for循环遍历values集合，将集合中的每个元素的值累加到sum变量中，最后将结果写入context中。
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        this.result.set(sum);
        context.write(key, this.result);
    }
}