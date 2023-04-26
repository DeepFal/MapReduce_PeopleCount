import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PeopleCount {
    public PeopleCount() {
    }
    //主函数
    public static void main(String[] args) throws Exception {
        //创建Configuration对象,用于读取配置文件
        Configuration conf = new Configuration();
        //getRemainingArgs()返回除了程序名以外的参数,即输入输出路径,如果参数个数小于2,则打印提示信息并退出程序
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar peoplecount.jar <in> [<in>...] <out>");
            System.exit(2);
        }

        //创建Job对象,用于封装本次作业的相关信息
        Job job = Job.getInstance(conf, "people count");
        //设置主类
        job.setJarByClass(PeopleCount.class);
        //设置Mapper类
        job.setMapperClass(PeopleCountTokenizerMapper.class);
        //设置Combiner类
        job.setCombinerClass(PeopleCountReducer.class);
        //设置Reducer类
        job.setReducerClass(PeopleCountReducer.class);
        //设置输出键类型
        job.setOutputKeyClass(Text.class);
        //设置输出值类型
        job.setOutputValueClass(IntWritable.class);

        //循环遍历输入路径,将输入路径添加到FileInputFormat对象中
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        //设置输出路径,将输出路径添加到FileOutputFormat对象中
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        //等待作业完成,退出程序.waitForCompletion()返回true表示作业成功完成,否则表示作业失败
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}