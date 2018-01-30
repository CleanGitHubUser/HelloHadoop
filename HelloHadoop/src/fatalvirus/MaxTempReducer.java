package fatalvirus;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer 
	extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text key, 
			Iterable<IntWritable> values,
			Context context) 
					throws IOException, InterruptedException {
		
		// 최고온도를 0으로 설정
		int maxValue = Integer.MIN_VALUE;
		
		// 맵의 결과로 추출된 년도와 온도가 들어 있는
		// values 를 for문으로 순환하며
		// 최고온도를 조사
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		
		// 최고 온도를 결과로 출력
		context.write(key,
				new IntWritable(maxValue));
		
	}


}
