package spring2018.lab2;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;

public class AAReducer  extends Reducer <Text,Text,Text,Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
		   throws IOException, InterruptedException {
        
        // TODO: initialize integer sums for each reading frame
    		int sum1=0;
    		int sum2=0;
    		int sum3=0;
    		
    		for (Text value: values)
    		{
    			if(value.toString().equals("a"))
    			{
    				sum1+=1;
    			}
    			else if(value.toString().equals("b"))
    			{
    				sum2+=1;
    			}
    			
    			else if(value.toString().equals("c"))
    			{
    				sum3+=1;
    			}
    		}
    		if(key.getLength()>=8)
    		{
    		context.write(new Text(key), new Text("\t"+sum1+" "+sum2+" "+sum3));
    		}
    		else
    		{
        		context.write(new Text(key), new Text("\t\t"+sum1+" "+sum2+" "+sum3));

    		}
        
        // TODO: loop through Iterable values and increment sums for each reading frame
       
        // TODO: write the (key, value) pair to the context
        // TODO: consider how to use tabs to format output correctly
               
	  
   }
}