package spring2018.lab2;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class AAMapper  extends Mapper <LongWritable,Text,Text,Text> {
    
    Map<String, String> codon2aaMap = new HashMap<String, String>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            Path[] codon2aaPath = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(codon2aaPath != null && codon2aaPath.length > 0) {
                codon2aaMap = readFile(codon2aaPath);
            }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
                System.exit(1);
            }
        }
    
    protected HashMap<String, String> readFile(Path[] codonFilePath) {
        HashMap<String, String> codonMap = new HashMap<String, String>();
        BufferedReader cacheReader=null;
        String line=null;
        String[] lineArray=null;
        try{
           cacheReader = new BufferedReader(new FileReader(codonFilePath[0].toString()));
           while((line=cacheReader.readLine())!=null) {
               // Isoleucine      I       ATT, ATC, ATA
                 lineArray = line.split("\\t");
                 String aminoAcid = lineArray[0];
                 String[] sequencesArray = lineArray[2].split(",");
                 for(String sequence: sequencesArray) {
                     codonMap.put(sequence.trim(), aminoAcid.trim());
                 }
           }
        }
        catch(Exception e) { 
            e.printStackTrace(); 
            System.exit(1);
        }
        return codonMap;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, 
        InterruptedException {
        // TODO declare String variable for Text value
    		String str = value.toString();
    	
        // TODO: declare and initialize variables for tokens of each of 3 reading frames
    		
    		int length=str.length()-3;
    		int i=0;
    		while(i<=length)
    		{
    					String str_temp=new String(str.substring(i,i+3));
    					
    					if(codon2aaMap.containsKey(str_temp))
    					{
    					String key_a=new String(codon2aaMap.get(str_temp));
    					context.write(new Text(key_a), new Text("a"));
    					}
    					i=i+3;
    		}
    		i=1;
    		while(i<length)
    		{
    					String str_temp=new String(str.substring(i,i+3));
    					
    					if(codon2aaMap.containsKey(str_temp))
    					{
    					String key_a=new String(codon2aaMap.get(str_temp));
    					context.write(new Text(key_a), new Text("b"));
    					}
    					i=i+3;
    		}
    		i=2;
        	while(i<length)
        	{
    					String str_temp=new String(str.substring(i,i+3));
    					
    					if(codon2aaMap.containsKey(str_temp))
    					{
    					String key_a=new String(codon2aaMap.get(str_temp));
    					context.write(new Text(key_a), new Text("c"));
    					}
    					i=i+3;
    		}
    		
    			
    			
    		
    		
        // TODO: read, process, and write reading frame 1
        // TCA GCC TTT TCT TTG ACC TCT TCT TTC TGT TCA TGT GTA TTT GCT GTC TCT TAG CCC AGA
        // does TCA exist in codon2aaMap?
        // if so, write (key, value) pair to context   

        // TODO: read, process, and write reading frame 2
        // T CAG CCT TTT CTT TGA CCT CTT CTT TCT GTT CAT GTG TAT TTG CTG TCT CTT AGC CCA GA
        // does CAG exist in codon2aaMap?
        // if so, write (key, value) pair to context 
        
        // TODO: read, process, and write reading frame 3
        // TC AGC CTT TTC TTT GAC CTC TTC TTT CTG TTC ATG TGT ATT TGC TGT CTC TTA GCC CAG A
        // does AGC exist in codon2aaMap?
        // if so, write (key, value) pair to context 
       
    }
}