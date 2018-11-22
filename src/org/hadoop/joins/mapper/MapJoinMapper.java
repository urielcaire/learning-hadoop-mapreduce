package org.hadoop.joins.mapper;

import java.util.HashMap;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    // key: store_id  value: store_location
    private HashMap<String, String> stores = new HashMap<String, String>();                   // [ {STR_1:Bangalore} ]

    //key: product_id  value: product_price
    private HashMap<String, String> products = new HashMap<String, String>();               // [{ PR_1:40}

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {

        BufferedReader br = null;
        Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        String line = "";
        for (Path path : cacheFilesLocal)
        {
            if (path.getName().toString().trim().equals("store.txt"))
            {

                br = new BufferedReader(new FileReader(path.toString()));
                line=br.readLine();

                while (line!= null)
                {
                    String storeData[] = line.split(",");        // [ {STR_1} { Bangalore} { Walmart} ]
                    /* store_id, store_location */
                    stores.put(storeData[0].trim(), storeData[1].trim());
                    line=br.readLine();
                }
            }

            else if (path.getName().toString().trim().equals("product.txt"))
            {
                br = new BufferedReader(new FileReader(path.toString()));
                line=br.readLine();

                while (line!= null)
                {
                    String productData[] = line.split(",");            // [ {PR_1} {Shoes} {Sport} {40} ]
                    /* product_id, product_price */
                    products.put(productData[0].trim(), productData[3].trim());
                    line=br.readLine();
                }
            }
        }	}

    @Override
    protected void map(LongWritable key, Text value,  Context context)throws IOException, java.lang.InterruptedException
    {

        String line = value.toString();         //   STR_1,PR_1,06:09:01,7
        /* Split csv string */
        String[] words = line.split(",");     //  [ {STR_1} {PR_1} {06:09:01} {7} ]

        String storeID= words[0];
        int productSale = Integer.parseInt(words[3].trim());              // 7


        int productPrice = Integer.parseInt(products.get(words[1]));         //40
        int revenue = 	productSale*productPrice;

        String location = stores.get(storeID.toString());                // Bangalore

        context.write(new Text ((storeID)+" " + location), new IntWritable (revenue));
    }
}
