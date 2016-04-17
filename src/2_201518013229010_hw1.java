import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Pattern;

class Hw1Grp2{
    /**
     *<p>
     *   main method of the program
     *</p>
     * @param args  parameters from user input
     * @throws IOException          thrown when error occurred during reading file
     * @throws URISyntaxException   thrown when given wrong URI
     */
    public static void main(String[]args)throws IOException,URISyntaxException{
        String[]splitStr={"=",":"};
        List<String> paras = parseParameters(args,splitStr);
        List<String>content = readHDFSFile(paras.get(0));
        groupBy(content,paras.get(1),paras.get(2));
    }

    /**
     * <p>
     *     parse input parameters.
     *     input parameters:R={input file} groupbykey:{R0} res:{count/max(R1)/avg(R2)}
     * </p>
     * @param args  input parameters from users
     * @param split list containing the split string for each input parameter
     * @return      list of string,containing parsed parameter
     * @throws IllegalArgumentException there must be 3 parameters
     *                                  the first is considered as the input file path,
     *                                  the second must contain groupby,the last one must contain res.
     */
    public static List parseParameters(String[] args,String []split)throws IllegalArgumentException{
        if(args.length != 3){
            throw new IllegalArgumentException("parameters should contain input file,groupby key and result!");
        }
        List<String> res = new ArrayList<>();
	    //get inputfile path
        res.add(args[0].split(split[0])[1]);
        for(int i=1;i<args.length;++i){
            if(args[i].contains("groupby")||args[i].contains("res")){
                String[] tmp = args[i].split(split[1]);
                res.add(tmp[tmp.length-1]);
            }else{
                throw new IllegalArgumentException("invalid parameter:"+args[i]);
            }
        }
        return res;
    }

    /**
     * <p>
     *     read contents in input file.
     * </p>
     * @param path  the path of input file which is in HDFS
     * @return      list of string,each string contains the content of a line in file
     * @throws IOException          io exception
     * @throws URISyntaxException   invalid file path
     */
    public static List readHDFSFile(String path)throws IOException,URISyntaxException{
        //fixed?
        String prefix="hdfs://localhost:9000/";
        String file = prefix+path;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file),conf);
        Path p = new Path(file);
        FSDataInputStream in = fs.open(p);

        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String>res = new ArrayList<>();
        String line;
        while((line=reader.readLine())!=null){
            res.add(line);
        }
        in.close();
        reader.close();
        return res;
    }

    /**
     *<p>
     *     do group by
     *</p>
     * @param content   file contents read from file
     * @param groupCol  group by index,must be R[0-9}*
     * @param outputOperation   operation string descriptors,each one must be (count|max[0-9]*|avg[0-9]*)
     * @throws IllegalArgumentException invalid parameter
     * @throws IllegalStateException    invalid state between maxData and maxDataString
     */
    public static void groupBy(List<String>content,String groupCol,String outputOperation)throws IllegalArgumentException,IllegalStateException{
        if(content == null || content.size() == 0){
            throw new IllegalArgumentException("contents should not be null!");
        }
        if(groupCol == null){
            throw new IllegalArgumentException("no valid groupby column!");
        }
        if(outputOperation == null){
            throw new IllegalArgumentException("no valid operations!");
        }
        String operationSplitStr = ",";
        String columnSplitStr = "\\|";

        int groupByIndex = Integer.parseInt(groupCol.substring(1));

        String[]operations = outputOperation.split(operationSplitStr);

	    String avgPatternStr="^avg\\(R[0-9]*\\)$";
	    String maxPatternStr="^max\\(R[0-9]*\\)$";
	    Pattern avgPattern = Pattern.compile(avgPatternStr);
	    Pattern maxPattern = Pattern.compile(maxPatternStr);

        boolean countOp = false;
        boolean avgOp = false;
        //stores indices of columns which average operates on
        List<Integer> avgColumns = null;
        boolean maxOp = false;
        //stores indices of columns which max operations on
        List<Integer> maxColumns= null;

        List<String> validOperation = new ArrayList<>();
        for(String op:operations){
            boolean  isAvg=false;
            boolean isMax=false;
            int columnIndex = -1;
            if(op.equals("count")) {
                countOp = true;
                validOperation.add(op);
            }else if((isAvg = avgPattern.matcher(op).lookingAt())||(isMax = maxPattern.matcher(op).lookingAt())){

                int colIndexStartIndex = op.indexOf("R");
                int colIndexEndIndex = op.indexOf(")");
                columnIndex = Integer.parseUnsignedInt(op.substring(colIndexStartIndex+1,colIndexEndIndex));
                if(isAvg){
                    avgOp = true;
                    if(avgColumns == null)
                        avgColumns = new ArrayList<>();
                    avgColumns.add(columnIndex);
                }else if(isMax){
                    maxOp = true;
                    if(maxColumns == null)
                        maxColumns = new ArrayList<>();
                    maxColumns.add(columnIndex);
                }
                validOperation.add(op);
            }
        }

        //map graoupbykey to line
        Hashtable<String,List<String>>htable = new Hashtable<>();
        //map groupbykey to average/max,each to a column
        Hashtable<String,List<Double>> avgData = null;
        //we want the precise form of the input
        Hashtable<String,List<String>> maxDataString = null;
        Hashtable<String,List<Double>> maxData = null;
        //map groupbykey to count
        Hashtable<String,Integer>countData =null;
        //the format of average
        DecimalFormat floatFormat = new DecimalFormat("#.00");
        if(avgOp){
            avgData = new Hashtable<>();
        }
        if(maxOp){
            maxData = new Hashtable<>();
            maxDataString = new Hashtable<>();
        }
        for(String record:content) {
           String[]attrs = record.split(columnSplitStr);
            if(attrs.length <= groupByIndex){
                throw new IllegalArgumentException(record + " does not have column used to graoup.!");
            }
            String groupKey = attrs[groupByIndex];
            if(avgOp){
                List<Double>value = avgData.get(groupKey);
                boolean first = false;
                //first time to access one record for the key
                if(value == null) {
                    value = new ArrayList<>(avgColumns.size());
                    first = true;
                }
                int i = 0;
                for(int index:avgColumns){
                    Double data = Double.parseDouble(attrs[index]);
                    if(first) {
                        value.add(i,data);
                    }else {
                        Double val = value.get(i);
                        value.set(i,val + data);
                    }
                    i++;
                }
                avgData.put(groupKey,value);
            }
            if(maxOp){
                List<Double>value = maxData.get(groupKey);
                List<String>valueString = maxDataString.get(groupKey);
                boolean first =false;
                if(value == null||valueString == null) {
                    if(!((value == null)&&(valueString == null))){
                        throw new IllegalStateException("maxData should be in the same state with maxDataStrng!");
                    }
                    value = new ArrayList<>(maxColumns.size());
                    valueString = new ArrayList<>(maxColumns.size());
                    first = true;
                }

                int i=0;
                for(int index:maxColumns){
                    Double data = Double.parseDouble(attrs[index]);
                    if(first) {
                        value.add(i,data);
                        valueString.add(i,attrs[index]);
                    }else {
                        Double val = value.get(i);
                        if(data>val){
                            value.set(i,data);
                            valueString.set(i,attrs[index]);
                        }
                    }
                    i++;
                }
                maxData.put(groupKey,value);
                maxDataString.put(groupKey,valueString);
            }
            List<String>value = htable.get(groupKey);
            if(value == null) {
                value = new ArrayList<>();
            }
            value.add(record);
            htable.put(groupKey, value);
        }
        //calculate count
        if (countOp) {
            if (countData == null)
                countData = new Hashtable<>();
            Iterator ite = htable.entrySet().iterator();
            while (ite.hasNext()) {
                Map.Entry<String, List<String>> entry = (Map.Entry) ite.next();
                countData.put(entry.getKey(), entry.getValue().size());
            }
        }
        //calculate average
        if(avgOp) {
            Iterator ite = htable.entrySet().iterator();
            while (ite.hasNext()) {
                Map.Entry<String, List<String>> entry = (Map.Entry) ite.next();
                String key = entry.getKey();
                int count = entry.getValue().size();

                List<Double> sum = avgData.get(key);
                for (int i = 0; i < sum.size(); ++i) {
                    double avg = sum.get(i) / count;
                    sum.set(i, avg);
                }
                avgData.put(key,sum);
            }
        }
        //make result
        try {
            if(avgOp||maxOp||countOp&&!htable.isEmpty())
                createTable();
            Iterator ite = htable.entrySet().iterator();
            while (ite.hasNext()) {
                Map.Entry<String, List<String>> entry = (Map.Entry) ite.next();
                String key = entry.getKey();
                List<Double>avg = null;
                if(avgOp){
                    avg = avgData.get(key);
                }
                //List<Double>max = maxData.get(key);
                List<String>maxString = null;
                if(maxOp){
                    maxString = maxDataString.get(key);
                }

                int avgIndex = 0, maxIndex = 0;
                for (String op : validOperation) {
                    if (op.contains("avg")&&avgOp) {
                        double val = avg.get(avgIndex);
                        putHbase(key,op,floatFormat.format(val));
                        avgIndex++;
                    } else if (op.contains("max")&&maxOp) {
                        //we want the precise value of the input
                        String valString = maxString.get(maxIndex);
                        putHbase(key,op,valString);
                        maxIndex++;
                    } else if (op.equals("count")&&countOp) {
                        putHbase(key,op,String.valueOf(countData.get(key)));
                    }
                }
            }
            closeHtable();
        }catch (IOException ex){
            ex.printStackTrace();
        }

    }
    //configuration used in hbase operations
    static Configuration conf =null;
    //htable used in operations
    static HTable resTable =null;
    //the table name
    static String tableName="Result";
    //the column family name
    static String columnFamily = "res";

    /**
     * <p>
     *     create the table Result in HBase
     * </p>
     * @throws MasterNotRunningException    HBase associated exception
     * @throws ZooKeeperConnectionException HBase associated exception
     * @throws IOException                  HBase associated exception
     */
    public static void createTable()throws MasterNotRunningException, ZooKeeperConnectionException,IOException{
        if(conf == null)
            conf=new Configuration();
        HBaseAdmin hAdmin = new HBaseAdmin(conf);
        //check whether the table has existed
        if(hAdmin.tableExists(tableName)){
            if(hAdmin.isTableEnabled(tableName))
                hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
        }
        //create new table
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor hcfd = new HColumnDescriptor(columnFamily);
        htd.addFamily(hcfd);
        hAdmin.createTable(htd);
        hAdmin.close();
    }

    /**
     * <p>
     *     put a value in specified key(row key,column falimy,column key)
     *     column family is res for all.
     * </p>
     * @param rowkey    the rowkey in HBase
     * @param column    the column column key
     * @param value     the value in HBase
     * @throws IOException  thrown when failed to put value in table
     */
    public static void putHbase(String rowkey,String column,String value)throws IOException{
        if(conf == null)
            conf=new Configuration();
        if(resTable == null)
            resTable = new HTable(conf,tableName);
        //row key
        Put put = new Put(rowkey.getBytes());
        //column family,column,value
        put.add(columnFamily.getBytes(),column.getBytes(),value.getBytes());
        resTable.put(put);
    }

    /**
     * <p>
     *     close htable when finishing put operations.
     * </p>
     * @throws IOException thrown when failed to close habase table
     */
    public static void closeHtable()throws IOException{
        if(resTable!=null)
            resTable.close();
    }

}
