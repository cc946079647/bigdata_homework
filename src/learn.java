import java.text.DecimalFormat;
import java.util.*;

class learn{
    public static void main(String[]args){
        //map groupbykey to average/max,each to a column
        Hashtable<String,List<Double>> avgData = new Hashtable<>();
        List<Double> data = new ArrayList<>();
        data.add(0,1.0);
        data.add(1,1.0);
        data.add(2,1.0);
        avgData.put("key",data);
        List<Double>value = avgData.get("key");
        value.set(0,2.0);
        value.set(1,2.0);
        value.set(2,2.0);
        avgData.put("key",value);
        Iterator ite = avgData.entrySet().iterator();
        while (ite.hasNext()) {
            Map.Entry<String, List<Double>> entry = (Map.Entry) ite.next();
            String key = entry.getKey();
            System.out.print(key+":");

            List<Double> sum = avgData.get(key);
            for (int i = 0; i < sum.size(); ++i) {
                System.out.print(sum.get(i)+"\t");
            }
            System.out.println();
        }

    }
}