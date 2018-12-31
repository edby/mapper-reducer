package com.mapper.flow;


import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/*treeMap排序*/
public class treeMap {

    public static void main(String[] args) {

        TreeMap<FlowBean, String> tm1 = new TreeMap<>(new Comparator<FlowBean>() {
            @Override
            public int compare(FlowBean o1, FlowBean o2) {
                //指定比较对象，按哪个字段排序

                //数量相同按，phonoe大小排序
                if( o2.getAmountFlow()-o1.getAmountFlow()==0){
                    return o1.getPhone().compareTo(o2.getPhone());
                }
                //总流量大的在前面
                return o2.getAmountFlow()-o1.getAmountFlow();
            }
        }
        );

/*        tm1.put("a",1);
        tm1.put("f",2);
        tm1.put("s",3);
        tm1.put("ax",4);*/
        FlowBean b1 = new FlowBean("1367788", 500, 300);
        FlowBean b2 = new FlowBean("1367766", 400, 200);
        FlowBean b3 = new FlowBean("1367755", 600, 400);
        FlowBean b4 = new FlowBean("1367744", 300, 500);


        tm1.put(b1, null);
        tm1.put(b2, null);
        tm1.put(b3, null);
        tm1.put(b4, null);
        Set<Map.Entry<FlowBean, String>> entries = tm1.entrySet();
        for (Map.Entry<FlowBean, String> entry : entries) {
            System.out.println(entry.getKey()+"\t"+entry.getValue());


        }


    }

}
