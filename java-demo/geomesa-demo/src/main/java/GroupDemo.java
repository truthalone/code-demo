import org.geotools.data.DataStore;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.SortByImpl;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.filter.expression.FastPropertyName;
import org.locationtech.geomesa.hbase.filters.CqlTransformFilter;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.parboiled.common.StringUtils;
import utils.BaseUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 聚合运算demo
 */
public class GroupDemo {
    public static void main(String[] args) {
        Map<String, String> params = new HashMap<>();
        params.put("hbase.zookeepers", "master:2181,slave1:2182,slave2:2183");
        params.put("hbase.catalog", "mycatalog");

        //        GroupDemo groupDemo = new GroupDemo();
//        groupDemo.insert(params);
//        groupDemo.search(params);
    }


    private static final String FEATURE_NAME = "group_demo";

    public void insert(Map<String, String> params) {
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建datastore失败");
            return;
        }

        String spec = "name:String,count:Double";
        SimpleFeatureType sft = BaseUtils.createSimpleFeatureType(FEATURE_NAME, spec);

        boolean bret = BaseUtils.createSchema(dataStore, sft);
        if (!bret) {
            System.out.println("创建schema失败");
            return;
        }

        //写入数据
        InputStream inputStream = null;
        try {
            inputStream = GroupDemo.class.getResource("example_data/group_data.csv").openStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "GBK");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            //跳过第一行
            String content = bufferedReader.readLine();
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
            List<SimpleFeature> featureList = new ArrayList<>();
            int count = 0;
            while (true) {
                content = bufferedReader.readLine();
                if (StringUtils.isEmpty(content)) {
                    break;
                }

                String[] arrs = content.split(",");
                if (arrs.length < 2) {
                    continue;
                }

                builder.set("name", arrs[0]);
                builder.set("count", Double.parseDouble(arrs[1]));
                SimpleFeature feature = builder.buildFeature(String.valueOf(count));
                count += 1;

                featureList.add(feature);
            }

            BaseUtils.writeFeatures(dataStore, sft, featureList);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }finally {
            if(inputStream!=null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    public void search(Map<String, String> params) {
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建datastore失败");
            return;
        }

        FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = null;
        try {
            Query query = new Query(FEATURE_NAME, ECQL.toFilter("name='山东'"));
            query.setSortBy(new SortBy[]{new SortByImpl(new FastPropertyName.FastPropertyNameAttribute("name", 1), SortOrder.ASCENDING)});

            //            query.getHints().put(QueryHints.STATS_STRING(), "GroupBy(\"name\",Count())");
//            query.getHints().put(QueryHints.STATS_STRING(), "MinMax(\"count\")");
//            query.getHints().put(QueryHints.STATS_STRING(), "Enumeration(\"count\")");
//            query.getHints().put(QueryHints.STATS_STRING(), "TopK(\"name\")");
//            query.getHints().put(QueryHints.STATS_STRING(), "Frequency(\"name\",50)");
//            query.getHints().put(QueryHints.STATS_STRING(), "GroupBy(\"name\",MinMax(\"count\"))");
//            query.getHints().put(QueryHints.STATS_STRING(), "GroupBy(\"name\")");
//            query.getHints().put(QueryHints.STATS_STRING(), "GroupBy(\"name\",DescriptiveStats(\"count\"))");
//            query.getHints().put(QueryHints.STATS_STRING(), "GroupBy(\"name\",IteratorStackCount())");
            //query.getHints().put(QueryHints.ENCODE_STATS(),true)


            featureReader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);
            if (featureReader == null) {
                System.out.println("查询失败");
                return;
            }

            while (featureReader.hasNext()) {
                SimpleFeature feature = featureReader.next();
                List<Object> attributes = feature.getAttributes();
                for (int i = 0; i < attributes.size(); i++) {
                    System.out.print(attributes.get(i).toString() + "  ");
                }
            }

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            if (featureReader != null) {
                try {
                    featureReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
