import org.geotools.data.DataStore;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.SortByImpl;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geomesa.filter.expression.FastPropertyName;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.sort.SortBy;
import org.opengis.filter.sort.SortOrder;
import org.parboiled.common.StringUtils;
import utils.BaseUtils;
import utils.DateUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * geomesa api 操作Demo
 */
public class GeomesaDemo {
    private static final String FEATURE_NAME = "demo1";

    public static void main(String[] args) {
        Map<String, String> params = new HashMap<>(4);
        params.put("hbase.zookeepers", "10.28.206.51:2181,10.28.206.52:2182,10.28.206.53:2183");
        params.put("hbase.catalog", "geomesa_learn");


        GeomesaDemo geomesaDemo = new GeomesaDemo();
//        geomesaDemo.insertDemoData(params);
//        geomesaDemo.attributeQuery1(params);
//        geomesaDemo.attributeQuery2(params);
//        geomesaDemo.spaceQuery(params);
//        geomesaDemo.dateTimeQuery(params);
//        geomesaDemo.deleteDemoFeature(params);
    }

    /**
     * 插入数据
     *
     * @param params
     */
    void insertDemoData(Map<String, String> params) {
        //1. 创建datastore
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建dataStore失败");
            return;
        }

        //2. 创建sft
        String spec = "fid:Integer,dtg:Date,lon:Double,lat:Double,geom:Point:srid=4326";
        SimpleFeatureType sft = BaseUtils.createSimpleFeatureType(FEATURE_NAME, spec);
        if (sft == null) {
            System.out.println("创建sft失败");
            return;
        }

        //3. 创建schema
        boolean bret = BaseUtils.createSchema(dataStore, sft);
        if (!bret) {
            System.out.println("创建bret失败");
            return;
        }


        //4. 写入数据
        InputStream inputStream = null;
        try {
            inputStream = GeomesaDemo.class.getResource("example_data/points.csv").openStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            //跳过第一行
            String content = null;
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(sft);
            List<SimpleFeature> featureList = new ArrayList<>();
            int count = 0;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm");
            GeometryFactory factory = new GeometryFactory();

            while (true) {
                content = bufferedReader.readLine();
                if (StringUtils.isEmpty(content)) {
                    break;
                }

                String[] arrs = content.split(",");
                if (arrs.length < 4) {
                    continue;
                }

                builder.set("fid", Integer.parseInt(arrs[0]));

                Date date = simpleDateFormat.parse(arrs[1]);
                builder.set("dtg", date);

                Double lon = Double.parseDouble(arrs[2]);
                Double lat = Double.parseDouble(arrs[3]);

                builder.set("lon", lon);
                builder.set("lat", lat);

                Coordinate coordinate = new Coordinate(lon, lat);
                Point point = factory.createPoint(coordinate);
                builder.set("geom", point);

                SimpleFeature feature = builder.buildFeature(String.valueOf(count));
                count += 1;

                featureList.add(feature);

                //达到最大写入数量时，写入一批数据
                int maxWriteSize = 1000;
                if (featureList.size() == maxWriteSize) {
                    BaseUtils.writeFeatures(dataStore, sft, featureList);
                    featureList.clear();
                }
            }

            //写入剩余数据
            BaseUtils.writeFeatures(dataStore, sft, featureList);

            System.out.println("写入数据完成");
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

    /**
     * 简单属性查询
     */
    void attributeQuery1(Map<String, String> params) {
        //1. 创建datastore
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建datastore失败");
            return;
        }

        //2. 构建查询条件
        StringBuffer buffer = new StringBuffer();
        buffer.append("fid=100");

        FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = null;
        try {
            Query query = new Query(FEATURE_NAME, ECQL.toFilter(buffer.toString()));
            featureReader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            while (featureReader.hasNext()) {
                SimpleFeature simpleFeature = featureReader.next();
                //获取所有属性列表
//                List<Object> attributes = simpleFeature.getAttributes();
//
//                int fid = Integer.parseInt(attributes.get(0).toString());
//                String dtg = attributes.get(1).toString();
//                dtg = DateUtils.ConvertUTCToStandardDate(dtg);
//
//                double lon = Double.parseDouble(attributes.get(2).toString());
//                double lat = Double.parseDouble(attributes.get(3).toString());
//                String geom = attributes.get(4).toString();


                //根据属性名称或者索引获取属性
                int fid = Integer.parseInt(simpleFeature.getAttribute("fid").toString());
                String dtg = simpleFeature.getAttribute("dtg").toString();
                dtg = DateUtils.ConvertUTCToStandardDate(dtg);

                double lon = Double.parseDouble(simpleFeature.getAttribute("lon").toString());
                double lat = Double.parseDouble(simpleFeature.getAttribute("lat").toString());
                String geom = simpleFeature.getAttribute("geom").toString();


                System.out.println(fid + "-->" + dtg + "-->" + lon + "-->" + lat + "-->" + geom);
            }
        } catch (Exception e) {
            e.printStackTrace();
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


    /**
     * 多条件查询
     * 排序 和 限制返回记录数
     *
     * @param params
     */
    void attributeQuery2(Map<String, String> params) {
        //1. 创建datastore
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建datastore失败");
            return;
        }

        //2. 构建查询条件
        StringBuffer buffer = new StringBuffer();
        buffer.append("fid> 100 and fid<= 300");

        FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = null;
        try {
            //设定要查询的字段
            Query query = new Query(FEATURE_NAME, ECQL.toFilter(buffer.toString()), new String[]{"fid", "dtg", "geom"});

            //设定最大返回记录数
            query.setMaxFeatures(20);

            //设定结果排序
            query.setSortBy(new SortBy[]{new SortByImpl(new FastPropertyName.FastPropertyNameAttribute("fid", 1), SortOrder.DESCENDING)});
            featureReader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            while (featureReader.hasNext()) {
                SimpleFeature simpleFeature = featureReader.next();
                List<Object> attributes = simpleFeature.getAttributes();

                int fid = Integer.parseInt(attributes.get(0).toString());
                String dtg = attributes.get(1).toString();
                dtg = DateUtils.ConvertUTCToStandardDate(dtg);

                String geom = attributes.get(2).toString();

                System.out.println(fid + "-->" + dtg + "-->" + "-->" + geom);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

    /**
     * 空间查询
     *
     * @param params
     */
    void spaceQuery(Map<String, String> params) {
        //1. 创建datastore
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建datastore失败");
            return;
        }

        //2. 构建查询条件
        StringBuffer buffer = new StringBuffer();
        buffer.append("DWITHIN(geom,POINT (116.32674 39.89577),500,meters)");

        FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = null;
        try {
            //设定要查询的字段
            Query query = new Query(FEATURE_NAME, ECQL.toFilter(buffer.toString()), new String[]{"fid", "dtg", "geom"});

            //设定最大返回记录数
            query.setMaxFeatures(20);

            //设定结果排序
            query.setSortBy(new SortBy[]{new SortByImpl(new FastPropertyName.FastPropertyNameAttribute("fid", 1), SortOrder.DESCENDING)});
            featureReader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            while (featureReader.hasNext()) {
                SimpleFeature simpleFeature = featureReader.next();

                int fid = Integer.parseInt(simpleFeature.getAttribute(0).toString());
                String dtg = simpleFeature.getAttribute(1).toString();
                dtg = DateUtils.ConvertUTCToStandardDate(dtg);

                String geom = simpleFeature.getAttribute(2).toString();

                System.out.println(fid + "-->" + dtg + "-->" + "-->" + geom);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

    /**
     * 日期查询
     *
     * @param params
     */
    void dateTimeQuery(Map<String, String> params) {
        //1. 创建datastore
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建datastore失败");
            return;
        }

        //2. 构建查询条件
        LocalDateTime start = LocalDateTime.parse("2008/2/2 13:34", DateTimeFormatter.ofPattern("yyyy/M/d HH:mm"));
        LocalDateTime end = LocalDateTime.parse("2008/2/2 14:02", DateTimeFormatter.ofPattern("yyyy/M/d HH:mm"));

        String strStartTime = start.atZone(ZoneId.systemDefault()).toInstant().toString();
        String strEndTime = end.atZone(ZoneId.systemDefault()).toInstant().toString();

        StringBuffer buffer = new StringBuffer();
        buffer.append(String.format("dtg>=%s and dtg<%s", strStartTime, strEndTime));

        FeatureReader<SimpleFeatureType, SimpleFeature> featureReader = null;
        try {
            //设定要查询的字段
            Query query = new Query(FEATURE_NAME, ECQL.toFilter(buffer.toString()), new String[]{"fid", "dtg", "geom"});

            featureReader = dataStore.getFeatureReader(query, Transaction.AUTO_COMMIT);

            while (featureReader.hasNext()) {
                SimpleFeature simpleFeature = featureReader.next();

                int fid = Integer.parseInt(simpleFeature.getAttribute(0).toString());
                String dtg = simpleFeature.getAttribute(1).toString();
                dtg = DateUtils.ConvertUTCToStandardDate(dtg);

                String geom = simpleFeature.getAttribute(2).toString();

                System.out.println(fid + "-->" + dtg + "-->" + "-->" + geom);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

    /**
     * 删除feature
     *
     * @param params
     */
    void deleteDemoFeature(Map<String, String> params) {
        //1. 创建datastore
        DataStore dataStore = BaseUtils.createDataStore(params);
        if (dataStore == null) {
            System.out.println("创建datastore失败");
            return;
        }

        try {
            //2. 移除schema
            dataStore.removeSchema(FEATURE_NAME);
            System.out.println("删除成功");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
