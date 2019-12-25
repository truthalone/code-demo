package utils;

import org.apache.commons.lang3.StringUtils;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.factory.Hints;
import org.geotools.filter.identity.FeatureIdImpl;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 基础工具类
 */
public class BaseUtils {
    /**
     * 创建 Hbase DataStore
     * <p>
     * Map<String, String> params = new HashMap<String, String>();
     * params.put("hbase.catalog", args[0]);
     * params.put("hbase.zookeepers", args[1]);
     *
     * @param params
     * @return
     */
    public static DataStore createDataStore(Map<String, String> params) {
        try {
            DataStore dataStore = DataStoreFinder.getDataStore(params);
            if (dataStore == null) {
                return null;
            }

            return dataStore;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 创建 SimpleFeatureType
     *
     * @param typeName
     * @param spec
     * @return
     */
    public static SimpleFeatureType createSimpleFeatureType(String typeName, String spec) {
        if (StringUtils.isEmpty(typeName) || StringUtils.isEmpty(spec)) {
            return null;
        }
        SimpleFeatureType sft = SimpleFeatureTypes.createType(typeName, spec);
        return sft;
    }


    /**
     * 创建Schema
     *
     * @param dataStore
     * @param sft
     * @return
     */
    public static boolean createSchema(DataStore dataStore, SimpleFeatureType sft) {
        if (dataStore == null || sft == null) {
            return false;
        }

        try {
            dataStore.createSchema(sft);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }


    /**
     * 存储SimpleFeature
     *
     * @param datastore
     * @param sft
     * @param features
     * @throws IOException
     */
    public static void writeFeatures(DataStore datastore, SimpleFeatureType sft, List<SimpleFeature> features) throws IOException {
        int errorNum = 0;
        if (features.size() > 0) {
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = datastore.getFeatureWriterAppend(sft.getTypeName(), Transaction.AUTO_COMMIT);
            for (SimpleFeature feature : features) {
                try {
                    // using a geotools writer, you have to get a feature, modify it, then commit it
                    // appending writers will always return 'false' for haveNext, so we don't need to bother checking
                    SimpleFeature toWrite = writer.next();

                    // copy attributes
                    toWrite.setAttributes(feature.getAttributes());

                    // if you want to set the feature ID, you have to cast to an implementation class
                    // and add the USE_PROVIDED_FID hint to the user data
                    ((FeatureIdImpl) toWrite.getIdentifier()).setID(feature.getID());
                    toWrite.getUserData().put(Hints.USE_PROVIDED_FID, Boolean.TRUE);

                    // alternatively, you can use the PROVIDED_FID hint directly
                    // toWrite.getUserData().put(Hints.PROVIDED_FID, feature.getID());

                    // if no feature ID is set, a UUID will be generated for you

                    // make sure to copy the user data, if there is any
                    toWrite.getUserData().putAll(feature.getUserData());

                    // write the feature
                    writer.write();
                } catch (Exception e) {
                    System.out.println("Invalid GDELT record: " + e.toString() + " " + feature.getAttributes());
                    errorNum++;
                }
            }

            if (writer != null) {
                writer.close();
            }
        }

//        int successNum = features.size() - errorNum;
//        System.out.println("Wrote " + successNum + " features");
//        System.out.println();
    }
}
