package utils;

import org.apache.commons.lang.StringUtils;
import org.geotools.feature.FeatureCollection;
import org.geotools.geojson.feature.FeatureJSON;
import org.geotools.geojson.geom.GeometryJSON;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.opengis.feature.simple.SimpleFeature;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * 几何对象转换类
 */
public class GeometryConvert {
    private static final int DEFAULT_DECIMALS = 6;

    /**
     * 转换WKT为Geometry 对象
     *
     * @param wkt
     * @return
     * @throws ParseException
     */
    public static Geometry convertWKTToGeometry(String wkt) {
        WKTReader reader = new WKTReader();
        try {
            return reader.read(wkt);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 转化Geojson 对象为 Geometry对象
     *
     * @param geoJson
     * @param decimals
     * @return
     * @throws IOException
     */
    public static Geometry convertGeoJsonToGeometry(String geoJson, int decimals) throws IOException {
        if (decimals < 0) {
            decimals = DEFAULT_DECIMALS;
        }
        GeometryJSON json = new GeometryJSON();
        StringReader reader = new StringReader(geoJson);

        return json.read(reader);
    }


    public static Geometry convertFeatureJsonToGeometry(String geoJson) {
        FeatureJSON featureJSON = new FeatureJSON();
        StringReader reader = new StringReader(geoJson);
        try {
            FeatureCollection features = featureJSON.readFeatureCollection(reader);
            features.size();

            if (features.features().hasNext()) {
                SimpleFeature feature = (SimpleFeature) features.features().next();
                if (feature != null) {
                    return (Geometry) feature.getDefaultGeometry();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * Geometry 对象转换为 WKT字符串
     *
     * @param geometry
     * @return
     */
    public static String convertGeometryToWKT(Geometry geometry) {
        if (geometry == null) {
            return null;
        }
        return geometry.toString();
    }

    /**
     * Geometry对象转换为 GeoJson对象
     *
     * @param geometry
     * @param dicimals
     * @return
     * @throws IOException
     */
    public static String convertGeometryToGeoJson(Geometry geometry, int dicimals) throws IOException {
        if (geometry == null) {
            return null;
        }

        if (dicimals < 0) {
            dicimals = DEFAULT_DECIMALS;
        }

        StringWriter writer = new StringWriter();
        GeometryJSON geometryJSON = new GeometryJSON(dicimals);
        geometryJSON.write(geometry, writer);
        String ret = writer.toString();
        writer.close();

        StringBuffer buffer = new StringBuffer();
        buffer.append("{\"type\":\"FeatureCollection\",");
        buffer.append("\"features\":[");
        buffer.append("{\"type\":\"Feature\",");
        buffer.append("\"geometry\":");
        buffer.append(ret);
        buffer.append("}]}");

        return buffer.toString();
    }


    public static Map<String, Object> convertGeometryToMap(Geometry geometry) {
        if (geometry == null) {
            return null;
        }

        Map<String, Object> dict = new HashMap<>();
        dict.put("type", geometry.getGeometryType());
        dict.put("coordinates", geometry.getCoordinates());

        return dict;
    }


    /**
     * 判定是否属于Geometry
     * <p>
     * case "Point":
     * case "LineString":
     * case "Polygon":
     * case "LinearRing":
     * case "MultiPoint":
     * case "MultiLineString":
     * case "MultiPolygon":
     *
     * @param type
     * @return
     */

    public static boolean isGeometry(String type) {
        if (StringUtils.isEmpty(type)) {
            return false;
        }

        type = type.toUpperCase();

        switch (type) {
            case "POINT":
            case "LINESTRING":
            case "POLYGON":
            case "LINEARRING":
            case "MULTIPOINT":
            case "MULTILINESTRING":
            case "MULTIPOLYGON":
                return true;
            default:
                return false;
        }
    }


}
