package utils;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * 投影坐标转换
 */
public class ProjectionUtils {
    private static final String strWKTMercator = "PROJCS[\"World_Mercator\","
            + "GEOGCS[\"GCS_WGS_1984\","
            + "DATUM[\"WGS_1984\","
            + "SPHEROID[\"WGS_1984\",6378137,298.257223563]],"
            + "PRIMEM[\"Greenwich\",0],"
            + "UNIT[\"Degree\",0.017453292519943295]],"
            + "PROJECTION[\"Mercator_1SP\"],"
            + "PARAMETER[\"False_Easting\",0],"
            + "PARAMETER[\"False_Northing\",0],"
            + "PARAMETER[\"Central_Meridian\",0],"
            + "PARAMETER[\"latitude_of_origin\",0],"
            + "UNIT[\"Meter\",1]]";


    /**
     * 经纬度坐标下的几何对象转换成墨卡托投影下的几何对象
     *
     * @param geom
     * @return
     */
    public static Geometry lonlat2WebMactor(Geometry geom) {
        if (geom == null) {
            return null;
        }
        try {
//            CoordinateReferenceSystem mactroCRS = CRS.parseWKT(strWKTMercator);
            CoordinateReferenceSystem mactroCRS = CRS.decode("EPSG:3857");

           MathTransform transform = CRS.findMathTransform(DefaultGeographicCRS.WGS84, mactroCRS);
            return JTS.transform(geom, transform);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 墨卡托投影下的几何对象转化成经纬度投影
     *
     * @param geom
     * @return
     */
    public static Geometry webMactor2LonLat(Geometry geom) {
        try {
            if (geom == null) {
                return null;
            }
            CoordinateReferenceSystem mercatroCRS = CRS.parseWKT(strWKTMercator);
            MathTransform transform = CRS.findMathTransform(mercatroCRS, DefaultGeographicCRS.WGS84);

            return JTS.transform(geom, transform);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
