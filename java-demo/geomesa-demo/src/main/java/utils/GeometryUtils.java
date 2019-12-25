package utils;

import org.locationtech.jts.geom.Coordinate;

import java.util.List;

/**
 * 集合对象工具类
 */
public class GeometryUtils {

    /**
     * 获取集合对象重心点
     *
     * @param geoCoordinateList
     * @return
     */
    public Coordinate GetCenterOfCoordinates(List<Coordinate> geoCoordinateList) {
        int total = geoCoordinateList.size();
        double X = 0, Y = 0, Z = 0;
        for (Coordinate g : geoCoordinateList) {
            double lat, lon, x, y, z;
            lat = g.getY() * Math.PI / 180;
            lon = g.getX() * Math.PI / 180;
            x = Math.cos(lat) * Math.cos(lon);
            y = Math.cos(lat) * Math.sin(lon);
            z = Math.sin(lat);
            X += x;
            Y += y;
            Z += z;
        }
        X = X / total;
        Y = Y / total;
        Z = Z / total;
        double Lon = Math.atan2(Y, X);
        double Hyp = Math.sqrt(X * X + Y * Y);
        double Lat = Math.atan2(Z, Hyp);
        return new Coordinate(Lon * 180 / Math.PI, Lat * 180 / Math.PI);
    }

}
