package utils;


/**
 * google 瓦片计算工具类
 */
public class GoogleTileUtils {
    public static String getTileNumber(final double lat, final double lon, final int zoom) {
        int xTile = (int) Math.floor((lon + 180) / 360 * (1 << zoom));
        int yTile = (int) Math.floor((1 - Math.log(Math.tan(Math.toRadians(lat)) + 1 / Math.cos(Math.toRadians(lat))) / Math.PI) / 2 * (1 << zoom));
        if (xTile < 0) {
            xTile = 0;
        }

        if (xTile >= (1 << zoom)) {
            xTile = ((1 << zoom) - 1);
        }

        if (yTile < 0) {
            yTile = 0;
        }

        if (yTile >= (1 << zoom)) {
            yTile = ((1 << zoom) - 1);
        }

        return ("" + zoom + "-" + xTile + "-" + yTile);
    }

    public static BoundingBox tile2boundingBox(final int x, final int y, final int zoom) {
        BoundingBox bb = new BoundingBox();
        bb.north = tile2lat(y, zoom);
        bb.south = tile2lat(y + 1, zoom);
        bb.west = tile2lon(x, zoom);
        bb.east = tile2lon(x + 1, zoom);
        return bb;
    }

    static double tile2lon(int x, int z) {
        return x / Math.pow(2.0, z) * 360.0 - 180;
    }

    static double tile2lat(int y, int z) {
        double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
        return Math.toDegrees(Math.atan(Math.sinh(n)));
    }
}
