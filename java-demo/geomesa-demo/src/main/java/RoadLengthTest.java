import org.apache.commons.lang3.StringUtils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import utils.CSVLineReader;
import utils.GeometryConvert;
import utils.ProjectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * 道路长度计算测试
 */
public class RoadLengthTest {
    public static void main(String[] args) {
        RoadLengthTest roadLengthTest = new RoadLengthTest();
        roadLengthTest.test();
    }

    public void test() {
        InputStream inputStream = null;
        try {
            inputStream = GroupDemo.class.getResource("example_data/beijing_road.csv").openStream();
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            //跳过第一行
            String content = bufferedReader.readLine();
            while (true) {
                content = bufferedReader.readLine();
                if (StringUtils.isEmpty(content)) {
                    break;
                }

                CSVLineReader csvLineReader = new CSVLineReader(content);
                String wkt = csvLineReader.nextItem();
                double length = Double.parseDouble(csvLineReader.nextItem()) * 1000;

                Geometry geometry = GeometryConvert.convertWKTToGeometry(wkt);
                geometry = ProjectionUtils.lonlat2WebMactor(geometry);

                double lineLenght = geometry.getLength();

                double offset = lineLenght - length;
                double scale = offset / length * 100;
                double scale2 = offset / lineLenght * 100;

                double croceet = lineLenght - (lineLenght *0.23);

                System.out.println(lineLenght + "--->" + length +"--->" + croceet+  "--->" + offset + "--->" + scale + "--->" + scale2);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
