package utils;


public class BoundingBox {
    public double north;
    public double south;
    public double east;
    public double west;

    public BoundingBox() {
        this.north = 0;
        this.south = 0;
        this.east = 0;
        this.west = 0;
    }

    public BoundingBox(double west, double north, double east, double south) {
        this.west = west;
        this.north = north;
        this.east = east;
        this.south = south;
    }

    public Point2D center() {
        double centerX = (west + east) / 2;
        double centerY = (north + south) / 2;

        return new Point2D(centerX, centerY);
    }

    @Override
    public String toString() {
        return String.format("%f;%f;%f;%f", west, north, east, south);
    }
}
