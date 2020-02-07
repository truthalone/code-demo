package bean;

/**
 * @author ming
 * @desc 文件元信息
 */
public class FileMeta {
    /**
     * 文件名
     */
    public String fileName;

    /**
     * 文件编码
     */
    public String encoding;

    /**
     * 是否含有标题
     */
    public boolean hasTitle;

    /**
     * 行数量
     */
    public int rowCount;

    /**
     * 列数量
     */
    public int columnCount;

    /**
     * 存储的表名称
     */
    public String catalog;

    /**
     * 文件索引id
     */
    public String feature;


    public FileMeta() {
    }

    public FileMeta(String fileName, String encoding, boolean hasTitle) {
        this.fileName = fileName;
        this.encoding = encoding;
        this.hasTitle = hasTitle;
    }


    @Override
    public String toString() {
        return String.format("fileName:%s encoding:%s catalog:%s feature:%s  rowCount:%d columnCount:%d", fileName, encoding,
                catalog, feature, rowCount, columnCount);
    }
}
