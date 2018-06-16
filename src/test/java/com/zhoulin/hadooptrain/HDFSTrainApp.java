package com.zhoulin.hadooptrain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * 使用Java API 操作 hadoop HDFS
 */
public class HDFSTrainApp {

    public static final String HDFS_PATH = "hdfs://192.168.74.129:9000";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        //添加目录文件
        fileSystem.mkdirs(new Path("/hdfsApi/test/zhoulin"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsApi/test/zhoulin/a.txt"));
        outputStream.write("hello hadoop !".getBytes());
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream inputStream = fileSystem.open(new Path("/hdfsApi/test/zhoulin/local_a.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }

    /**
     * 对文件重命名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception{
        Path oldPath = new Path("/hdfsApi/test/zhoulin/a.txt");
        Path newPath = new Path("/hdfsApi/test/zhoulin/a_new.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     *  上传本地文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws IOException {
        Path localPath = new Path("C:\\Users\\周林\\Desktop\\jdk-8u141-linux-x64.tar.gz");
        Path path = new Path("/hdfsApi/test/zhoulin/jdk.tar.gz");
        fileSystem.copyFromLocalFile(localPath, path);
    }

    /**
     * 存在数据流问题
     * 待解决
     * @throws IOException
     */
    @Test
    public void copyFromLocalFileWithProgress() throws IOException {

        InputStream in = new BufferedInputStream(new FileInputStream(new File("C:\\Users\\周林\\Desktop\\jdk-8u141-linux-x64.tar.gz")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsApi/test/zhoulin/jdk-8u141-linux-x64-2.tar.gz"),
                () -> System.out.println(".")
        );
        outputStream.flush();
        outputStream.close();
    }

    /**
     * 下载文件到本地
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception{

        Path localPath = new Path("C:\\Users\\周林\\Desktop\\hello_zl.txt");
        Path hdfsPath = new Path("/hdfsApi/test/zhoulin/a_new.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);

    }

    /**
     * 查看某个目录下所有文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsApi/test"));
        for (FileStatus fileStatus : fileStatuses){
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = String.valueOf(fileStatus.getPath());
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 删除指定文件
     * @throws Exception
     */
    @Test
    public void delelteFile() throws Exception{
        Path deletePath = new Path("/hdfsApi/test/zhoulin/15103330243 周林1.zip");
        boolean isDel = fileSystem.delete(deletePath, true);
        System.out.println(isDel);
    }

    @Before
    public void setUp() throws Exception{
        configuration = new Configuration();
        // user 权限用户
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
        System.out.println("HDFSTrainApp.setUp");
    }

    @After
    public void tearDown() throws Exception{
        fileSystem = null;
        configuration = null;
        System.out.println("HDFSTrainApp.tearDown");
    }
}
