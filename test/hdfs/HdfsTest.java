package hdfs;

import org.junit.Assert;
import org.junit.Test;

public class HdfsTest {

    @Test
    public void testLongUtil(){
        byte[] buf = new byte[Constants.CMD_BUFFER_SIZE];
        long l1 = 4326L;
        Constants.putLong(buf, l1);
        Assert.assertEquals(l1, Constants.getLong(buf));
        long l2 = -8L;
        Constants.putLong(buf, l2);
        Assert.assertEquals(l2, Constants.getLong(buf));
    }


    @Test
    public void testHumanReadableSize(){
        long s1 = 1500;
        Assert.assertEquals("1.5 kB", Constants.getHumanReadableSize(s1));

        long s2 = 110;
        Assert.assertEquals("110 B", Constants.getHumanReadableSize(s2));

        long s3 = 15000000L;
        Assert.assertEquals("15 MB", Constants.getHumanReadableSize(s3));

        long s4 = 150043200000L;
        Assert.assertEquals("150 GB", Constants.getHumanReadableSize(s4));

    }

}
