package ordo;

import formats.Format;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class JobTest {

    private Job job;

    @Before
    public void init(){
        job = new Job();
    }

    @Test
    public void setInputFormat1() {
        job.setInputFormat(Format.Type.LINE);
        assert  (job.getInputFormat() == Format.Type.LINE);
    }

    @Test
    public void setInputFormat2() {
        job.setInputFormat(Format.Type.KV);
        assert  (job.getInputFormat() == Format.Type.KV);
    }

    @Test
    public void setInputFormat3() {
        job.setInputFormat(Format.Type.KV);
        assert  (job.getInputFormat() != Format.Type.LINE);
    }

    @Test
    public void setInputFname1() {
        job.setInputFname("test");
        assert (job.getInputFname().equals("test"));
    }

    @Test
    public void setInputFname2() {
        job.setInputFname("test1");
        assert (!job.getInputFname().equals("test2"));
    }

    @Test
    public void startJob() {
    }
}