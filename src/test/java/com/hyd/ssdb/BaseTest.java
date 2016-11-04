package com.hyd.ssdb;

import org.junit.After;
import org.junit.Before;

/**
 * (description)
 * created at 15-12-7
 *
 * @author Yiding
 */
public class BaseTest {

    protected SsdbClient ssdbClient;

    @Before
    public void init() {
        this.ssdbClient = new SsdbClient("127.0.0.1", 18888);
    }

    @After
    public void finish() {
        this.ssdbClient.close();
    }

}
