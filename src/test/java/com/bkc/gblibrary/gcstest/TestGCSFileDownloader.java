package com.bkc.gblibrary.gcstest;

import com.bkc.gblibrary.pipeline.gcs.GCSWriter;
import org.junit.Assert;

public class TestGCSFileDownloader {

    @org.junit.jupiter.api.Test
    public void test() {
//        String[] optionArgs = new String[4];
//        optionArgs[0] = "--runner=DirectRunner";
//        optionArgs[1] = "--jobName=test-file-downloader";
//        optionArgs[2] = "--inputPath=projects/bkc-ai-ml-core/subscriptions/bkc-test-topic-sub,projects/bkc-ai-ml-core/subscriptions/bkc-test-topic-2-sub";
//        optionArgs[3] = "--pubsubOutput=projects/bkc-ai-ml-core/topics/bkc-output-topic";

        String[] optionArgs = new String[8];
        optionArgs[0] = "--runner=DataflowRunner";
        optionArgs[1] = "--jobName=test-file-downloader";
        optionArgs[2] = "--project=bkc-ai-ml-core";
        optionArgs[3] = "--gcpTempLocation=gs://bkc-ai-ml-core-dataflow/temp/";
        optionArgs[4] = "--stagingLocation=gs://bkc-ai-ml-core-dataflow/staging/";
        optionArgs[5] = "--region=us-central1";
        optionArgs[6] = "--inputPath=projects/bkc-ai-ml-core/subscriptions/bkc-test-topic-sub,projects/bkc-ai-ml-core/subscriptions/bkc-test-topic-2-sub";
        optionArgs[7] = "--pubsubOutput=projects/bkc-ai-ml-core/topics/bkc-output-topic";

        GCSWriter.main(optionArgs);
//        Assert.assertEquals("test","test");
    }



}
