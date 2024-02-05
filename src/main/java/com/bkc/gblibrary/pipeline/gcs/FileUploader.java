package com.bkc.gblibrary.pipeline.gcs;

import java.io.*;
import java.net.URL;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import com.google.cloud.storage.*;


public class FileUploader {
    public static void uploadFile(String sourceUri, String fileName, String projectId, String targetUri) throws IOException {
        URL url = new URL(sourceUri+File.separatorChar+fileName);
        File file = new File(targetUri+File.separatorChar+fileName);
        Boolean writeFile = false;

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        BlobId blobId = BlobId.of(targetUri, fileName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        // Set a generation-match precondition to avoid potential race
        // conditions and data corruptions. The request returns a 412 error if the
        // preconditions are not met.
        Storage.BlobTargetOption precondition;
        if (storage.get(targetUri, fileName) == null) {
            precondition = Storage.BlobTargetOption.doesNotExist();
            // File does not exist. Write to file object
            FileUtils.copyURLToFile(url, file);
            writeFile = true;
        } else {
            precondition =
                Storage.BlobTargetOption.generationMatch(
                    storage.get(targetUri, fileName).getGeneration());
        }

        if (writeFile) {
            storage.create(
                blobInfo,
                Files.toByteArray(file),
                precondition
            );
        }
    }
}
