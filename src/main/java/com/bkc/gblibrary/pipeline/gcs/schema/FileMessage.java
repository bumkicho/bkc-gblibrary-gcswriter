package com.bkc.gblibrary.pipeline.gcs.schema;

public class FileMessage {
	private String source;
	private String sourceUri;
	private String fileName;
	private String target;
	private String targetUri;

//	{"source":"web","sourceUri":"https://gutenberg.org/cache/epub/feeds","fileName":"rdf-files.tar.bz2","target":"gcs","targetUri":"gs://bkc-ai-ml-core-files"}
	public FileMessage(String source, String sourceUri, String fileName, String target, String targetUri) {
		this.source = source;
		this.sourceUri = sourceUri;
		this.fileName = fileName;
		this.target = target;
		this.targetUri = targetUri;
	}

	public String getSource() {
		return source;
	}

	public String getSourceUri() {
		return sourceUri;
	}

	public String getFileName() {
		return fileName;
	}

	public String getTarget() {
		return target;
	}

	public String getTargetUri() {
		return targetUri;
	}
}
