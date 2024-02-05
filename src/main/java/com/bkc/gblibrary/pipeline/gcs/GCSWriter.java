package com.bkc.gblibrary.pipeline.gcs;

import com.bkc.gblibrary.pipeline.gcs.schema.FileMessage;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class GCSWriter {
	private static final Logger logger = LogManager.getLogger();

	public interface GCSPubsubOptions extends PubsubOptions {
		@Description("Project ID")
		@Default.String("bkc-ai-ml-core")
		String getProjectId();
		void setProjectId(String projectId);

		@Description("The list of inputs")
		@Validation.Required
		String[] getInputPath();
		void setInputPath(String[] inputPath);

		@Description("Output Topic")
		@Validation.Required
		String getPubsubOutput();
		void setPubsubOutput(String pubsubOutput);
	}

	public class GCSOutputTag {
		static TupleTag<String> pubsubOutput = new TupleTag<String>("pubsubOutput");
		static TupleTag<String> gcsOutput = new TupleTag<String>("gcsOutput");
	}

	static PCollection<String> flattenFromSubscriptions(List<String> subscriptionList, Pipeline pipeline) {
		ArrayList<PCollection<String>> pCollections = new ArrayList<>();

		for (String sub : subscriptionList) {
			PCollection<String> messages = pipeline
				.apply("Read " + sub,
					PubsubIO.<String>readStrings().fromSubscription(sub)
				);
//				.apply("Map " + sub,
//					MapElements.via(
//						new SimpleFunction<String, KV<String, String>>() {
//							@Override
//							public KV<String, String> apply(String input) {
//								return KV.of(sub, input);
//							}
//						}
//					)
//				);
			pCollections.add(messages);
		}

		PCollectionList<String> pcollectionList = PCollectionList.of(pCollections);

//		subscriptionList
//			.forEach(subscription ->
//				pcollectionList.and(
//					pipeline.apply(
//						"Read From " + subscription,
//						PubsubIO.readStrings().fromSubscription(subscription)
//					)
//				)
//			);

		return pcollectionList.apply("Combine Messages",Flatten.pCollections()).setCoder(StringUtf8Coder.of());
	}

	public static void main(String[] args) {
		GCSPubsubOptions options = PipelineOptionsFactory
			.fromArgs(args)
			.withValidation()
			.as(GCSPubsubOptions.class);

		Pipeline pipeline = Pipeline.create(options);

		List<String> subscriptionList = Arrays.stream(options.getInputPath()).toList();

		PCollection<String> pubsubMessages = flattenFromSubscriptions(subscriptionList, pipeline);

		PCollectionTuple branchedMessages = pubsubMessages.apply(
			"Output branching",
			ParDo.of(
				new SplitMessages()
			).withOutputTags(GCSOutputTag.pubsubOutput, TupleTagList.of(GCSOutputTag.gcsOutput))
		);

		branchedMessages.get(GCSOutputTag.pubsubOutput).setCoder(StringUtf8Coder.of());
		branchedMessages.get(GCSOutputTag.gcsOutput).setCoder(StringUtf8Coder.of());

		PCollection<String> pubsubOut = branchedMessages.get(GCSOutputTag.pubsubOutput);
		PCollection<String> gcsOut = branchedMessages.get(GCSOutputTag.gcsOutput);

		pubsubOut.apply(
			"Process Pubsub Message",
			PubsubIO.writeStrings().to(options.getPubsubOutput()).withMaxBatchSize(100)
		);

		gcsOut.apply(
			"Process File Message",
			ParDo.of(
				new ProcessFileMessage(options.getProjectId())
			)
		);

		//
		// Below merging of PCollection and applying windowing not applicable
		// This was more for future reference
		//
//		PCollection<String> mergedCollectionWithFlatten =
//			PCollectionList.of(pubsubOut).and(gcsOut).apply(Flatten.<String>pCollections());

//		mergedCollectionWithFlatten.apply("Window",
//			Window.<String>into(
//				FixedWindows.of(Duration.standardMinutes(10))
//			).discardingFiredPanes()
//		);

		pipeline.run().waitUntilFinish();


	}

	public static class ProcessFileMessage extends DoFn<String, String> {

		private String projectId;

		public ProcessFileMessage(String projectId) {
			this.projectId = projectId;
		}

		@ProcessElement
		public void ProcessElement(ProcessContext c) throws IOException {
			String element = c.element();
			logger.info("process element:" + Objects.requireNonNull(element));
			Gson gson = new Gson();
			FileMessage fileMessage = gson.fromJson(element, FileMessage.class);
			String src = fileMessage.getSource();
			String srcUri = fileMessage.getSourceUri();
			String fileName = fileMessage.getFileName();
			String tgt = fileMessage.getTarget();
			String tgtUri = fileMessage.getTargetUri();
			if (src.equalsIgnoreCase("web") && tgt.equalsIgnoreCase("gcs")) {
				FileUploader.uploadFile(srcUri, fileName, this.projectId, tgtUri);
			} else if (src.equalsIgnoreCase("gcs") && tgt.equalsIgnoreCase("gcs")) {
				logger.info("Copy from gcs to gcs");
			} else {
				logger.info("Unknown Operation");
			}

            c.output(element);
		}

	}

	public static class SplitMessages extends DoFn<String, String> {

		@ProcessElement
		public void processElement(ProcessContext c) {
			String element = c.element();
			Gson gson = new Gson();
			FileMessage fileMessage = gson.fromJson(element, FileMessage.class);

			if (fileMessage.getTarget().equalsIgnoreCase("pubsub")) {
				c.output(GCSOutputTag.pubsubOutput, element);
				logger.info("message to pubsub: " + fileMessage.getTargetUri());
			} else {
				c.output(GCSOutputTag.gcsOutput, element);
			}
		}

	}
}
