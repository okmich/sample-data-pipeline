/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.hackerday;

import com.dezyre.hackerday.messaging.Event;
import com.dezyre.hackerday.messaging.KafkaEventProducer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;
import java.util.HashMap;

/**
 *
 * @author m.enudi
 */
public class GithubPipelineRunner {

	private static final Logger LOG = Logger
			.getLogger(GithubPipelineRunner.class.getName());

	private FileDownloadUtil fileDownloadUtil;
	private FileExtractorUtil fileExtractorUtil;
	private FileUploadUtil fileUploadUtil;

	private KafkaEventProducer kafkaEventProducer;

	private String tempDir;
	private String hdfsTargetDir;
	private String fileKey;

	public static final String BASE_URL = "http://data.githubarchive.org/";

	public GithubPipelineRunner(String fileKey, String hdfsTargetDir,
			String kafkaBrokerUrl) {
		this.tempDir = System.getProperty("user.home");
		this.hdfsTargetDir = hdfsTargetDir;
		this.fileKey = fileKey;
		// instantiate all classes
		this.fileDownloadUtil = new FileDownloadUtil(BASE_URL + fileKey
				+ ".json.gz", this.tempDir);
		this.kafkaEventProducer = new KafkaEventProducer(kafkaBrokerUrl);
	}

	public void run() {
		LOG.info("preparing to run pipeline for key " + fileKey);
		try {
			// download the file
			LOG.info("downloading filekey " + fileKey);
			long bytesTxnfered = this.fileDownloadUtil.start();
			String fileName = this.fileDownloadUtil.getDownloadedFileName();

			LOG.info("File for key " + fileKey + " downloaded to path "
					+ fileName);

			// check amount transfer for errors

			// if no error, get the name of the file
			// extract the file
			LOG.info("Extracting file " + fileName);
			this.fileExtractorUtil = new FileExtractorUtil(fileName);
			this.fileExtractorUtil.extract();
			LOG.info("Extracting file completed "
					+ this.fileExtractorUtil.getExtractedFile());
			// upload the file to hdfs

			LOG.info("Uploading file " + fileName + " to hdfs directory "
					+ this.hdfsTargetDir);
			this.fileUploadUtil = new FileUploadUtil(this.fileKey,
					this.fileExtractorUtil.getExtractedFile(),
					this.hdfsTargetDir);

			boolean success = this.fileUploadUtil.upload();

			LOG.info("File uploading completed "
					+ this.fileUploadUtil.getHdfsLocation());
			// send a message to kafka
			if (success) {
				this.kafkaEventProducer.send(new Event(
						"file-ingestion-complete", this.fileKey,
						this.fileUploadUtil.getHdfsLocation()));
			} else {
				this.kafkaEventProducer.send(new Event("file-ingestion-error",
						this.fileKey,
						"Error occured trying to upload the fileKey the hdfs"));
			}
		} catch (Exception ex) {
			LOG.log(Level.SEVERE, null, ex);

			this.kafkaEventProducer.send(new Event("file-ingestion-error",
					this.fileKey, ex.getMessage()));
		}
	}

	public static void main(String[] args) {
		//
		Map<String, String> arguments = parseCommandLineArgs(args);
		System.out.println(arguments);
		if (arguments.isEmpty()) {
			System.err
					.println("Argument usage::  -fileKey=2015-01-01-15 -hdfsloc=... -kafkaBroker=...");
			System.exit(-1);
		}
		new GithubPipelineRunner(arguments.get("filekey"),
				arguments.get("hdfsloc"), arguments.get("kafkabroker")).run();
	}

	private static Map<String, String> parseCommandLineArgs(String[] args) {
		Map<String, String> params = new HashMap<>();
		// -key=lsd
		for (String arg : args) {
			String[] option = arg.split("=");
			params.put(option[0].substring(1).toLowerCase(), option[1]);
		}
		return params;
	}
}
