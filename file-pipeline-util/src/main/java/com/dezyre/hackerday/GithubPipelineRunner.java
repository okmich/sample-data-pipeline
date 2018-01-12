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

/**
 *
 * @author m.enudi
 */
public class GithubPipelineRunner {

	private FileDownloadUtil fileDownloadUtil;
	private FileExtractorUtil fileExtractorUtil;
	private FileUploadUtil fileUploadUtil;

	private KafkaEventProducer kafkaEventProducer;

	private String tempDir;
	private String hdfsTargetDir;
	private String fileKey;

	public static final String BASE_URL = "http://data.githubarchive.org/";

	public GithubPipelineRunner(String fileKey, String hdfsTargetDir,
			String kafkaBrokerUrl, String topic) {
		this.tempDir = System.getProperty("user.home");
		this.hdfsTargetDir = hdfsTargetDir;
		this.fileKey = fileKey;
		// instantiate all classes
		this.fileDownloadUtil = new FileDownloadUtil(BASE_URL + fileKey
				+ ".json.gz", this.tempDir);
		this.kafkaEventProducer = new KafkaEventProducer(kafkaBrokerUrl, topic);
	}

	public void run() {
		// download the file
		long bytesTxnfered = this.fileDownloadUtil.start();
		String fileName = this.fileDownloadUtil.getDownloadedFileName();
		// check amount transfer for errors

		// if no error, get the name of the file
		// extract the file
		this.fileExtractorUtil = new FileExtractorUtil(fileName);
		this.fileExtractorUtil.extract();
		// upload the file to hdfs

		this.fileUploadUtil = new FileUploadUtil(this.fileKey,
				this.fileExtractorUtil.getExtractedFile(), this.hdfsTargetDir);

		try {
			boolean success = this.fileUploadUtil.upload();
			// send a message to kafka
			if (success) {
				this.kafkaEventProducer.send(new Event("FILE_ING_COMPLT",
						this.fileKey, this.fileUploadUtil.getHdfsLocation()));
			} else {
				this.kafkaEventProducer.send(new Event("FILE_ING_ERR",
						this.fileKey,
						"Error occured trying to upload the fileKey the hdfs"));
			}
		} catch (Exception ex) {
			System.err.println("OH BAD!!!");
			Logger.getLogger(GithubPipelineRunner.class.getName()).log(
					Level.SEVERE, null, ex);
		}
	}

	public static void main(String[] args) {
		// String fKey = args(0);
		// String hdfsPath = args(1)
		// String brokerUrl = args(2)
		// String topic = args(3)

		new GithubPipelineRunner("2015-01-01-15",
				"hdfs://quickstart.cloudera:8020/user/cloudera/githubarchives",
				"", "").run();
	}
}
