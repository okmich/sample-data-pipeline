/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.hackerday;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m.enudi
 */
public class FileDownloadUtil {

	private URL downloadUrl;
	private FileOutputStream localOutputStream;
	private String downloadedFile;

	/**
	 *
	 * @param url
	 * @param savePath
	 */
	public FileDownloadUtil(String url, String savePath) {
		try {
			this.downloadUrl = new URL(url);
		} catch (MalformedURLException ex) {
			throw new IllegalArgumentException("Invalid url");
		}
		String fileName = url.substring(url.lastIndexOf('/') + 1);
		this.downloadedFile = savePath + File.separator + fileName;
		File file = new File(downloadedFile);
		try {
			this.localOutputStream = new FileOutputStream(file);
		} catch (FileNotFoundException ex) {
			Logger.getLogger(FileDownloadUtil.class.getName()).log(
					Level.SEVERE, null, ex);
			throw new RuntimeException(ex.getMessage());
		}
	}

	public long start() {
		long bytesTransfered = -1;
		try (ReadableByteChannel rbc = Channels.newChannel(downloadUrl
				.openStream());) {
			bytesTransfered = localOutputStream.getChannel().transferFrom(rbc,
					0, Long.MAX_VALUE);
			localOutputStream.close();
		} catch (Exception ex) {
			Logger.getLogger(FileDownloadUtil.class.getName()).log(
					Level.SEVERE, null, ex);
		}
		return bytesTransfered;
	}

	/**
	 *
	 * @return
	 */
	public String getDownloadedFileName() {
		return this.downloadedFile;
	}

	// public static void main(String[] args) {
	// new FileDownloadUtil(
	// "http://data.githubarchive.org/2015-01-01-16.json.gz",
	// "/home/cloudera/").start();
	// }
}
