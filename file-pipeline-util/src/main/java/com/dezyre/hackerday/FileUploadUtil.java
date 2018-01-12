/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dezyre.hackerday;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author m.enudi
 */
public class FileUploadUtil {

	private final Path localPath;
	private final Path hdfsPath;

	/**
	 *
	 * @param localFile
	 * @param hdfsPath
	 */
	public FileUploadUtil(String fileKey, String localFile, String hdfsPath) {
		this.localPath = new Path("file://" + localFile);
		this.hdfsPath = new Path(hdfsPath + "/" + fileKey);
	}

	public boolean upload() throws Exception {
		Configuration conf = new Configuration();

		try (FileSystem hdfsFs = FileSystem.get(hdfsPath.toUri(), conf);) {
			if (!hdfsFs.exists(hdfsPath)) {
				hdfsFs.mkdirs(hdfsPath);
			}
			hdfsFs.moveFromLocalFile(localPath, hdfsPath);
			return true;
		} catch (Exception ex) {
			Logger.getLogger(FileUploadUtil.class.getName()).log(Level.SEVERE,
					null, ex);
		}
		return false;
	}

	public String getHdfsLocation() {
		return this.hdfsPath.toString();
	}

	// public static void main(String[] args) throws Exception {
	// new FileUploadUtil(
	// "file:///home/cloudera/2015-01-01-16.json",
	// "hdfs://quickstart.cloudera:8020/user/cloudera/githubarchives/2015-01-01-16.json")
	// .upload();
	// System.out.println("Done");
	// }
}
