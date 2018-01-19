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
		// this.localPath = new Path("file://" + localFile); -- wrong
		this.localPath = new Path(localFile);
		this.hdfsPath = new Path(hdfsPath + "/" + fileKey);
	}

	public boolean upload() throws Exception {
		Configuration conf = new Configuration();
		conf.addResource("/etc/hadoop/conf/core-site.xml");
		conf.addResource("/etc/hadoop/conf/hdfs-site.xml");

		conf.set("fs.hdfs.impl",
				org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl",
				org.apache.hadoop.fs.LocalFileSystem.class.getName());

		try (FileSystem hdfsFs = FileSystem.get(hdfsPath.toUri(), conf);) {
			System.out.println(hdfsFs.getClass());
			if (!hdfsFs.exists(hdfsPath)) {
				hdfsFs.mkdirs(hdfsPath);
			}
			hdfsFs.moveFromLocalFile(localPath, hdfsPath);
			return true;
		} catch (Exception ex) {
			Logger.getLogger(FileUploadUtil.class.getName()).log(Level.SEVERE,
					null, ex);
			throw ex;
		}
	}

	public String getHdfsLocation() {
		return this.hdfsPath.toString();
	}

	// public static void main(String[] args) throws Exception {
	// new FileUploadUtil("2015-01-01-16",
	// "/home/cloudera/2015-01-01-15.json",
	// "hdfs://quickstart.cloudera:8020/user/cloudera/githubarchives")
	// .upload();
	// System.out.println("Done");
	// }
}
