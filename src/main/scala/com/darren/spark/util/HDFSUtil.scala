package com.darren.spark.util

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}

object HDFSUtil {
  val SUCCESS = 0
  val FAILURE = -1
  val NOT_EXIST = 1

  /**
    * delete file or folder from HDFS
    *
    * @param hdfs file system
    * @param path file path or folder path
    * @throws IOException
    * @return 0: Removed success, -1: Removed failure, 1: Not exist
    */
  @throws[IOException]
  def deleteFile(hdfs: FileSystem, path: String): Int = {
    val hdfsPath = new Path(path);
    if (hdfs.exists(hdfsPath)) {

      // 0: removed success
      //-1: removed failure
      if (hdfs.delete(hdfsPath, true)) SUCCESS else FAILURE
    } else {

      // 1: file doesn't exist
      NOT_EXIST
    }
  }
}
