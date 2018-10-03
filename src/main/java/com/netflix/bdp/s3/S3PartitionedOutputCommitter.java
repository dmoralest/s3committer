/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.bdp.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.bdp.s3.util.ConflictResolution;
import com.netflix.bdp.s3.util.HiddenPathFilter;
import com.netflix.bdp.s3.util.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class S3PartitionedOutputCommitter extends S3MultipartOutputCommitter {
  protected static final String TABLE_ROOT = "table_root";

  private static final Logger LOG = LoggerFactory.getLogger(
      S3PartitionedOutputCommitter.class);
  private final int maxNumberOfAttempts;

  public S3PartitionedOutputCommitter(Path outputPath, JobContext context)
      throws IOException {
    super(outputPath, context);
    Configuration conf = context.getConfiguration();
    this.maxNumberOfAttempts = conf.getInt(
            S3Committer.MAX_NUMBER_ATTEMPTS, S3Committer.DEFAULT_NUM_ATTEMPTS);
  }

  public S3PartitionedOutputCommitter(Path outputPath,
                                      TaskAttemptContext context)
      throws IOException {
    super(outputPath, context);
    Configuration conf = context.getConfiguration();
    this.maxNumberOfAttempts = conf.getInt(
            S3Committer.MAX_NUMBER_ATTEMPTS, S3Committer.DEFAULT_NUM_ATTEMPTS);
  }

  @Override
  protected List<FileStatus> getTaskOutput(TaskAttemptContext context)
      throws IOException {
    PathFilter filter = HiddenPathFilter.get();

    // get files on the local FS in the attempt path
    Path attemptPath = getTaskAttemptPath(context);
    FileSystem attemptFS = attemptPath.getFileSystem(context.getConfiguration());
    RemoteIterator<LocatedFileStatus> iter = attemptFS
        .listFiles(attemptPath, true /* recursive */ );

    List<FileStatus> stats = Lists.newArrayList();
    while (iter.hasNext()) {
      FileStatus stat = iter.next();
      if (filter.accept(stat.getPath())) {
        stats.add(stat);
      }
    }

    return stats;
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    // these checks run before any files are uploaded to S3, so it is okay for
    // this to throw failures.
    List<FileStatus> taskOutput = getTaskOutput(context);
    Path attemptPath = getTaskAttemptPath(context);
    Configuration conf = context.getConfiguration();
    FileSystem attemptFS = attemptPath.getFileSystem(conf);
    Set<String> partitions = getPartitions(attemptFS, attemptPath, taskOutput);

    // enforce conflict resolution, but only if the mode is FAIL. for APPEND,
    // it doesn't matter that the partitions are already there, and for REPLACE,
    // deletion should be done during task commit.
    if (getMode(context) == ConflictResolution.FAIL) {
      FileSystem s3 = getOutputPath(context)
          .getFileSystem(context.getConfiguration());
      for (String partition : partitions) {
        // getFinalPath adds the UUID to the file name. this needs the parent.
        Path partitionPath = getFinalPath(partition + "/file", context).getParent();
        if (s3.exists(partitionPath)) {
          throw new AlreadyExistsException("Output partition " + partition +
              " already exists: " + partitionPath);
        }
      }
    }

    commitTaskInternal(context, taskOutput);
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    List<S3Util.PendingUpload> pending = getPendingUploads(context);

    final FileSystem s3 = getOutputPath(context)
        .getFileSystem(context.getConfiguration());
    Set<Path> partitions = Sets.newLinkedHashSet();
    for (S3Util.PendingUpload commit : pending) {
      Path filePath = new Path(
          "s3://" + commit.getBucketName() + "/" + commit.getKey());
      partitions.add(filePath.getParent());
    }

    // enforce conflict resolution
    boolean threw = true;
    try {
      switch (getMode(context)) {
        case FAIL:
          // FAIL checking is done on the task side, so this does nothing
          break;
        case APPEND:
          // no check is needed because the output may exist for appending
          break;
        case REPLACE:
          Set<S3Util.DirectoryReplacement> directories = Sets.newLinkedHashSet();
          for (Path partition : partitions) {
            directories.add(new S3Util.DirectoryReplacement(partition));
          }
          final AmazonS3 client = getClient(
                  getOutputPath(context), context.getConfiguration());

          Tasks.foreach(directories)
                  .stopOnFailure().throwFailureWhenFinished()
                  .executeWith(getThreadPool(context))
                  .onFailure(new Tasks.FailureTask<S3Util.DirectoryReplacement, RuntimeException>() {
                    @Override
                    public void run(S3Util.DirectoryReplacement replacement,
                                    Exception exception) {
                        LOG.error("Could not verify directory replacement", exception);
                    }
                  })
                  .abortWith(new Tasks.Task<S3Util.DirectoryReplacement, RuntimeException>() {
                    @Override
                    public void run(S3Util.DirectoryReplacement replacement) {
                      LOG.error("Aborting " + replacement);
                    }
                  })
                  .run(new Tasks.Task<S3Util.DirectoryReplacement, RuntimeException>() {
                    @Override
                    public void run(S3Util.DirectoryReplacement replacement) {
                      if (replacement.exists(s3, maxNumberOfAttempts)) {
                        LOG.info("Removing partition path to be replaced: " +
                                replacement);
                        replacement.delete(s3, maxNumberOfAttempts);
                      }
                    }
                  });
          break;
        default:
          throw new RuntimeException(
              "Unknown conflict resolution mode: " + getMode(context));
      }

      threw = false;

    } catch (Throwable e) {
      throw new IOException(
          "Failed to enforce conflict resolution", e);

    } finally {
      if (threw) {
        abortJobInternal(context, pending, threw);
      }
    }

    commitJobInternal(context, pending);
  }

  protected Set<String> getPartitions(FileSystem attemptFS, Path attemptPath,
                                      List<FileStatus> taskOutput)
      throws IOException {
    // get a list of partition directories
    Set<String> partitions = Sets.newLinkedHashSet();
    for (FileStatus stat : taskOutput) {
      // sanity check the output paths
      Path outputFile = stat.getPath();
      if (!attemptFS.isFile(outputFile)) {
        throw new RuntimeException(
            "Task output entry is not a file: " + outputFile);
      }
      String partition = getPartition(
          Paths.getRelativePath(attemptPath, outputFile));
      if (partition != null) {
        partitions.add(partition);
      } else {
        partitions.add(TABLE_ROOT);
      }
    }

    return partitions;
  }
}
