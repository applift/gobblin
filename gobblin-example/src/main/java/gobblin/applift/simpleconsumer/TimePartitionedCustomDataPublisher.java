package gobblin.applift.simpleconsumer;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.BaseDataPublisher;
import gobblin.util.FileListUtils;
import gobblin.util.ForkOperatorUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;

public class TimePartitionedCustomDataPublisher extends BaseDataPublisher {
	private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedCustomDataPublisher.class);

	public TimePartitionedCustomDataPublisher(State state) throws IOException {
		super(state);
	}
	
	@Override
	protected void publishData(WorkUnitState state, int branchId, boolean publishSingleTaskData,
      Set<Path> writerOutputPathsMoved) throws IOException {
    // Get a ParallelRunner instance for moving files in parallel
    ParallelRunner parallelRunner = this.getParallelRunner(this.writerFileSystemByBranches.get(branchId));

    // The directory where the workUnitState wrote its output data.
    Path writerOutputDir = WriterUtils.getWriterOutputDir(state, this.numBranches, branchId);

    if (!this.writerFileSystemByBranches.get(branchId).exists(writerOutputDir)) {
      LOG.warn(String.format("Branch %d of WorkUnit %s produced no data", branchId, state.getId()));
      return;
    }

    // The directory where the final output directory for this job will be placed.
    // It is a combination of DATA_PUBLISHER_FINAL_DIR and WRITER_FILE_PATH.
    Path publisherOutputDir = getPublisherOutputDir(state, branchId);

    if (publishSingleTaskData) {

      // Create final output directory
      WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId), publisherOutputDir,
          this.permissions.get(branchId));
      addSingleTaskWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, state, branchId, parallelRunner);
    } else {
      if (writerOutputPathsMoved.contains(writerOutputDir)) {
        // This writer output path has already been moved for another task of the same extract
        // If publishSingleTaskData=true, writerOutputPathMoved is ignored.
        return;
      }
      
      if (this.publisherFileSystemByBranches.get(branchId).exists(publisherOutputDir)) {
        // The final output directory already exists, check if the job is configured to replace it.
        // If publishSingleTaskData=true, final output directory is never replaced.
        boolean replaceFinalOutputDir = this.getState().getPropAsBoolean(ForkOperatorUtils
            .getPropertyNameForBranch(ConfigurationKeys.DATA_PUBLISHER_REPLACE_FINAL_DIR, this.numBranches, branchId));

        // If the final output directory is not configured to be replaced, put new data to the existing directory.
        if (!replaceFinalOutputDir) {
          addWriterOutputToExistingDir(writerOutputDir, publisherOutputDir, state, branchId, parallelRunner);
          writerOutputPathsMoved.add(writerOutputDir);
          return;
        }
        // Delete the final output directory if it is configured to be replaced
        LOG.info("Deleting publisher output dir " + publisherOutputDir);
        this.publisherFileSystemByBranches.get(branchId).delete(publisherOutputDir, true);
      } else {
        // Create the parent directory of the final output directory if it does not exist
        WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
            publisherOutputDir.getParent(), this.permissions.get(branchId));
      }

      LOG.info(String.format("Moving %s to %s", writerOutputDir, publisherOutputDir));
      parallelRunner.movePath(writerOutputDir, this.publisherFileSystemByBranches.get(branchId),
          publisherOutputDir, this.publisherFinalDirOwnerGroupsByBranches.get(branchId));
      writerOutputPathsMoved.add(writerOutputDir);
    }
  }

	/**
	 * This method needs to be overridden for TimePartitionedCustomDataPublisher,
	 * since the output folder structure contains timestamp, we have to move the
	 * files recursively.
	 *
	 * For example, move {writerOutput}/2015/04/08/15/output.avro to
	 * {publisherOutput}/2015/04/08/15/output.avro
	 */
	@Override
	protected void addWriterOutputToExistingDir(Path writerOutput, Path publisherOutput, WorkUnitState workUnitState,
	    int branchId, ParallelRunner parallelRunner) throws IOException {
		for (FileStatus status : FileListUtils.listFilesRecursively(this.writerFileSystemByBranches.get(branchId),
		    writerOutput)) {
			String filePathStr = status.getPath().toString();
			String pathSuffix = filePathStr
			    .substring(filePathStr.indexOf(writerOutput.toString()) + writerOutput.toString().length() + 1);
			Path outputPath = new Path(publisherOutput, pathSuffix);
			WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
			    outputPath.getParent(), this.permissions.get(branchId));

			LOG.info(String.format("Moving %s to %s", status.getPath(), outputPath));
			parallelRunner.movePath(status.getPath(), this.publisherFileSystemByBranches.get(branchId), outputPath,
			    Optional.<String> absent());
		}
	}
	
	private ParallelRunner getParallelRunner(FileSystem fs) {
    String uri = fs.getUri().toString();
    if (!this.parallelRunners.containsKey(uri)) {
      this.parallelRunners.put(uri, this.closer.register(new ParallelRunner(this.parallelRunnerThreads, fs)));
    }
    return this.parallelRunners.get(uri);
  }
	
	protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    Path outputPath = WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId);
    String[] directories = outputPath.getParent().toString().split("\\/");
    String basepath = directories[0];
    String topic = directories[2];
    LOG.warn("Applift: OutputPath = "+ basepath+"/"+ topic);
    return new Path(basepath+"/"+ topic);
  }
}
