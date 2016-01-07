package gobblin.applift.simpleconsumer;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.publisher.BaseDataPublisher;
import gobblin.util.FileListUtils;
import gobblin.util.ParallelRunner;
import gobblin.util.WriterUtils;

public class TimePartitionedCustomDataPublisher extends BaseDataPublisher {
	private static final Logger LOG = LoggerFactory.getLogger(TimePartitionedCustomDataPublisher.class);

	public TimePartitionedCustomDataPublisher(State state) throws IOException {
		super(state);
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
			String[] directories = pathSuffix.split("\\/");
			
			Path basePath = new Path(publisherOutput.getName().split("\\/")[0]);
			String topic = publisherOutput.getName().split("\\/")[1];
			
			String year = directories[0];
			String month = directories[1];
			String date = directories[2];
			String hour = directories[3];
			String filename = directories[4];

			String outputPathString = year + "/" + month + "/" + date + "/" + hour + "/" + topic + "/" + filename;

			Path outputPath = new Path(basePath, outputPathString);

			WriterUtils.mkdirsWithRecursivePermission(this.publisherFileSystemByBranches.get(branchId),
			    outputPath.getParent(), this.permissions.get(branchId));

			LOG.info(String.format("Moving %s to %s", status.getPath(), outputPath));
			parallelRunner.movePath(status.getPath(), this.publisherFileSystemByBranches.get(branchId), outputPath,
			    Optional.<String> absent());
		}
	}
	
	@Override
	protected Path getPublisherOutputDir(WorkUnitState workUnitState, int branchId) {
    String basePath = WriterUtils.getDataPublisherFinalDir(workUnitState, this.numBranches, branchId).getParent().getName();
    String[] directories = basePath.split("\\/KAFKA\\/");
    return new Path(directories[0]+"/"+directories[1]);
  }

}
