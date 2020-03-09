package mariner.task;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import mariner.task.MergeTask.Merge;
import mariner.task.MergeTask.ReportSummary;

public class MergeTaskTest {

	@Test(expected = Exception.class)
	public void testMerge_Fail_0() throws Exception {

		File csvFile = new File("test/mariner/task/reports.csv_no_exist");
		File jsonFile = new File("test/mariner/task/reports.json");
		File xmlFile = new File("test/mariner/task/reports.xml");
		File outputReportFile = new File("merge-report.csv");

		Merge merge = new MergeTask.Merge(csvFile, jsonFile, xmlFile, outputReportFile);
		ReportSummary summary = merge.produceReports();
	}

	@Test(expected = Exception.class)
	public void testMerge_Fail_1() throws Exception {

		File csvFile = new File("test/mariner/task/reports.csv");
		File jsonFile = new File("test/mariner/task/reports.json_no_exist");
		File xmlFile = new File("test/mariner/task/reports.xml");
		File outputReportFile = new File("merge-report.csv");

		Merge merge = new MergeTask.Merge(csvFile, jsonFile, xmlFile, outputReportFile);
		ReportSummary summary = merge.produceReports();
	}

	@Test(expected = Exception.class)
	public void testMerge_Fail_2() throws Exception {

		File csvFile = new File("test/mariner/task/reports.csv");
		File jsonFile = new File("test/mariner/task/reports.json");
		File xmlFile = new File("test/mariner/task/reports.xml_no_exist");
		File outputReportFile = new File("merge-report.csv");

		Merge merge = new MergeTask.Merge(csvFile, jsonFile, xmlFile, outputReportFile);
		ReportSummary summary = merge.produceReports();
	}

	@Test
	public void testMerge_Success() throws Exception {

		File csvFile = new File("test/mariner/task/reports.csv");
		File jsonFile = new File("test/mariner/task/reports.json");
		File xmlFile = new File("test/mariner/task/reports.xml");
		File outputReportFile = new File("merge-report.csv");

		outputReportFile.delete();
		assertTrue(!outputReportFile.exists());

		Merge merge = new MergeTask.Merge(csvFile, jsonFile, xmlFile, outputReportFile);
		ReportSummary summary = merge.produceReports();

		assertTrue(outputReportFile.exists());
		assertTrue(summary.getServiceGuidCounts().get("caaca31e-bee2-4ed3-8e72-5dab24079744") == 1);
		assertTrue(summary.getServiceGuidCounts().get("7d619a45-2b4d-4a54-9e85-6913c9545e34") == 1);
		assertTrue(summary.getServiceGuidCounts().get("04402d03-0952-4d42-8c1f-f2d0b1a9f376") == 1);
		assertTrue(summary.getServiceGuidCounts().get("04402d03-0952-4d42-8c1f-f2d0b1a9f377") == 1);
		assertTrue(summary.getServiceGuidCounts().get("3cc76b74-7d16-4651-9699-34332a56f6e7") == 2);
		assertTrue(summary.getServiceGuidCounts().size() == 5);
		Path path = FileSystems.getDefault().getPath(".", outputReportFile.getName());
		long count = Files.lines(path, Charset.defaultCharset()).count();
		assertTrue(count == 7); // 1+1+1+1+2+header
	}
	
	@Test
	public void testMerge_CheckAscendingOrderChronologically() throws Exception {

		File csvFile = new File("test/mariner/task/reports.csv");
		File jsonFile = new File("test/mariner/task/reports.json");
		File xmlFile = new File("test/mariner/task/reports.xml");
		File outputReportFile = new File("merge-report.csv");

		outputReportFile.delete();
		assertTrue(!outputReportFile.exists());

		Merge merge = new MergeTask.Merge(csvFile, jsonFile, xmlFile, outputReportFile);
		ReportSummary summary = merge.produceReports();

		Path path = FileSystems.getDefault().getPath(".", outputReportFile.getName());
		List<String> lines = Files.lines(path, Charset.defaultCharset()).skip(1).collect(Collectors.toList());
		
		LocalDateTime lastDateTime = null;
		for (String line : lines) {
			String dateTimeString = line.split(",")[2];
			LocalDateTime mostRecentDateTime = LocalDateTime.parse(dateTimeString, MergeTask.DateTimeFormatter);
			if( lastDateTime == null) {
				lastDateTime = mostRecentDateTime;
				return;
			}
			if(!mostRecentDateTime.isBefore(lastDateTime))
			{
				throw new Exception("report entries are not in ascending chronological order");
			}
		}
		
	}

}
