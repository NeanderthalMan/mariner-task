package mariner.task;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class MergeTask {

	public static void main(String[] args) {
		String reportsCsvPath = "datamerge/reports.csv";
		String reportsJsonPath = "datamerge/reports.json";
		String reportsXmlPath = "datamerge/reports.xml";
		String outputFilePath = "merge-report.csv";

		try {
			Merge merge = new Merge(new File(reportsCsvPath), new File(reportsJsonPath), new File(reportsXmlPath),
					new File(outputFilePath));
			merge.produceReports();
		} catch (Exception e) {
			System.err.println("Failed to merge reports");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static DateTimeFormatter DateTimeFormatter = java.time.format.DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm:ss zzz").withZone(ZoneId.systemDefault());

	private static Predicate<ReportItem> NonZeroPacketsServiced = new Predicate<ReportItem>() {
		@Override
		public boolean test(ReportItem item) {
			return !item.packetsServiced.contentEquals("0");
		}
	};

	public static class Merge {
		private File csvFile;
		private File jsonFile;
		private File xmlFile;
		private File outputFile;

		public Merge(File csv, File json, File xml, File outputFile) {
			this.csvFile = csv;
			this.jsonFile = json;
			this.xmlFile = xml;
			this.outputFile = outputFile;
		}

		public ReportSummary produceReports() throws Exception {

			List<ReportItem> mergedData = doMerge();

			/*
			 * output a combined CSV file with the following characteristics:
			 * 
			 * The same column order and formatting as reports.csv
			 */
			List<String> output = new ArrayList<String>();
			output.add(
					"client-address,client-guid,request-time,service-guid,retries-request,packets-requested,packets-serviced,max-hole-size");
			output.addAll(mergedData.stream().map(item -> {
				return String.join(",", item.clientAddress, item.clientGuid, item.requestTime, item.serviceGuid,
						item.retriesRequest, item.packetsRequested, item.packetsServiced, item.maxHoleSize);
			}).collect(Collectors.toList()));
			FileUtils.writeLines(outputFile, output);

			/*
			 * Additionally, the application should print a summary showing the number of
			 * records in the output file associated with each service-guid.
			 */
			ReportSummary reportSummary = new ReportSummary();
			mergedData.stream().forEach(item -> {
				reportSummary.increment(item.serviceGuid);
			});
			reportSummary.getServiceGuidCounts().keySet().stream().forEach(key -> {
				System.out.println(String.format("service-guid [%s] :: count [%d]", key,
						reportSummary.getServiceGuidCounts().get(key)));
			});

			return reportSummary;
		}

		private List<ReportItem> doMerge() throws Exception {
			try {
				List<ReportItem> merged = new ArrayList<ReportItem>();
				/*
				 * Read the 3 input files reports.json, reports.csv, reports.xml
				 * 
				 */
				merged.addAll(this.produceReportItemsFromCsv(this.csvFile));
//				System.out.println(String.format("item count [%d]", merged.size()));
				merged.addAll(this.produceReportItemsFromJson(this.jsonFile));
//				System.out.println(String.format("item count [%d]", merged.size()));
				merged.addAll(this.produceReportItemsFromXml(this.xmlFile));
//				System.out.println(String.format("item count [%d]", merged.size()));

				/*
				 * records should be sorted by request-time in ascending order
				 */

				merged = merged.stream().sorted((item1, item2) -> {

					return LocalDateTime.parse(item1.requestTime, DateTimeFormatter)
							.compareTo(LocalDateTime.parse(item2.requestTime, DateTimeFormatter));
				}).collect(Collectors.toList());

				return merged;
			} catch (Exception e) {
				throw new Exception("unable to merge target files", e);
			}

		}

		private List<ReportItem> produceReportItemsFromCsv(File f) throws Exception {
			try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));) {

				List<ReportItem> list = null;

				list = br.lines().skip(1).map(line -> {
					String[] values = line.split(",");
					ReportItem item = new ReportItem();
					item.clientAddress = values[0];
					item.clientGuid = values[1];
					item.requestTime = values[2];
					item.serviceGuid = values[3];
					item.retriesRequest = values[4];
					item.packetsRequested = values[5];
					item.packetsServiced = values[6];
					item.maxHoleSize = values[7];
					return item;
				})
						/*
						 * All report records with packets-serviced equal to zero should be excluded
						 */
						.filter(NonZeroPacketsServiced).collect(Collectors.toList());

				return list;
			} catch (Exception e) {
				throw new Exception("unable to produce file stream for file: [" + f.getAbsolutePath() + "]");

			}
		}

		private List<ReportItem> produceReportItemsFromJson(File f) throws Exception {
			List<Map<String, Object>> jsonMaps = new ObjectMapper().readValue(f, List.class);

			List<ReportItem> items = (List<ReportItem>) jsonMaps.stream().map(linkedHashMap -> {
				ReportItem item = new ReportItem();
				item.clientAddress = "" + linkedHashMap.get("client-address");
				item.clientGuid = "" + linkedHashMap.get("client-guid");

				/*
				 * looks like request times here are epochmillis
				 */
				long epochMillis = (Long) linkedHashMap.get("request-time");
				LocalDateTime date = Instant.ofEpochMilli(epochMillis).atZone(ZoneId.systemDefault()).toLocalDateTime();

				item.requestTime = date.format(DateTimeFormatter);
				item.serviceGuid = "" + linkedHashMap.get("service-guid");
				item.retriesRequest = "" + linkedHashMap.get("retries-request");
				item.packetsRequested = "" + linkedHashMap.get("packets-requested");
				item.packetsServiced = "" + linkedHashMap.get("packets-serviced");
				item.maxHoleSize = "" + linkedHashMap.get("max-hole-size");
				return item;
			})
					/*
					 * All report records with packets-serviced equal to zero should be excluded
					 */
					.filter(NonZeroPacketsServiced).collect(Collectors.toList());

			return items;
		}

		private List<ReportItem> produceReportItemsFromXml(File f) throws Exception {

			List<Map<String, Object>> xmlMaps = new XmlMapper().readValue(f, List.class);

			List<ReportItem> items = (List<ReportItem>) xmlMaps.stream().map(linkedHashMap -> {
				ReportItem item = new ReportItem();
				item.clientAddress = "" + linkedHashMap.get("client-address");
				item.clientGuid = "" + linkedHashMap.get("client-guid");
				item.requestTime = "" + linkedHashMap.get("request-time");
				item.serviceGuid = "" + linkedHashMap.get("service-guid");
				item.retriesRequest = "" + linkedHashMap.get("retries-request");
				item.packetsRequested = "" + linkedHashMap.get("packets-requested");
				item.packetsServiced = "" + linkedHashMap.get("packets-serviced");
				item.maxHoleSize = "" + linkedHashMap.get("max-hole-size");
				return item;
			})
					/*
					 * All report records with packets-serviced equal to zero should be excluded
					 */
					.filter(NonZeroPacketsServiced).collect(Collectors.toList());

			return items;
		}
	}

	private static class ReportItem {
		private String clientAddress;
		private String clientGuid;
		private String requestTime;
		private String serviceGuid;
		private String retriesRequest;
		private String packetsRequested;
		private String packetsServiced;
		private String maxHoleSize;
	}

	public static class ReportSummary {
		private Map<String, Long> serviceGuidCounts = new HashMap<String, Long>();

		public void increment(String serviceGuid) {
			Long count = serviceGuidCounts.get(serviceGuid);
			if (count == null) {
				count = 0L;
			}
			count++;
			serviceGuidCounts.put(serviceGuid, count);
		}

		public Map<String, Long> getServiceGuidCounts() {
			return serviceGuidCounts;
		}
	}
}
