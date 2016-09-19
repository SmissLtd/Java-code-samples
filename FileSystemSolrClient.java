package com.oxseed.utils.verification.solrclient;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

import com.oxseed.data.order.Document;
import com.oxseed.data.order.Order;
import com.oxseed.data.order.Process;
import com.oxseed.data.util.XMLSerializer;
/**
 * Client for Solr, which returns json
 */
public class FileSystemSolrClient extends SolrClient implements InitializingBean{
	private final static Log LOG = LogFactory.getLog(FileSystemSolrClient.class);

	private String baseFolder;

	private List<File> cachedListOfFiles;

	private final ThreadLocal<String> cachedLastSearchString = new ThreadLocal<>();
	private final ThreadLocal<String> cachedLastFoundId = new ThreadLocal<>();
	private final ThreadLocal<Integer> cachedLastFoundStartRow = new ThreadLocal<>();

	private SolrClient solrClientForCount;

	@Override
	public void afterPropertiesSet() throws Exception {
		LOG.debug("Caching list of files in " + baseFolder);
		if (new File(baseFolder).exists() && new File(baseFolder).isDirectory()) {
			cachedListOfFiles = Arrays.asList(new File(baseFolder).listFiles());
		}
	}

	@Override
	public int getResultCount(String searchString) {
		if (solrClientForCount == null) {
			LOG.warn("solrClientForCount isn't set - COUNT operation will take a lot of time");
			Map<String, List<String>> requestResult = request(searchString, 0, Integer.MAX_VALUE);
			cachedLastSearchString.set(null);
			for (String indexName : requestResult.keySet()) {
				return requestResult.get(indexName).size();
			}
			return 0;
		} else {
			return solrClientForCount.getResultCount(searchString);
		}
	}

	@Override
	public Map<String, List<String>> request(String searchString, int start, int rows) {
		LOG.debug("GET " + searchString + "&start=" + start + "&rows=" + rows);

		List<File> orderFolders = listFiles(buildDateFilter(searchString));
		if (orderFolders.isEmpty()){
			return Collections.emptyMap();
		}

		if (!searchString.equals(cachedLastSearchString.get())) {
			cachedLastSearchString.set(searchString);
			cachedLastFoundId.set(null);
			cachedLastFoundStartRow.set(-1);
		}

		AtomicInteger skippedIndex = new AtomicInteger(cachedLastFoundStartRow.get());
		AtomicInteger acceptedCount = new AtomicInteger(0);

		AtomicBoolean rightToLastFoundId = new AtomicBoolean(cachedLastFoundId.get() == null);

		Map<String, List<String>> indexes =
			orderFolders
			.stream()
				//optimisation
				.filter(orderFolder -> acceptedCount.get() < rows)
				.filter(orderFolder -> rightToLastFoundId.get()
							|| rightToLastFoundId.compareAndSet(!orderFolder.getName().equals(cachedLastFoundId.get()), true))
				//---
				.filter(orderFolder -> new File(orderFolder, "order.xml").exists())
				.map(orderFolder -> loadOrder(orderFolder))
				.flatMap(order -> order.getStack().getProcessList().stream())
				.reduce(new HashMap<>(), (map, process) -> {
					LOG.debug("Processing process, skippedIndex=" + skippedIndex.get() + ", start=" + start);
					process.getDocumentList().stream()
						.filter(document -> skippedIndex.incrementAndGet() >= start)
						.filter(document -> acceptIndex(process, document, searchString))
						.filter(document -> acceptedCount.getAndIncrement() < rows)
						.flatMap(document -> Stream.concat(process.getIndexData().getIndexMap().entrySet().stream(),
								document.getIndexData().getIndexMap().entrySet().stream()))
						.forEach((entry) -> {
							String value = entry.getValue().stream().collect(Collectors.joining(",")).trim();
							if (map.containsKey(entry.getKey())) {
								map.get(entry.getKey()).add(value.trim());
							} else {
								map.put(entry.getKey(), new ArrayList<>(Arrays.asList(value.trim())));
							}
							//optimisation
							if ("pi_order_id_str".equals(entry.getKey())){
								cachedLastFoundId.set(value);
							}
							cachedLastFoundStartRow.set(skippedIndex.get());
							//---
						});
					return map;
				}, (a, b) -> {
					a.putAll(b);
					return a;
				});

//		LOG.debug("Extracted: " + indexes);
		return indexes;
	}

	private boolean acceptIndex(Process process, Document document, String searchString) {
		Map<String, List<String>> map = Stream.concat(process.getIndexData().getIndexMap().entrySet().stream(),
				document.getIndexData().getIndexMap().entrySet().stream())
			.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		boolean andComparasion = true;
		for (String indexPairWithOr : searchString.split(" +[aA][nN][dD] +")) {

			boolean orComparasion = false;
			for (String indexPair : indexPairWithOr.split(" +[oO][rR] +")) {
				String[] index = indexPair.split(":");
				String indexName = index[0];
				String indexValue = index[1];

				boolean isNot = false;
				if (isNot = indexName.startsWith("-")){
					indexName = indexName.substring(1);
				} else if (isNot = indexName.startsWith("NOT ")){
					indexName = indexName.substring("NOT ".length());
				}

				if (!map.containsKey(indexName)){
					break;
				}

				orComparasion |= !isNot == map.get(indexName).stream().anyMatch(actualIndexValue -> acceptIndex(indexValue, actualIndexValue));
			}
			andComparasion &= orComparasion;
			if (!andComparasion){
				return false;
			}
		}
		return andComparasion;
	}

	private boolean acceptIndex(String query, String actual){
		if (actual == null) {
			return false;
		}
		String queryWithoutAsterix = query.replaceAll("\\*", "");
		if (query.startsWith("*") && query.endsWith("*")){
			return actual.contains(queryWithoutAsterix);
		} else if (query.endsWith("*")){
			return actual.startsWith(queryWithoutAsterix);
		} else if (query.startsWith("*")){
			return actual.endsWith(queryWithoutAsterix);
		}
		return actual.equals(query);
	}

	private Order loadOrder(File orderFolder){
		LOG.debug("Loaded order: " + orderFolder);
		try {
			Order order = (Order) XMLSerializer.getInstance().readObject(new FileInputStream(new File(orderFolder, "order.xml")));
			for (Process process : order.getStack().getProcessList()) {
				process.getIndexData().getIndexMap().putSingle("pi_path_to_order_xml", orderFolder.getAbsolutePath());
			}
			return order;
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private String buildDateFilter(String searchString){
		return searchString.split("_id_str")[1].split("[: ]")[1].replaceAll("[^\\d]", "");
	}

	private List<File> listFiles(String dateFilter){
		return cachedListOfFiles.stream()
				.filter(file -> file.getName().startsWith("order") || file.getName().startsWith(dateFilter))
				.collect(Collectors.toList());
	}

	public String getBaseFolder() {
		return baseFolder;
	}

	public void setBaseFolder(String baseFolder) {
		this.baseFolder = baseFolder;
	}

	public SolrClient getSolrClientForCount() {
		return solrClientForCount;
	}

	public void setSolrClientForCount(SolrClient solrClientForCount) {
		this.solrClientForCount = solrClientForCount;
	}
}
