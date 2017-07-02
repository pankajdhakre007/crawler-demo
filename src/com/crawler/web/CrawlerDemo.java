package com.crawler.web;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.validator.routines.UrlValidator;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class CrawlerDemo {

	private static DbService dbService = new DbService();
	private static String TABLE = "Record";
	private static Integer hits = 0;

	public static void main(String[] args) throws SQLException, IOException {
		dbService.truncateTable(TABLE);
		processPage("http://www.snapdeal.com");
	}

	public static void processPage(String url) throws SQLException, IOException {
		String[] schemes = { "http", "https" };
		UrlValidator urlValidator = new UrlValidator(schemes);
		if (urlValidator.isValid(url)) {
			Map<String, String> criteria = new HashMap<>();
			criteria.put("url", url);
			ResultSet rs = dbService.fetchRecords(TABLE, criteria);
			if (rs.next()) {
			} else {
				criteria.clear();
				criteria.put("url", url);
				dbService.insertRecord(TABLE, criteria);

				// get useful information
				Document doc = Jsoup.connect("http://www.snapdeal.com").get();
				if (doc.text().contains("Shoes")) {
					System.out.println(url);
					// get all links and recursively call the processPage method
					Elements questions = doc.select("a[href]");
					for (Element link : questions) {
						if (link.attr("href").contains("snapdeal.com"))
							hits++;
							processPage(link.attr("abs:href"));
					}
				}
			}
		}
	}

}
