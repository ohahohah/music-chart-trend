package com.ankus;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

/**
* <pre>
* 1. 패키지명 : com.ankus
* 2. 타입명 : GaonCrawler.java
* 3. 작성일 : 2017. 11. 20. 오전 1:24:47
* 4. 작성자 : mypc
* 5. 설명 : 가온 사이트 크롤러
* </pre>
*/
public class GaonCrawler {
	public static void main(String[] args){
		try {
			final String url_1 = "http://gaonchart.co.kr/main/section/chart/album.gaon?nationGbn=T&serviceGbn=&targetTime=";
			final String url_2 = "&hitYear=";
			final String url_3 = "&termGbn=month";
			final int ok = 200;
			String currentURL;
			int year = 2011;
			int month = 1;
			int status = ok;
			Connection.Response response = null;
			Document doc = null;
			// change file route
			String fileName = "D:\\gaonchart3.txt";
			String information = "";
			File file = new File(fileName);
			FileWriter fw = new FileWriter(file,true);
			while(status ==ok){
				currentURL=url_1+String.format("%02d", month)+url_2+String.format("%d",year)+url_3;
				response = Jsoup.connect(currentURL).timeout(10*5000)
						.userAgent("Mozilla/5.0")
						.execute();
				status = response.statusCode();
				if(status==ok){
					doc = response.parse();
					Elements main = doc.select("div#wrap").select("div.chart");
					Boolean lml = main.select("td.subject").first().getElementsByTag("p").isEmpty();
					if( lml!=true){
						for(int p=0;p<100;p++){
							information += (p+1)+"��"+main.select("td.subject").get(p).getElementsByTag("p").get(0).text()+"��"+
									main.select("td.subject").get(p).getElementsByTag("p").get(1).text()+"��"+
									main.select("td.count").get(p).getElementsByTag("p").text()+"\r\n";
						}
						fw.write(information);
						information="";
					}
					System.out.println(year+"-"+month);
					month++;
					if(month==13){
						month=1;
						year++;
					}
					if(year==2017&&month==12){
						break;
					}
				}
				
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
