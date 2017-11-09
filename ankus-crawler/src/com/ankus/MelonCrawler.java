package com.ankus;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

/* 
		Melon Site Crawling
*/

public class MelonCrawler {
	public static void main(String[] args){
		try {
			final String url_1 = "http://www.melon.com/genre/song_list.htm?gnrCode=GN0100#params%5BgnrCode%5D=GN0100&params%5BdtlGnrCode%5D=&params%5BorderBy%5D=NEW&params%5BsteadyYn%5D=N&po=pageObj&startIndex=";
			//final String url_2 = "http://www.melon.com/genre/song_list.htm?gnrCode=GN0100#params%5BgnrCode%5D=GN0100&params%5BdtlGnrCode%5D=&params%5BorderBy%5D=NEW&params%5BsteadyYn%5D=N&po=pageObj&startIndex=";
			
			final int ok = 200;
			String currentURL;
			int startNum = 51;
			//int ganre = 100;
			int status = ok;
			Connection.Response response = null;
			Document doc = null;
			String fileName = "D:\\melonchart.txt";
			String information = "";
			File file = new File(fileName);
			FileWriter fw = new FileWriter(file,true);
			while(status ==ok&&startNum<100){
				
				currentURL=url_1+String.format("%d",startNum);
				response = Jsoup.connect(currentURL).timeout(10*5000)
						.userAgent("Mozilla/5.0")
						.execute();
				status = response.statusCode();
				if(status==ok){
					doc = response.parse();
					Elements main = doc.select("div#songList").select("div.service_list_song");//
					Boolean lml = main.select("div.wrap_song_info").first().getElementsByTag("a").isEmpty();
					if( lml!=true){
						for(int p=0;p<50;p++){
							information += "Ballade"+"#"+main.select("div.wrap_song_info").select("div.ellipsis.rank01").get(p).text()+"#"+
									main.select("div.wrap_song_info").select("div.ellipsis.rank02").select("span.checkEllipsis").get(p).text()+"#"+
									main.select("div.wrap_song_info").select("div.ellipsis.rank03").get(p).text()+"\r\n";
						}
						fw.write(information);
						information="";
					}	
				}
				System.out.println(startNum);
				startNum = startNum + 50;
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
