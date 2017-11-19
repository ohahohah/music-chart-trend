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
* 2. 타입명 : MelonCrawler.java
* 3. 작성일 : 2017. 11. 20. 오전 1:26:03
* 4. 작성자 : mypc
* 5. 설명 : 멜론 사이트 크롤러
* </pre>
*/
public class MelonCrawler {
	public static void main(String[] args){
		try {
			final String url_1 = "http://www.melon.com/genre/song_listPaging.htm?startIndex=";
			final String url_2 = "&pageSize=50&gnrCode=GN";
			final String url_3 = "&dtlGnrCode=&orderBy=NEW&steadyYn=N";
			final int ok = 200;
			String currentURL;
			int startnum = 1;
			int ganrenum = 100;
			String[] Ganre = { "Ballade","Dance","Rap/Hiphop","R&B/Soul","Indie","Rock/Metal","Trot","Folk/Blues"};
			int[] Ganrecount = {26451,9951,14151,4501,14551,14401,16251,6951};
			int status = ok;
			Connection.Response response = null;
			Document doc = null;
			
			// change file route
			String fileName = "D:\\melonchart2.txt";
			String information = "";
			File file = new File(fileName);
			FileWriter fw = new FileWriter(file,true);
			while(status ==ok&&(ganrenum!=900)){
				
				currentURL=url_1+String.format("%d", startnum)+url_2+String.format("%04d",ganrenum)+url_3;
				response = Jsoup.connect(currentURL).timeout(10*5000)
						.userAgent("Mozilla/5.0")
						.execute();
				status = response.statusCode();
				if(status==ok){
					doc = response.parse();
					Elements main = doc.select("div.service_list_song");
					Boolean lml = main.select("div.wrap_song_info").first().getElementsByTag("a").isEmpty();
					if( lml!=true){
						for(int p=0;p<50;p++){
							information += Ganre[ganrenum/100-1]+"��"+main.select("div.wrap_song_info").select("div.ellipsis.rank01").get(p).text()+"��"+
									main.select("div.wrap_song_info").select("div.ellipsis.rank02").select("span.checkEllipsis").get(p).text()+"��"+
									main.select("div.wrap_song_info").select("div.ellipsis.rank03").get(p).text()+"\r\n";
						}
						fw.write(information);
						information="";
					}
					System.out.println(startnum);
					if(startnum==Ganrecount[ganrenum/100-1]){
						ganrenum = ganrenum + 100;
						startnum = 1;
					}else{
						startnum = startnum + 50;
					}
				}
				
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
