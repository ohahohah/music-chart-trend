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

public class MnetCrawler {
	public static void main(String[] args){
		try {
			final String url = "http://www.mnet.com/chart/TOP100/2011";
			final int ok = 200;
			String currentURL;
			int page = 801;
			int flag =1;
			int status = ok;
			Connection.Response response = null;
			Document doc = null;
			String fileName = "D:\\mnetchart.txt";
			String information = "";
			File file = new File(fileName);
			FileWriter fw = new FileWriter(file,true);
			while(status ==ok&&page<830){
				flag=1;
				while(flag!=3){
					currentURL=url+String.format("%04d",page)+"00?pNum="+String.format("%d", flag);
				
					response = Jsoup.connect(currentURL).timeout(10*5000)
							.userAgent("Mozilla/5.0")
							.execute();
					status = response.statusCode();
					if(status==ok){
						doc = response.parse();
						Elements main = doc.select("div.fix").select("div#content").select("div.MnetMusicList").select("div.MMLTable");//
						Boolean lml = main.select("div.MMLITitleSong_Box").first().getElementsByTag("a").isEmpty();
						if( lml!=true){
							for(int p=0;p<50;p++){
								information += (p+1+(flag-1)*50)+"#"+"2011"+String.format("%04d",page)+"#"+main.select("div.MMLITitleSong_Box").get(p).select("a.MMLI_Song").text()+"#"+
												main.select("div.MMLITitle_Info").get(p).select("a.MMLIInfo_Artist").text()+"#"+
												main.select("div.MMLITitle_Info").get(p).select("a.MMLIInfo_Album").text()+"\r\n";
							}
							fw.write(information);
							information="";
						}	
					}
					System.out.println("2011-0"+page+"-"+flag);
					flag++;
				}
				page++;
			}
			fw.flush();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
