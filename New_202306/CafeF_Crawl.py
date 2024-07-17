import time
import pandas as pd
import requests
from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import datetime
from bs4 import BeautifulSoup as bts 

def Crawl_CafeF(driver):
    data = []
    link_List = ['https://cafef.vn/xa-hoi.chn','https://cafef.vn/thi-truong-chung-khoan.chn','https://cafef.vn/bat-dong-san.chn', 'https://cafef.vn/doanh-nghiep.chn', 'https://cafef.vn/tai-chinh-ngan-hang.chn'
            ,'https://cafef.vn/tai-chinh-quoc-te.chn','https://cafef.vn/vi-mo-dau-tu.chn', 'https://cafef.vn/kinh-te-so.chn', 'https://cafef.vn/thi-truong.chn']
    driver.get("https://cafef.vn/")
    current_datetime = datetime.datetime.now()
    list_news = driver.find_elements(By.CSS_SELECTOR, "#listNewHeader > ul > li")
    if list_news:
        topic = driver.find_element(By.CSS_SELECTOR, "#admWrapsite > div.header > div.header_new > div > div > div.tinmoi > div.title_box > p").text
        for tag_li in list_news:
            tag_li_a = tag_li.find_element(By.CSS_SELECTOR, "a")
            title_content = tag_li_a.get_attribute("title")
            href_news = tag_li_a.get_attribute("href")
            data.append({
                "Topic": topic,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news
            })
    for link in link_List:
        driver.get(link)
        time.sleep(3)
        top_noibat = driver.find_element(By.CSS_SELECTOR, "#admWrapsite > div.main > div.list-section > div > div.list-main > div.noibat_cate > div.list-focus-main")
        topic = top_noibat.find_element(By.CSS_SELECTOR,"div.sub_cate > h1 > a").text
        time_news = driver.find_element(By.CSS_SELECTOR,"#admWrapsite > div.main > div.list-section > div > div.list-main > div.noibat_cate > div.list-focus-main > div.firstitem.wp100.clearfix > div > p.time").text
        title = driver.find_element(By.CSS_SELECTOR, "#admWrapsite > div.main > div.list-section > div > div.list-main > div.noibat_cate > div.list-focus-main > div.firstitem.wp100.clearfix > div > h2 > a")
        title_content = title.get_attribute("title")
        href_news = title.get_attribute("href")
        short_description = driver.find_element(By.CSS_SELECTOR, "#admWrapsite > div.main > div.list-section > div > div.list-main > div.noibat_cate > div.list-focus-main > div.firstitem.wp100.clearfix > div > p.sapo.box-category-sapo").text
        data.append({
                "Topic": topic,
                "News_Time": time_news,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": short_description
            })
        big_new_list = driver.find_elements(By.CSS_SELECTOR,"#admWrapsite > div.main > div.list-section > div > div.list-main > div.noibat_cate > div.list-focus-main > div.cate-hl-row2.mgt20 > div.big")
        for big_new in big_new_list:
            tag_a= big_new.find_element(By.CSS_SELECTOR, "h3 > a")
            title_content = tag_a.get_attribute("title")
            href_news = tag_a.get_attribute("href")
            time_news = big_new.find_element(By.CSS_SELECTOR, "p.time").text
            data.append({
                "Topic": topic,
                "News_Time": time_news,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
            })
        most_view_list = driver.find_elements(By.CSS_SELECTOR,'#loadMostViewTop > div.cate-mostview.tieudiem > div > div.cate-mostview-list > div.item')
        for mv_item in most_view_list:
            position = 'Đọc Nhiều'
            tag_a= mv_item.find_element(By.CSS_SELECTOR, "a")
            title_content = tag_a.get_attribute("title")
            href_news = tag_a.get_attribute("href")
            data.append({
                "Topic": topic,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Position": position
            })
        list_news_main = driver.find_elements(By.CSS_SELECTOR,'#hasWechoice > div.tlitem')
        for news_item in list_news_main:
            tag_a= news_item.find_element(By.CSS_SELECTOR, "h3 > a")
            title_content = tag_a.text
            href_news = tag_a.get_attribute("href")
            time_news = news_item.find_element(By.CSS_SELECTOR, 'div > span.time.time-ago').text
            short_description = news_item.find_element(By.CSS_SELECTOR, "div.tlitem-flex > div > p.sapo.box-category-sapo").text
            data.append({
                "Topic": topic,
                "News_Time": time_news,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": short_description
            })
    df_crawl_data = pd.DataFrame(data)
    df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
    # Read the existing Excel file into a DataFrame
    existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

    # Concatenate the new data with the existing data
    final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)
    # Save the concatenated DataFrame back to the Excel file
    final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")


def Crawl_VietStock(driver):
    data = []
    link_List = ['https://vietstock.vn/chu-de/1-2/moi-cap-nhat.htm','https://vietstock.vn/chung-khoan.htm','https://vietstock.vn/doanh-nghiep.htm', 'https://vietstock.vn/bat-dong-san.htm'
                    , 'https://vietstock.vn/tai-chinh.htm','https://vietstock.vn/hang-hoa.htm','https://vietstock.vn/kinh-te.htm', 'https://vietstock.vn/the-gioi.htm', 
                    'https://vietstock.vn/tai-chinh-ca-nhan.htm']
    current_time = datetime.datetime.now()
    for link in link_List: 
        driver.get(link)
        topic = driver.find_element(By.CSS_SELECTOR , 'body > div.bg-event.lr-banner-pos > div.bg-full > div > div.container > div:nth-child(1) > div > div > span:nth-child(2) > a').get_attribute('title')
        item_news_list = driver.find_elements(By.CSS_SELECTOR, '#channel-container > div.single_post')
        for item_news in item_news_list:
            news_time = item_news.find_element(By.CSS_SELECTOR, 'div.single_post_text > div.row > div > div > a:nth-child(2)').text
            tag_a = item_news.find_element(By.CSS_SELECTOR, 'div.single_post_text > h4 > a')
            title_content = tag_a.get_attribute('title')
            href_news= tag_a.get_attribute('href')
            short_description = item_news.find_element(By.CSS_SELECTOR,'div.single_post_text > p').text
            data.append({
                    "Topic": topic,
                    "News_Time": news_time,
                    "Crawl_Time": current_time,
                    "Title": title_content,
                    "Link": href_news,
                    "Short_Description": short_description
            })
    df_crawl_data = pd.DataFrame(data)
    df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
    # Read the existing Excel file into a DataFrame
    existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

    # Concatenate the new data with the existing data
    final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

    # Save the concatenated DataFrame back to the Excel file
    final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")

def Crawl_VnEconomy(driver):
    data = []
    current_time = datetime.datetime.now()
    link_list = ['https://vneconomy.vn/chung-khoan.htm','https://vneconomy.vn/tieu-dung.htm','https://vneconomy.vn/tieu-diem.htm','https://vneconomy.vn/dau-tu.htm'
                 ,'https://vneconomy.vn/tai-chinh.htm','https://vneconomy.vn/kinh-te-so.htm','https://vneconomy.vn/thi-truong.htm','https://vneconomy.vn/nhip-cau-doanh-nghiep.htm'
                 ,'https://vneconomy.vn/dia-oc.htm','https://vneconomy.vn/kinh-te-the-gioi.htm','https://vneconomy.vn/dan-sinh.htm']
    for link in link_list:
        driver.get(link)
        topic = driver.find_element(By.CSS_SELECTOR, 'body > main > div.container-xxl > div.breadcrumbs > div > h1 > a').text
        tag_a= driver.find_element(By.CSS_SELECTOR, 'body > main > div.container-xxl > section.zone.zone--highlight > div > div.row > div.col-12.col-lg-6 > article > div > h3 > a')
        title_content = tag_a.text
        href_news = tag_a.get_attribute("href")
        short_description = driver.find_element(By.CSS_SELECTOR,'body > main > div.container-xxl > section.zone.zone--highlight > div > div.row > div.col-12.col-lg-6 > article > div > div').text
        data.append({
                "Topic": topic,
                "Crawl_Time": current_time,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": short_description
        })
        tag_row = driver.find_elements(By.CSS_SELECTOR, 'body > main > div.container-xxl > section.zone.zone--highlight > div > div.row > div.col-12.col-lg-6 > div.row > div.col-md-6')
        for tag_row_item in tag_row:
            tag_a = tag_row_item.find_element(By.CSS_SELECTOR, 'article > div > h3 > a')
            title_content = tag_a.text
            href_news = tag_a.get_attribute("href")
            data.append({
                    "Topic": topic,
                    "Crawl_Time": current_time,
                    "Title": title_content,
                    "Link": href_news
            })
        tag_col = driver.find_elements(By.CSS_SELECTOR, 'body > main > div.container-xxl > section.zone.zone--highlight > div > div.row > div> article.story.story--featured')
        for tag_col_item in tag_col:
            tag_a = tag_col_item.find_element(By.CSS_SELECTOR, 'div > h3 > a')
            title_content = tag_a.text
            href_news = tag_a.get_attribute("href")
            short_description = tag_col_item.find_element(By.CSS_SELECTOR,'div > div').text
            data.append({
                    "Topic": topic,
                    "Crawl_Time": current_time,
                    "Title": title_content,
                    "Link": href_news,
                    "Short_Description": short_description
            })
        tag_list = driver.find_elements(By.CSS_SELECTOR, 'body > main > div.container-xxl > section.zone.zone--featured > div > div.row > div.col-12.col-lg-9.column-border > article.story.story--featured')
        for tag_list_item in tag_list:
            tag_a = tag_list_item.find_element(By.CSS_SELECTOR, 'header > h3 > a')
            title_content = tag_a.text
            href_news = tag_a.get_attribute("href")
            short_description = tag_list_item.find_element(By.CSS_SELECTOR,'header > div.story__summary').text
            news_time = tag_list_item.find_element(By.CSS_SELECTOR,'header > div.story__meta > time').text
            data.append({
                    "Topic": topic,
                    "News_Time": news_time,
                    "Crawl_Time": current_time,
                    "Title": title_content,
                    "Link": href_news,
                    "Short_Description": short_description
            })
    df_crawl_data = pd.DataFrame(data)
    df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
    # Read the existing Excel file into a DataFrame
    existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

    # Concatenate the new data with the existing data
    final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

    # Save the concatenated DataFrame back to the Excel file
    final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")

def Crawl_NhipCauDauTu(driver):

    link_List = ['https://nhipcaudautu.vn/kinh-doanh/', 'https://nhipcaudautu.vn/cong-nghe/',
                     'https://nhipcaudautu.vn/doanh-nhan/', 'https://nhipcaudautu.vn/chuyen-de/',
                     'https://nhipcaudautu.vn/tai-chinh/', 'https://nhipcaudautu.vn/bat-dong-san/',
                     'https://nhipcaudautu.vn/phong-cach-song/', 'https://nhipcaudautu.vn/the-gioi/',
                     'https://nhipcaudautu.vn/kieu-bao/']

    data = []
    current_datetime = datetime.datetime.now()
    for link in link_List:
        driver.get(link)
        box_list_xem_nhieu = driver.find_elements(By.CSS_SELECTOR, "#main_container > div.wrapper > div.section1 > div.row > div.col-md-4 > div.box_xemnhieu > div.warp > ul > li")
        topic = driver.find_element(By.CSS_SELECTOR, "#main_container > div.wrapper > h1").text
        position = "XEM NHIỀU"
        # Crawl các slide có trên các Topic
        list_slide = driver.find_elements(By.CSS_SELECTOR, "#main_container > div.wrapper > div.section1 > div.row > div.col-md-8 > div.slide_top > div.owl-carousel > div.owl-stage-outer > div.owl-stage > div.owl-item")
        for slide in list_slide:
            img_tag = slide.find_element(By.CSS_SELECTOR, "div > a > img")
            one_slide = slide.find_element(By.CSS_SELECTOR, "div > h3 > a")
            slide_title = img_tag.get_attribute("title")
            slide_href = one_slide.get_attribute("href")
            data.append({
                    "Topic" : topic,
                    "Position": None,
                    "News_Time": None,
                    "Crawl_Time": current_datetime,
                    "Title": slide_title,
                    "Link": slide_href,
                    "Short_Description": None
                    })
        # Crawl danh sách các tin tức được xem nhiều theo topic
        for xem_nhieu in box_list_xem_nhieu:
            tag_li_a = xem_nhieu.find_element(By.CSS_SELECTOR, "a")
            title_content = tag_li_a.text
            href_news = tag_li_a.get_attribute("href")

            data.append({
                "Topic": topic,
                "Position": position,
                "News_Time": None,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": None
            })
        # Crawl danh sách các tin tức mới theo topic
        list_new = driver.find_elements(By.CSS_SELECTOR,"#main_container > div.wrapper > div.section3 > div.col_md8 > div.container-post-wrap > div.row > div.col-xs-12 > article.post")
        for item in list_new:
            title = item.find_element(By.CSS_SELECTOR, "ul > li > div.media_body> div.entry-data > p > a")
            title_content = title.get_attribute("title")
            href_news = title.get_attribute("href")
            short_description = item.find_element(By.CSS_SELECTOR, "ul > li > div.media_body> div.entry-data > div.description").text
            data.append({
                "Topic": topic,
                "Position": None,
                "News_Time": None,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": short_description
            })
    df_crawl_data = pd.DataFrame(data)
    df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
    # Read the existing Excel file into a DataFrame
    existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

    # Concatenate the new data with the existing data
    final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

    # Save the concatenated DataFrame back to the Excel file
    final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")

def Crawl_VnExpress(driver):
    link_list = ['https://vnexpress.net/the-gioi','https://vnexpress.net/kinh-doanh','https://vnexpress.net/bat-dong-san'
                 ,'https://vnexpress.net/khoa-hoc']
    data = []
    current_datetime = datetime.datetime.now()
    for link in link_list:
        driver.get(link)
        topic = driver.find_element(By.CSS_SELECTOR,'#dark_theme > section.section.top-header.top-header-folder > div > nav > div > h1 > a').text
        header_news = driver.find_element(By.CSS_SELECTOR, '#dark_theme > section > div > div > div > article')
        tag_a = header_news.find_element(By.CSS_SELECTOR,'h3 > a')
        href_news = tag_a.get_attribute("href")
        title_content =tag_a.get_attribute("title")
        short_description = header_news.find_element(By.CSS_SELECTOR,'p > a').text
        if 'https://c.eclick.vn/' in href_news:
            pass
        else:
            data.append({
                "Topic": topic,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": short_description
            })
        item_5news = driver.find_elements(By.CSS_SELECTOR, '#automation_5News > article.item-news.item-news-common')
        for item_new in item_5news:
            tag_a = item_new.find_element(By.CSS_SELECTOR, 'p.description > a')
            href_news = tag_a.get_attribute("href")
            title_content =tag_a.get_attribute("title")
            short_description = tag_a.text
            if 'https://c.eclick.vn/' in href_news:
                pass
            else:
                data.append({
                    "Topic": topic,
                    "Crawl_Time": current_datetime,
                    "Title": title_content,
                    "Link": href_news,
                    "Short_Description": short_description
                })
    artical_list1 = driver.find_elements(By.CSS_SELECTOR, '#automation_TV0 > div > article')
    for artical in artical_list1:
        tag_a= artical.find_element(By.CSS_SELECTOR, 'p > a')
        href_news = tag_a.get_attribute("href")
        title_content =tag_a.get_attribute("title")
        short_description = tag_a.text
        if 'https://c.eclick.vn/' in href_news:
            pass
        else:
            data.append({
                "Topic": topic,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": short_description
            })
    artical_list2 = driver.find_elements(By.CSS_SELECTOR, '#automation_TV1 > div > article')
    for artical in artical_list1:
        tag_a= artical.find_element(By.CSS_SELECTOR, 'p > a')
        href_news = tag_a.get_attribute("href")
        title_content =tag_a.get_attribute("title")
        short_description = tag_a.text
        if 'https://c.eclick.vn/' in href_news:
            pass
        else:
            data.append({
                "Topic": topic,
                "Crawl_Time": current_datetime,
                "Title": title_content,
                "Link": href_news,
                "Short_Description": short_description
            })
    df_crawl_data = pd.DataFrame(data)
    df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
    # Read the existing Excel file into a DataFrame
    existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

    # Concatenate the new data with the existing data
    final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

    # Save the concatenated DataFrame back to the Excel file
    final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")






def Crawl_Cafeland(driver):
    link_list = ['https://cafeland.vn/tin-tuc/thi-truong/1/','https://cafeland.vn/tin-tuc/tai-chinh-chung-khoan/3/','https://cafeland.vn/tin-tuc/nhin-ra-the-gioi/6/','https://cafeland.vn/tin-tuc/hoat-dong-doanh-nghiep/8/'
                 ,'https://cafeland.vn/tin-tuc/ha-tang/49/','https://cafeland.vn/quy-hoach/chinh-sach-quy-hoach/2/','https://cafeland.vn/phan-tich/bao-cao-phan-tich/10/','https://cafeland.vn/tin-tuc/tin-du-an/55/']
    data = []
    current_datetime = datetime.datetime.now()
    try:
        driver.get('https://cafeland.vn/')
        topic= driver.find_element(By.CSS_SELECTOR, '#header > div.container.header-main > div.top-news-feed > div.left-top-news > div > div.box-title > span').text
        li_list = driver.find_elements(By.CSS_SELECTOR, '#header > div.container.header-main > div.top-news-feed > div.left-top-news > div > div.box-content > ul > li')
        for li in li_list:
            tag_a= li.find_element(By.CSS_SELECTOR, 'a')
            href_news = tag_a.get_attribute("href")
            title_content =tag_a.get_attribute("title")
            news_time = tag_a.find_element(By.CSS_SELECTOR, 'span.timeliveheader').text
            data.append({
                    "Topic": topic,
                    "News_Time": news_time,
                    "Crawl_Time": current_datetime,
                    "Title": title_content,
                    "Link": href_news,
            })
    except: 
        pass
    driver.close()
    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=chrome_options) 
    for link in link_list:
        try: 
            driver.get(link)
            topic = driver.find_element(By.CSS_SELECTOR, 'body > div.wrap-main.get-main-home > div.container.wrap-main-page > div.left-col > div > div.page-content > div.box-title.box-title-02.mb15 > ul > li.active > h1.sevenMainTitle').text
            li_list = driver.find_elements(By.CSS_SELECTOR, 'body > div.wrap-main.get-main-home > div.container.wrap-main-page > div.left-col > div > div.page-content > div.box-content > ul > li')
            for li in li_list:
                tag_a = li.find_element(By.CSS_SELECTOR, 'h3 > a')    
                href_news = tag_a.get_attribute("href")
                title_content =tag_a.text
                news_time = li.find_element(By.CSS_SELECTOR, 'span').text
                short_description = li.find_element(By.CSS_SELECTOR, 'p').text
                data.append({
                    "Topic": topic,
                    "News_Time": news_time,
                    "Crawl_Time": current_datetime,
                    "Title": title_content,
                    "Link": href_news,
                    "Short_Description": short_description
                })
        except:
            pass
    df_crawl_data = pd.DataFrame(data)
    df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
    # Read the existing Excel file into a DataFrame
    existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

    # Concatenate the new data with the existing data
    final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

    # Save the concatenated DataFrame back to the Excel file
    final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")
    driver.close()

def baodautu_implement():
    pages = ['https://baodautu.vn/thoi-su-d1/','https://baodautu.vn/quoc-te-d54/','https://baodautu.vn/dau-tu-d2/','https://baodautu.vn/doanh-nghiep-d3/','https://baodautu.vn/doanh-nhan-d4/','https://baodautu.vn/ngan-hang-d5/','https://baodautu.vn/batdongsan/chuyen-lang-chuyen-pho-c33/','https://baodautu.vn/batdongsan/kien-truc-phong-thuy-c35/','https://baodautu.vn/batdongsan/vat-lieu-cong-nghe-c34/','https://baodautu.vn/tai-chinh-chung-khoan-d6/','https://baodautu.vn/batdongsan/chuyen%20dong-thi-truong-c31/','https://baodautu.vn/batdongsan/du-an--quy-hoach-c32/']
    data_list = []
    for page in pages: 
        for i in range(1,4):
            web = requests.get(page+"p"+str(i))
            soup = bts(web.text, "html.parser")
            data_list.append(
                {
                    "Topic": soup.find('div',{'class':'head_four_news mb20'}).get_text(),
                            "Title": soup.find('a',{'class':'fs32 fbold'}).get_text(),
                            "Crawl_Time": datetime.datetime.now(),
                            "Link" : soup.find('a',{'class':'fs32 fbold'}).get("href"),
                            "Short_Description": soup.find('div',{'class':'sapo_thumb_news mt20'}).get_text()
                })
            #first = soup.css.select("body > main > div:nth-child(1) > section > div.col810.mr20 > div.d-flex.mb20 > div.col555.mr20 > div > article.mb20 > a.fs32.fbold")
            mydivs = soup.find_all("div", {"class": "desc_list_news_home"})
            for div in mydivs:
                data_list.append(
                            {  
                            "Topic": soup.find('div',{'class':'head_four_news mb20'}).get_text(),
                            "Title": div.find('a').get_text(),
                            "Crawl_Time": datetime.datetime.now(),
                            "Link" : div.find('a', {'class': "fs22 fbold"}).get('href'),
                            "Short_Description": div.find('div', {"class": "sapo_thumb_news"}).get_text()
                            })     
    df_crawl_data = pd.DataFrame(data_list)
    df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
    # Read the existing Excel file into a DataFrame
    existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

    # Concatenate the new data with the existing data
    final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

    # Save the concatenated DataFrame back to the Excel file
    final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")
def batdongsan_com_vn(): 
    link = 'https://batdongsan.com.vn/tin-tuc'
    data_list = []
    try:
        driver = webdriver.Chrome()
        print('Scraping batdongsan.com.vn..........')
        print('__________')
        driver.get(link)
        for i in range(3):
            time.sleep(2)
            driver.execute_script('arguments[0].click();', driver.find_element(By.CLASS_NAME,'ArticleFeed_showMoreButton__UWX16'))
        html = driver.find_elements(By.CSS_SELECTOR,'#__next > main > div > div:nth-child(3) > div.col-xl-8.col-lg-8.col-md-12.col-12 > div.ArticleFeed_wrapper__ikMl2')
        for h in html:
            top3new = h.find_elements(By.CLASS_NAME,'ArticleCardLarge_articleWrapper___8Xih')
            for new in top3new:
                data_list.append(
                    {
                        "Topic": "BẤT ĐỘNG SẢN" ,
                        "Title":new.find_element(By.CSS_SELECTOR," div.ArticleCardLarge_articleCards__HUvIR > div.ArticleCardLarge_articleContent__E_bBy > h3").text,
                        "Crawl_Time":datetime.datetime.now(),
                        "Link" :new.find_element(By.CSS_SELECTOR,"div.ArticleCardLarge_articleCards__HUvIR > div.ArticleCardLarge_articleContent__E_bBy > h3 >a").get_attribute("href"),
                        "News_Time": re.search(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}', new.find_element(By.CSS_SELECTOR,'div.ArticleCardLarge_articleCards__HUvIR > div.ArticleCardLarge_articleContent__E_bBy > div.ArticleCardLarge_articleDate__zZMrc > p >span').text).group(),
                        "Short_Description": new.find_element(By.CSS_SELECTOR,"div.ArticleCardLarge_articleCards__HUvIR > div.ArticleCardLarge_articleContent__E_bBy > div.ArticleCardLarge_articleInfo__ayTMP>p.ArticleCardLarge_articleExcerpt__wetHv").text
                    }
            )
        df_crawl_data = pd.DataFrame(data_list)
        df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
        # Read the existing Excel file into a DataFrame
        existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

        # Concatenate the new data with the existing data
        final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

        # Save the concatenated DataFrame back to the Excel file
        final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")
    except Exception:
        pass


def vietnamfinance_vn_implement(data_list,pages,i):
    driver = webdriver.Chrome()
    pages = ['tieu-diem', 'tai-chinh','ngan-hang','thi-truong','bat-dong-san','tai-chinh-quoc-te','ma','cong-nghe','nhan-vat','tai-chinh-tieu-dung','dien-dan-vnf']
    
    for page in pages:
        for iter in range(1,2):
            try:
                link = "https://vietnamfinance.vn/{}-p{}.htm/".format(page,iter)
                driver.get(link)
                topic = driver.find_element(By.CSS_SELECTOR,'#MainContentSection > div.main-wrapper > div.left-wrapper > div.page-breakcrum > h1').text
                new_focus_zone = driver.find_element(By.CSS_SELECTOR,'#MainContentSection > div.main-wrapper > div.left-wrapper > div.news-focus-by-zone')
                focus_item = new_focus_zone.find_element(By.CSS_SELECTOR,'div.focus-item')
                normal_list_item = new_focus_zone .find_element(By.CSS_SELECTOR,"div.normal-list-item")
                data_list.append(
                    {
                        'Topic': topic,
                        'Crawl_Time' : datetime.datetime.now(),
                        'Title' : focus_item.find_element(By.CSS_SELECTOR, "h2 > a").text,
                        "Link": focus_item.find_element(By.CSS_SELECTOR, "h2 > a").get_attribute("href"),
                        'Short_Description' : focus_item.find_element(By.CSS_SELECTOR, "div").text
                    },
                
                )
                for item in normal_list_item.find_elements(By.CSS_SELECTOR, "div"):
                    data_list.append(
                    {
                        'Topic': topic,
                        'Crawl_Time' :datetime.datetime.now(),
                        'Title' : focus_item.find_element(By.CSS_SELECTOR, "h2 > a ").text,
                        "Link": focus_item.find_element(By.CSS_SELECTOR, "h2 > a").get_attribute("href"),
                    }
                )
                block_timeline = driver.find_element(By.CSS_SELECTOR,"#MainContentSection > div.main-wrapper > div.left-wrapper > div.block-timeline")
                for item in block_timeline.find_elements(By.CSS_SELECTOR, "div.timeline-item"):
                    data_list.append(
                        {
                            'Topic': topic,
                            'Crawl_Time' : datetime.datetime.now(),
                            'Title' : item.find_element(By.CSS_SELECTOR, "div > h3 > a").text,
                            "Link": item.find_element(By.CSS_SELECTOR, "div > h3 > a").get_attribute("href"),
                            "News_Time" : re.compile(r'[^0-9, : /]+').sub('', item.find_element(By.CSS_SELECTOR, "div > span").text).strip()
                        }
                    )
                for item in block_timeline.find_elements(By.CLASS_NAME, "timeline-item.x2-item"):
                    data_list.append(
                        {
                            'Topic': topic,
                            'Crawl_Time' : datetime.datetime.now(),
                            'Title' : item.find_element(By.CSS_SELECTOR, "div > h3 > a").text,
                            "Link": item.find_element(By.CSS_SELECTOR, "div > h3 > a").get_attribute("href"),
                            "News_Time" : re.compile(r'[^0-9, : /]+').sub("",item.find_element(By.CSS_SELECTOR, "div > span").text).strip()
                        }
                        )
                df_crawl_data = pd.DataFrame(data_list)
                df_crawl_data = df_crawl_data.drop_duplicates(subset=['Link'])
                # Read the existing Excel file into a DataFrame
                existing_df = pd.read_excel("Data_News.xlsx", sheet_name="Sheet1")

                # Concatenate the new data with the existing data
                final_df = pd.concat([existing_df, df_crawl_data], ignore_index=True)

                # Save the concatenated DataFrame back to the Excel file
                final_df.to_excel("Data_News.xlsx", index=False, sheet_name="Sheet1")    
            except:
                print('_____________')
                pass
    time.sleep(2)
    driver.quit()
def main(): 
    chrome_options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(options=chrome_options) 
    Crawl_VietStock(driver)
    print('Crawl_VietStock')
    Crawl_VnEconomy(driver)
    print('Crawl_VnEconomy')
    Crawl_NhipCauDauTu(driver)
    print('Crawl_NhipCauDauTu')
    Crawl_CafeF(driver)
    print('Crawl_CafeF')
    Crawl_Cafeland(driver)
    print('Crawl_Cafeland')
    baodautu_implement()
    print('baodautu_implement')
    Crawl_VnExpress(driver)
    batdongsan_com_vn()
    
    driver.close()


if __name__ == "__main__":
    main()