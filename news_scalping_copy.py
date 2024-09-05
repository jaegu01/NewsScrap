import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from datetime import datetime, timedelta
import FinanceDataReader as fdr

class NaverNewsScraper:
    def __init__(self, stock_codes, stock_names, start_date, end_date):
        self.stock_codes = stock_codes
        self.stock_names = stock_names
        self.start_date = start_date
        self.end_date = end_date
        self.base_url = "https://news.naver.com/main/list.naver?mode=LS2D&mid=sec&sid1=101&sid2=258&listType=title"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'
        }
        self.news_data = []
        self.previous_titles = None

    def get(self, url):
        wait = 1
        while wait < 64:
            try:
                res = self.session.get(url, headers=self.headers)
                res.raise_for_status()
                return res
            except requests.exceptions.RequestException as e:
                print(f"Connection error: {e}. Retrying in {wait} seconds...")
                time.sleep(wait)
                wait *= 2
        raise Exception(f"Failed to fetch data from {url}")

    def parse_article_content(self, article_url):
        res = self.get(article_url)
        soup = bs(res.text, 'html.parser')

        content = soup.select_one('article#dic_area')
        if content:
            for tag in content(['script', 'style', 'div', 'span']):
                tag.extract()
            return content.get_text().strip()
        return "No content available"

    def parse_news(self, soup, date):
        articles = soup.select('ul.type02 li')
        if not articles:
            print("No articles found.")
            return False

        current_titles = [article.find('a').get_text().strip() for article in articles if article.find('a')]

        if self.previous_titles and current_titles == self.previous_titles:
            print("All titles on this page are the same as the previous page. Moving to the next date.")
            return False

        self.previous_titles = current_titles

        for article in articles:
            title_tag = article.find('a')
            if title_tag:
                title = title_tag.get_text().strip()
                link = title_tag['href']
                date_tag = article.find('span', {'class': 'date'})
                time_str = date_tag.get_text().strip() if date_tag else ""

                # 날짜와 시간을 각각 파싱한 후 결합
                time_str = time_str.replace('오전', 'AM').replace('오후', 'PM')
                news_datetime = datetime.strptime(f"{date} {time_str}", '%Y%m%d %p %I:%M')

                for code, name in zip(self.stock_codes, self.stock_names):
                    if code in title or name in title:
                        content = self.parse_article_content(link)
                        self.news_data.append({
                            '시간': news_datetime,
                            '종목명': name,
                            '종목코드': code,
                            '제목': title,
                            '내용': content
                        })
                        print(f"Found article for {name} ({code}) on {news_datetime}")

        return True

    def scrape(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y%m%d')
            print(f"\nScraping date: {date_str}")
            page_num = 1

            while True:
                url = f"{self.base_url}&date={date_str}&page={page_num}"
                res = self.get(url)
                soup = bs(res.text, 'html.parser')

                if not self.parse_news(soup, date_str):
                    print(f"\nNo more articles found on page {page_num}. Moving to the next day.")
                    break

                print(page_num, end=' ')
                page_num += 1

                time.sleep(1)

            self.previous_titles = None
            current_date += timedelta(days=1)

    def save_to_csv(self, filepath):
        if self.news_data:
            df = pd.DataFrame(self.news_data)
            df.sort_values(by='시간', ascending=False, inplace=True)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"Saved {len(df)} news items to {filepath}")

def main():
    df_kospi = fdr.StockListing('KOSPI')
    stock_names = df_kospi['Name'].tolist()
    stock_codes = df_kospi['Code'].tolist()

    start_date = datetime.strptime('2023-08-01', '%Y-%m-%d')
    end_date = datetime.strptime('2023-08-27', '%Y-%m-%d')

    scraper = NaverNewsScraper(stock_codes, stock_names, start_date, end_date)
    scraper.scrape()

    scraper.save_to_csv('data_news/news_all.csv')

if __name__ == "__main__":
    main()