import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from datetime import datetime, timedelta
import FinanceDataReader as fdr
import multiprocessing
import random
import calendar

class NaverNewsScraper:
    def __init__(self, stock_codes, stock_names, start_date, end_date, process_id):
        self.stock_codes = stock_codes
        self.stock_names = stock_names
        self.start_date = start_date
        self.end_date = end_date
        self.process_id = process_id
        self.base_url = "https://news.naver.com/main/list.naver?mode=LS2D&mid=sec&sid1=101&sid2=258&listType=title"
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.79 Safari/537.36'
        }
        self.news_data = []
        self.previous_titles = None

    def get(self, url):
        wait = 1
        retries = 0
        max_retries = 5
        while retries < max_retries:
            try:
                res = self.session.get(url, headers=self.headers)
                res.raise_for_status()
                return res
            except (requests.exceptions.RequestException, OSError) as e:
                print(f"[Process {self.process_id}] Connection error: {e}. Retrying in {wait} seconds...")
                time.sleep(wait)
                wait *= 2
                retries += 1
        print(f"[Process {self.process_id}] Failed to fetch data from {url} after {max_retries} attempts.")
        return None

    def parse_article_content(self, article_url):
        res = self.get(article_url)
        if res is None:
            return "Failed to retrieve content"
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
            print(f"[Process {self.process_id}] No articles found on this page.")
            return False

        current_titles = [article.find('a').get_text().strip() for article in articles if article.find('a')]

        if self.previous_titles == current_titles:
            print(f"[Process {self.process_id}] All titles on this page are the same as the previous page. Moving to the next date.")
            return False

        self.previous_titles = current_titles

        parsed_any = False
        for article in articles:
            try:
                title_tag = article.find('a')
                if title_tag:
                    title = title_tag.get_text().strip()
                    link = title_tag['href']
                    date_tag = article.find('span', {'class': 'date'})
                    time_str = date_tag.get_text().strip() if date_tag else ""

                    time_str = time_str.split()[-2:]
                    time_str = ' '.join(time_str).replace('오전', 'AM').replace('오후', 'PM')
                    
                    try:
                        news_datetime = datetime.strptime(f"{date} {time_str}", '%Y%m%d %p %I:%M')
                    except ValueError as ve:
                        print(f"Error parsing date and time: {ve}")
                        continue

                    for code, name in zip(self.stock_codes, self.stock_names):
                        if code in title or name in title:
                            try:
                                content = self.parse_article_content(link)
                                self.news_data.append({
                                    '시간': news_datetime,
                                    '종목명': name,
                                    '종목코드': code,
                                    '제목': title,
                                    '내용': content
                                })
                                parsed_any = True
                                #print(f"[Process {self.process_id}] Found article for {name} ({code}) on {news_datetime}")
                            except Exception as e:
                                print(f"Error parsing article content: {e}")
                                continue
            except Exception as e:
                print(f"Error processing article: {e}")
                continue
        return parsed_any

    def scrape(self):
        current_date = self.start_date
        session_start_time = time.time()

        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y%m%d')
            print(f"\n[Process {self.process_id}] Scraping date: {date_str}")
            page_num = 1

            while True:
                url = f"{self.base_url}&date={date_str}&page={page_num}"
                res = self.get(url)
                if res is None:
                    print(f"[Process {self.process_id}] Skipping page {page_num} on date {date_str} due to repeated failures.")
                    break
                soup = bs(res.text, 'html.parser')

                if not self.parse_news(soup, date_str):
                    print(f"[Process {self.process_id}] No more new articles or repeated articles detected. Moving to the next date.")
                    break

                print(f"[Process {self.process_id}] {date_str}, {page_num}page.")
                page_num += 1

                time.sleep(5)

                if time.time() - session_start_time > 1800:
                    print(f"[Process {self.process_id}] Restarting session due to long session duration.")
                    self.session.close()
                    self.session = requests.Session()
                    session_start_time = time.time()

            current_date += timedelta(days=1)
            self.previous_titles = None

            if current_date.day == 1 or current_date > self.end_date:
                self.save_to_csv(f'data_news/news_{self.start_date.strftime("%Y-%m-%d")}_to_{(current_date - timedelta(days=1)).strftime("%Y-%m-%d")}_proc_{self.process_id}.csv')
                self.news_data = []

        if self.news_data:
            self.save_to_csv(f'data_news/news_{self.start_date.strftime("%Y-%m-%d")}_to_{self.end_date.strftime("%Y-%m-%d")}_proc_{self.process_id}.csv')

    def save_to_csv(self, filepath):
        if self.news_data:
            df = pd.DataFrame(self.news_data)
            df.sort_values(by='시간', ascending=False, inplace=True)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"[Process {self.process_id}] Saved {len(df)} news items to {filepath}")

def run_scraper(stock_codes, stock_names, start_date, end_date, process_id):
    delay = random.uniform(1, 5)
    print(f"Process {process_id} for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} starting after {delay:.2f} seconds delay.")
    time.sleep(delay)
    
    scraper = NaverNewsScraper(stock_codes, stock_names, start_date, end_date, process_id)
    scraper.scrape()

def main():
    df_kospi = fdr.StockListing('KOSPI')
    stock_names = df_kospi['Name'].tolist()
    stock_codes = df_kospi['Code'].tolist()

    start_date = '2017-04-01'
    end_date = '2017-4-30'
    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')

    periods = []
    current_start = start_date_dt
    while current_start <= end_date_dt:
        year, month = current_start.year, current_start.month
        last_day_of_month = calendar.monthrange(year, month)[1]
        current_end = datetime(year, month, last_day_of_month)
        if current_end > end_date_dt:
            current_end = end_date_dt
        periods.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)

    max_processes = 6
    with multiprocessing.Pool(processes=max_processes) as pool:
        pool.starmap(run_scraper, [(stock_codes, stock_names, start, end, i) for i, (start, end) in enumerate(periods)])

if __name__ == "__main__":
    main()