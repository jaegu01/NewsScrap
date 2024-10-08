import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import time
from datetime import datetime, timedelta
import random
import calendar
import multiprocessing

class NaverNewsScraper:
    def __init__(self, start_date, end_date, process_id):
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
        # 요청 간 랜덤한 대기 시간 추가
        #time.sleep(random.uniform(1, 5))  # 1~5초 사이 랜덤 대기
        res = self.get(article_url)
        if res is None:
            return "Failed to retrieve content"
        soup = bs(res.text, 'html.parser')
        content = soup.select_one('article#dic_area')
        if content:
            for tag in content(['script', 'style', 'div', 'span']):
                tag.extract()  # 불필요한 태그 제거
            return content.get_text().strip()  # 기사 내용 텍스트로 반환
        return "No content available"

    def parse_news(self, soup, date):
        articles = soup.select('ul.type02 li')
        if not articles:
            print(f"[Process {self.process_id}] No articles found on this page.")
            return False

        current_titles = [article.find('a').get_text().strip() for article in articles if article.find('a')]

        # 중복된 타이틀 확인
        if self.previous_titles == current_titles:
            print(f"[Process {self.process_id}] All titles on this page are the same as the previous page. Moving to the next date.")
            return False

        self.previous_titles = current_titles  # 현재 페이지 타이틀을 저장하여 다음과 비교

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

                    try:
                        content = self.parse_article_content(link)
                        self.news_data.append({
                            '시간': news_datetime,
                            '제목': title,
                            '내용': content
                        })
                        parsed_any = True
                        print(title)
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

                print(f"[Process {self.process_id}] {date_str}, {page_num} page.")
                page_num += 1

                # 요청 사이 가변적인 딜레이 설정
                time.sleep(random.uniform(5, 10))  # 5초에서 10초 사이 랜덤 대기

                if time.time() - session_start_time > 1800:
                    print(f"[Process {self.process_id}] Restarting session due to long session duration.")
                    self.session.close()
                    self.session = requests.Session()
                    session_start_time = time.time()

            current_date += timedelta(days=1)
            self.previous_titles = None

            if current_date.day == 1 or current_date > self.end_date:
                self.save_to_csv(f'data_news_all/news_all_{self.start_date.strftime("%Y-%m-%d")}_to_{(current_date - timedelta(days=1)).strftime("%Y-%m-%d")}_proc_{self.process_id}.csv')
                self.news_data = []

        if self.news_data:
            self.save_to_csv(f'data_news_all/news_all_{self.start_date.strftime("%Y-%m-%d")}_to_{self.end_date.strftime("%Y-%m-%d")}_proc_{self.process_id}.csv')

    def save_to_csv(self, filepath):
        if self.news_data:
            df = pd.DataFrame(self.news_data)
            df.sort_values(by='시간', ascending=False, inplace=True)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"[Process {self.process_id}] Saved {len(df)} news items to {filepath}")

def run_scraper(start_date, end_date, process_id):
    delay = random.uniform(1, 5)
    print(f"Process {process_id} for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} starting after {delay:.2f} seconds delay.")
    time.sleep(delay)
    
    scraper = NaverNewsScraper(start_date, end_date, process_id)
    scraper.scrape()

def main():
    start_date = '2015-01-01'
    end_date = '2015-12-31'
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

    max_processes = 4
    with multiprocessing.Pool(processes=max_processes) as pool:
        pool.starmap(run_scraper, [(start, end, i) for i, (start, end) in enumerate(periods)])

if __name__ == "__main__":
    main()