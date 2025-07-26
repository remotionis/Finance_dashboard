from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pickle
import time
import os
import traceback
import validators

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
import undetected_chromedriver as uc
import pendulum

tzinfo = pendulum.timezone("Asia/Seoul")

def run_colab(**kwargs):
    def sleep(seconds):
        for i in range(seconds):
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                continue

    def exists_by_text2(driver, text):
        try:
            WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH,"//*[contains(text(), '"+str(text)+"')]")))
        except Exception:
            return False
        return True

    def exists_by_text(driver, text):
        driver.implicitly_wait(2)
        try:
            driver.find_element(By.XPATH, "//*[contains(text(), '"+str(text)+"')]")
        except NoSuchElementException:
            driver.implicitly_wait(5)
            return False
        driver.implicitly_wait(5)
        return True

    def is_cookie_valid(cookies):
        now = time.time()
        for cookie in cookies:
            if cookie.get("name") in KEY_COOKIES:
                expiry = cookie.get("expiry")
                if expiry is None or expiry < now:
                    return False
        return True

    def user_logged_in(driver):
        try:
            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CLASS_NAME, 'gb_Ud'))
            )
            return True
        except:
            print("login check timeout")
            return False

    def login_and_save_cookie(driver, cookie_path):
        try:
            print("[LOGIN] 로그인 시도 중...")
            
            driver.get("https://accounts.google.com/signin")

            # Google 로그인 버튼 클릭
            #WebDriverWait(driver, 20).until(
            #    EC.element_to_be_clickable((By.CLASS_NAME, 'header-onegoogle-container'))
            #).click()
            print("login")

            # ID 입력
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, 'identifierId'))
            ).send_keys(Variable.get("colab_id"))
            sleep(1)
            driver.find_element(By.ID, 'identifierNext').click()
            print("input id")

            # Password 입력
            print(driver.current_url)
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='password']"))
            ).send_keys(Variable.get("colab_passwd"))
            sleep(1)
            driver.find_element(By.ID, 'passwordNext').click()
            print("input pw")

            print("[LOGIN] 로그인 정보 제출 완료. 세션 처리 대기 중...")
            driver.implicitly_wait(10)  # 로그인 세션 처리 여유 시간

            # 로그인 성공 후 쿠키 저장
            cookies = driver.get_cookies()
            with open(cookie_path, "wb") as f:
                pickle.dump(cookies, f)

            print(f"[LOGIN] 쿠키 저장 완료: {cookie_path}")
        except Exception as e:
            print("[LOGIN] 로그인 실패 또는 오류 발생:", type(e).__name__)
            traceback.print_exc()
            # 실패 시 페이지 저장 (디버깅용)
            with open("/tmp/login_fail_page.html", "w", encoding='utf-8') as f:
                f.write(driver.page_source)
                raise



    def wait_for_xpath(driver, xpath):
        while True:
            try:
                driver.find_element(By.XPATH, xpath)
                return True
            except:
                time.sleep(0.1)


    def scroll_to_bottom(driver):
        SCROLL_PAUSE_TIME = 0.5
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def wait_for_all_cells_to_finish(driver, timeout=1800, stable_checks=3):
        start_time = time.time()
        stable_count = 0
        print("셀 실행 완료 대기 중...")
        while True:
            try:
                running = driver.find_elements(By.CSS_SELECTOR, '.cell.code.running')
                pending = driver.find_elements(By.CSS_SELECTOR, '.cell.code.pending')

                if not running and not pending:
                    stable_count += 1
                    if stable_count >= stable_checks:
                        print("모든 셀 실행 완료.")
                        break
                else:
                    stable_count = 0
            except Exception as e:
                print("셀 상태 검사 중 오류 발생:", e)
                stable_count = 0

            if time.time() - start_time > timeout:
                raise TimeoutError("Timeout: 셀 실행이 완료되지 않았습니다.")
            time.sleep(2)

    try:
        COOKIE_PATH = "/opt/airflow/shared/gCookies.pkl"
        KEY_COOKIES = {"SID", "HSID", "SSID", "SAPISID"}

        # Chrome 종료 방지
        uc.Chrome.__del__ = lambda self: None
        colab_url = Variable.get('btc_full_retrain_url')

        chrome_options = uc.ChromeOptions()
        chrome_options.binary_location = "/usr/bin/google-chrome"  # 환경에 맞게 조정
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

        wd = uc.Chrome(options=chrome_options)

        wd.get(colab_url)
        sleep(2)

        cookies = []
        if os.path.exists(COOKIE_PATH):
            try:
                with open(COOKIE_PATH, "rb") as f:
                    cookies = pickle.load(f)
                if isinstance(cookies, dict):
                    cookies = [cookies]

                if is_cookie_valid(cookies):
                    for cookie in cookies:
                        wd.add_cookie(cookie)
                    wd.get(colab_url)
                    sleep(10)
                    print("cookie loaded, check login")
                    if user_logged_in(wd):
                        print("Login 유지 성공")
                    else:
                        print("쿠키는 유효하나 서버 측 무효화됨 → 로그인 시도")
                        login_and_save_cookie(wd, COOKIE_PATH)
                        wd.get(colab_url)
                else:
                    print("쿠키 만료 → 로그인 시도")
                    login_and_save_cookie(wd, COOKIE_PATH)

            except Exception as e:
                print("[ERROR] Cookie loading failed:", e)

        print("Notebook 로드 완료.")
        
        wait_for_xpath(wd, '//*[@id="file-menu-button"]/div/div/div[1]')
        wd.implicitly_wait(5)


        exec_date = kwargs['execution_date'].strftime('%Y-%m-%d')
        print(exec_date)
        WebDriverWait(wd, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'textarea')))
        lines = wd.find_elements(By.TAG_NAME, 'textarea')
        if not lines:
            raise Exception("No textarea found")
        line = lines[0]
        for _ in range(28):
            line.send_keys(Keys.CONTROL + Keys.ARROW_RIGHT)
            time.sleep(0.1)
        for _ in range(len(str(exec_date))+2):
            line.send_keys(Keys.BACKSPACE)
            time.sleep(0.2)
        line.send_keys(f"'{exec_date}'")
        print("exec_date 입력 완료")
        sleep(2)

        wd.find_element(By.TAG_NAME, 'body').send_keys(Keys.CONTROL + Keys.SHIFT + "q")
        wd.find_element(By.TAG_NAME, 'body').send_keys(Keys.CONTROL + Keys.SHIFT + "k")
        sleep(1)
        wd.find_element(By.XPATH, "//md-text-button[contains(@slot, 'primaryAction')]").click()
        sleep(3)
        wd.find_element(By.TAG_NAME, 'body').send_keys(Keys.CONTROL + Keys.F9)
        try:
            wd.find_element(By.XPATH, "//md-text-button[contains(@slot, 'primaryAction')]").click()
        except:
            pass

        wait_for_all_cells_to_finish(wd)

        print("실행 완료")
        wd.implicitly_wait(10)
    except:
        raise
    finally:
        if 'wd' in locals():
            try:
                wd.quit()
                print("브라우저 종료")
            except Exception as e:
                print(f"종료 오류: {e}")
            del wd


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='colabctl_btc_full_retrain',
    default_args=default_args,
    start_date=datetime(2025, 7, 22, 20, 10, tzinfo=tzinfo),
    schedule_interval=timedelta(days=14),
    catchup=True,
    tags=['colab']
)

task = PythonOperator(
    task_id='run_colab_notebooks',
    python_callable=run_colab,
    dag=dag
)

task
