from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import os
from datetime import datetime
import gspread
from google.oauth2 import service_account

class GoogleSheetsParser:
    def __init__(self):
        self.driver = None
        self.google_sheet_id = "1Xd4kikdV3FT8EtGYZOpCONfzs08gp86q8xEBE5OXbkY"
        self.worksheet = None
        self.last_page_file = r"C:\Users\Vasiliy\Desktop\Парсер_лицензий\last_page_gsheets.txt"
        self.dental_count = 0
        self.processed_count = 0
        self.skipped_count = 0
        self.duplicates_count = 0
        self.credentials_file = r"C:\Users\Vasiliy\Desktop\Парсер_лицензий\stomatologyscraper-7f64e5b6d7b7.json"
        self.target_url = "https://license.gov.uz/registry?filter%5Bdocument_id%5D=2908&filter%5Bdocument_type%5D=LICENSE"
        self.existing_records = set()
    
    def setup_google_sheets(self):
        print("Подключение к Google Sheets...")
        
        try:
            if not os.path.exists(self.credentials_file):
                print(f"Файл не найден: {self.credentials_file}")
                raise Exception("Файл credentials не найден")
            
            print(f"Найден файл credentials: {os.path.basename(self.credentials_file)}")
            
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive']
            )
            
            gc = gspread.authorize(credentials)
            spreadsheet = gc.open_by_key(self.google_sheet_id)
            self.worksheet = spreadsheet.get_worksheet(0)
            
            print(f"Подключено к Google Sheets")
            print(f"Таблица: {spreadsheet.title}")
            print(f"Лист: {self.worksheet.title}")
            print(f"Ссылка: https://docs.google.com/spreadsheets/d/{self.google_sheet_id}")
            
            headers = self.worksheet.row_values(1)
            if not headers or 'RegNumber_label' not in headers:
                new_headers = [
                    'RegNumber_label',
                    'Дата',
                    'ИНН',
                    'Флаг Сети',
                    'Название',
                    'Статус организации',
                    'Адрес',
                    'Специализации в лицензии',
                    'ВидДеятельности'
                ]
                self.worksheet.update('A1:I1', [new_headers])
                print("Заголовки созданы/обновлены")
            else:
                print(f"Заголовки существуют")
            
            self.load_existing_records()
                
        except Exception as e:
            print(f"Ошибка подключения к Google Sheets: {str(e)}")
            raise
    
    def load_existing_records(self):
        try:
            print("Загружаем существующие записи для проверки дубликатов...")
            
            all_values = self.worksheet.get_all_values()
            
            if len(all_values) <= 1:
                print("Таблица пустая или содержит только заголовки")
                return
            
            for row in all_values[1:]:
                if len(row) >= 3:
                    license_num = str(row[0]).strip()
                    inn = str(row[2]).strip()
                    
                    if inn and license_num and inn != '' and license_num != '':
                        unique_key = f"{inn}_{license_num}"
                        self.existing_records.add(unique_key)
                        
            print(f"Загружено {len(self.existing_records)} существующих записей")
            
            if self.existing_records:
                sample_keys = list(self.existing_records)[:3]
                print(f"Примеры ключей: {sample_keys}")
            
        except Exception as e:
            print(f"Не удалось загрузить существующие записи: {str(e)}")
            print("Продолжаем работу без проверки дубликатов")
    
    def check_duplicate(self, inn, license_num):
        if not inn or not license_num:
            return False
            
        unique_key = f"{inn}_{license_num}"
        is_duplicate = unique_key in self.existing_records
        
        if is_duplicate:
            print(f"Проверка: ИНН={inn}, Лицензия={license_num} - ДУБЛИКАТ")
        else:
            print(f"Проверка: ИНН={inn}, Лицензия={license_num} - новая запись")
            
        return is_duplicate
    
    def setup_driver(self):
        print("Запуск браузера...")
        
        self.driver = Driver(
            browser="chrome",
            uc=True,
            headless=False,
            locale_code="ru"
        )
        
        print("Браузер запущен")
    
    def get_last_processed_page(self):
        try:
            if os.path.exists(self.last_page_file):
                with open(self.last_page_file, 'r') as f:
                    return int(f.read().strip())
        except:
            pass
        return 0
    
    def save_last_processed_page(self, page_num):
        try:
            os.makedirs(os.path.dirname(self.last_page_file), exist_ok=True)
            with open(self.last_page_file, 'w') as f:
                f.write(str(page_num))
        except:
            pass
    
    def add_to_google_sheets(self, record):
        try:
            inn = str(record.get('ИНН', '')).strip()
            license_num = str(record.get('Номер документа', '')).strip()
            
            if inn and license_num:
                if self.check_duplicate(inn, license_num):
                    self.duplicates_count += 1
                    return False
            
            row_data = [
                license_num,
                record.get('Дата выдачи', ''),
                inn,
                '',
                record.get('Наименование лицензиата', '').replace('"', '').strip(),
                record.get('Статус', 'Активный'),
                record.get('Адрес деятельности', ''),
                record.get('Специализации', ''),
                ''
            ]
            
            self.worksheet.append_row(row_data, value_input_option='USER_ENTERED')
            
            if inn and license_num:
                unique_key = f"{inn}_{license_num}"
                self.existing_records.add(unique_key)
                print(f"НОВАЯ запись сохранена в Google Sheets")
            
            self.dental_count += 1
            print(f"Всего стоматологических в таблице: {self.dental_count}")
            return True
            
        except Exception as e:
            print(f"Ошибка сохранения: {str(e)[:100]}")
            
            try:
                time.sleep(2)
                print("Переподключаемся к Google Sheets...")
                self.setup_google_sheets()
                
                self.worksheet.append_row(row_data, value_input_option='USER_ENTERED')
                
                if inn and license_num:
                    unique_key = f"{inn}_{license_num}"
                    self.existing_records.add(unique_key)
                
                self.dental_count += 1
                print(f"Сохранено после переподключения")
                return True
            except:
                print(f"Не удалось сохранить даже после переподключения")
                return False
    
    def select_russian_language(self):
        try:
            print("Выбираем русский язык...")
            
            language_selectors = [
                '//div[contains(@class, "LanguageSwitcher")]//div[text()="РУ"]',
                '//div[contains(@class, "LanguageSwitcher")]//div[contains(text(), "РУ")]',
                '//button[text()="РУ"]',
                '//a[text()="РУ"]',
                '//*[contains(@class, "language")]//span[text()="РУ"]',
                '//*[contains(@class, "lang")]//span[text()="РУ"]'
            ]
            
            for selector in language_selectors:
                try:
                    lang_button = self.driver.find_element(By.XPATH, selector)
                    if lang_button.is_displayed():
                        lang_button.click()
                        print("Русский язык выбран")
                        time.sleep(2)
                        return True
                except:
                    continue
            
            try:
                flag_button = self.driver.find_element(By.CSS_SELECTOR, '[class*="flag-ru"], [class*="russia"]')
                flag_button.click()
                print("Русский язык выбран через флаг")
                time.sleep(2)
                return True
            except:
                pass
            
            print("Не удалось найти переключатель языка")
            return False
            
        except Exception as e:
            print(f"Ошибка выбора языка: {str(e)[:50]}")
            return False
    
    def wait_for_table_and_navigate(self, target_page):
        print("Ожидаем загрузки страницы с фильтрами...")
        
        self.select_russian_language()
        
        max_wait = 180
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
                if not rows:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
                
                data_rows = [row for row in rows if row.text and ('Медицина' in row.text or 'Лицензия' in row.text)]
                
                if len(data_rows) > 0:
                    print(f"Таблица загружена, найдено {len(data_rows)} записей")
                    
                    if target_page > 1:
                        print(f"Переход на страницу {target_page}...")
                        
                        for page in range(2, target_page + 1):
                            success = self.go_to_page_number(page)
                            if not success:
                                print(f"Не удалось перейти на страницу {page}")
                                return False
                            time.sleep(3)
                        
                        print(f"Перешли на страницу {target_page}")
                    
                    return True
                
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0 and elapsed > 0:
                    print(f"Ждем загрузки... {elapsed} сек")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"Ошибка при ожидании: {str(e)[:50]}")
                time.sleep(2)
        
        print("Таймаут ожидания загрузки таблицы")
        return False
    
    def go_to_page_number(self, page_number):
        try:
            page_selectors = [
                f'//button[text()="{page_number}"]',
                f'//a[text()="{page_number}"]',
                f'//button[contains(@class, "pagination") and text()="{page_number}"]',
                f'//a[contains(@class, "pagination") and text()="{page_number}"]'
            ]
            
            for selector in page_selectors:
                try:
                    page_button = self.driver.find_element(By.XPATH, selector)
                    if page_button.is_displayed() and page_button.is_enabled():
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", page_button)
                        time.sleep(1)
                        
                        try:
                            page_button.click()
                        except:
                            self.driver.execute_script("arguments[0].click();", page_button)
                        
                        return True
                except:
                    continue
            
            return self.click_next_button()
            
        except Exception as e:
            print(f"Ошибка перехода на страницу {page_number}: {str(e)[:50]}")
            return False
    
    def click_next_button(self):
        try:
            next_selectors = [
                '//button[contains(text(), "→")]',
                '//button[contains(text(), ">")]',
                '//a[contains(text(), "→")]',
                '//a[contains(text(), ">")]',
                '[aria-label="Next"]',
                '[aria-label="next"]',
                '.pagination-next',
                '[class*="pagination"] button:last-child'
            ]
            
            for selector in next_selectors:
                try:
                    if selector.startswith('//'):
                        next_btn = self.driver.find_element(By.XPATH, selector)
                    else:
                        next_btn = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if next_btn.is_displayed() and next_btn.is_enabled():
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                        time.sleep(1)
                        
                        try:
                            next_btn.click()
                        except:
                            self.driver.execute_script("arguments[0].click();", next_btn)
                        
                        return True
                except:
                    continue
            
            return False
            
        except:
            return False
    
    def close_modal_window(self):
        close_selectors = [
            '[class*="close"]',
            '[aria-label="close"]',
            '[aria-label="Close"]',
            'button[class*="close"]',
            'svg[class*="close"]',
            '[class*="modal"] button',
            '[role="dialog"] button'
        ]
        
        for selector in close_selectors:
            try:
                close_buttons = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for btn in close_buttons:
                    if btn.is_displayed() and btn.is_enabled():
                        btn_text = btn.text.strip().lower()
                        aria_label = btn.get_attribute('aria-label') or ''
                        
                        if (not btn_text or 
                            btn_text in ['x', '×', 'закрыть', 'close'] or
                            'close' in aria_label.lower()):
                            
                            btn.click()
                            time.sleep(0.5)
                            return True
            except:
                continue
        
        return False
    
    def extract_specializations(self, modal):
        specializations = []
        
        try:
            spec_selectors = [
                '.List_itemDescription_30s1n',
                '[class*="List_itemDescription"]',
                '.List_item_2GXxQ .List_itemDescription_30s1n',
                '[class*="List_item"] [class*="itemDescription"]',
            ]
            
            for selector in spec_selectors:
                try:
                    spec_elements = modal.find_elements(By.CSS_SELECTOR, selector)
                    if spec_elements:
                        for elem in spec_elements:
                            text = elem.text.strip()
                            if text and len(text) > 5:
                                specializations.append(text)
                        if specializations:
                            break
                except:
                    continue
            
            if not specializations:
                try:
                    spec_headers = modal.find_elements(By.XPATH, "//*[contains(text(), 'Специализации')]")
                    
                    for header in spec_headers:
                        parent = header.find_element(By.XPATH, "../..")
                        list_items = parent.find_elements(By.CSS_SELECTOR, '[class*="List_item"], li')
                        
                        for item in list_items:
                            text = item.text.strip()
                            if text and len(text) > 5 and 'специализации' not in text.lower():
                                specializations.append(text)
                        
                        if specializations:
                            break
                except:
                    pass
            
            if not specializations:
                modal_text = modal.text
                lines = modal_text.split('\n')
                in_spec_section = False
                
                for line in lines:
                    line = line.strip()
                    
                    if 'специализаци' in line.lower():
                        in_spec_section = True
                        continue
                    
                    if in_spec_section and any(x in line.lower() for x in ['статус', 'адрес', 'инн']):
                        break
                    
                    if in_spec_section and line:
                        medical_keywords = ['диагностика', 'лечение', 'терапия', 'исследование', 
                                          'консультация', 'массаж', 'узи', 'стоматология']
                        
                        if len(line) > 10 and any(kw in line.lower() for kw in medical_keywords):
                            clean_line = re.sub(r'^\d+\s*', '', line).strip()
                            if clean_line:
                                specializations.append(clean_line)
            
        except Exception as e:
            print(f"Ошибка извлечения специализаций: {str(e)[:100]}")
        
        cleaned_specs = []
        seen = set()
        
        for spec in specializations:
            spec = re.sub(r'^\d+\s*', '', spec).strip()
            spec = spec.rstrip(':').strip()
            
            if len(spec) > 5 and spec.lower() not in seen:
                cleaned_specs.append(spec)
                seen.add(spec.lower())
        
        return '; '.join(cleaned_specs) if cleaned_specs else ''
    
    def check_dental(self, modal_text, org_name="", specializations=""):
        dental_keywords = [
            'стоматолог', 'стома', 'зуб', 'dental', 'dent',
            'ортодонт', 'пародонт', 'имплант', 'протез',
            'кариес', 'пульпит', 'периодонт', 'эндодонт',
            'челюст', 'полост рта', 'зубн', 'десн', 'прикус'
        ]
        
        if org_name:
            org_lower = org_name.lower()
            if any(kw in org_lower for kw in dental_keywords[:5]):
                return True
        
        if specializations:
            spec_lower = specializations.lower()
            if any(kw in spec_lower for kw in dental_keywords):
                return True
        
        if modal_text:
            text_lower = modal_text.lower()
            if any(kw in text_lower for kw in dental_keywords):
                return True
        
        return False
    
    def extract_info(self, modal, modal_text):
        record = {}
        
        try:
            lines = modal_text.split('\n')
            
            for j in range(len(lines) - 1):
                line = lines[j].strip()
                
                if 'Статус' in line and j + 1 < len(lines):
                    record['Статус'] = lines[j + 1].strip()
                elif 'Наименование лицензиата' in line and j + 1 < len(lines):
                    record['Наименование лицензиата'] = lines[j + 1].strip()
                elif 'ИНН лицензиата' in line and j + 1 < len(lines):
                    record['ИНН'] = lines[j + 1].strip()
                elif 'Номер документа' in line and j + 1 < len(lines):
                    record['Номер документа'] = lines[j + 1].strip()
                elif 'Дата выдачи' in line and j + 1 < len(lines):
                    record['Дата выдачи'] = lines[j + 1].strip()
                elif 'Адрес деятельности' in line and j + 1 < len(lines):
                    record['Адрес деятельности'] = lines[j + 1].strip()
            
        except Exception as e:
            print(f"Ошибка извлечения данных: {str(e)[:50]}")
        
        return record
    
    def open_modal_with_retries(self, row, view_button, max_retries=2):
        
        for retry in range(max_retries):
            try:
                if retry > 0:
                    time.sleep(3)
                    
                    try:
                        rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
                        for r in rows:
                            if row.text[:30] in r.text:
                                row = r
                                cells = row.find_elements(By.TAG_NAME, 'td')
                                if cells:
                                    try:
                                        view_button = cells[-1].find_element(By.CSS_SELECTOR, 'svg, button, a')
                                    except:
                                        view_button = row
                                break
                    except:
                        pass
                
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_button)
                time.sleep(1)
                
                try:
                    view_button.click()
                except:
                    try:
                        self.driver.execute_script("arguments[0].click();", view_button)
                    except:
                        try:
                            row.click()
                        except:
                            self.driver.execute_script("arguments[0].click();", row)
                
                start_wait = time.time()
                modal = None
                
                while time.time() - start_wait < 120:
                    try:
                        modals = self.driver.find_elements(By.CSS_SELECTOR, '[class*="Details"], [class*="modal"], [role="dialog"]')
                        for m in modals:
                            if m.is_displayed():
                                modal = m
                                break
                        
                        if modal:
                            time.sleep(2)
                            if modal.is_displayed() and len(modal.text) > 100:
                                return modal
                            
                    except:
                        pass
                    
                    elapsed = int(time.time() - start_wait)
                    if elapsed > 0 and elapsed % 10 == 0:
                        print(f"...ждем {elapsed} сек...")
                        
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Попытка {retry + 1} не удалась: {str(e)[:100]}")
        
        print("Обновляем страницу...")
        try:
            self.driver.refresh()
            time.sleep(5)
            
            time.sleep(5)
            
            rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
            original_text = row.text[:50]
            
            for r in rows:
                if original_text in r.text:
                    try:
                        cells = r.find_elements(By.TAG_NAME, 'td')
                        if cells:
                            view_button = cells[-1].find_element(By.CSS_SELECTOR, 'svg, button, a')
                        else:
                            view_button = r
                        
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_button)
                        time.sleep(1)
                        
                        try:
                            view_button.click()
                        except:
                            self.driver.execute_script("arguments[0].click();", view_button)
                        
                        time.sleep(3)
                        modals = self.driver.find_elements(By.CSS_SELECTOR, '[class*="Details"], [class*="modal"], [role="dialog"]')
                        for m in modals:
                            if m.is_displayed() and len(m.text) > 100:
                                return m
                    except:
                        pass
                    break
            
        except:
            pass
        
        return None
    
    def go_to_next_page(self, target_page):
        try:
            print(f"Переход на страницу {target_page}...")
            
            self.close_modal_window()
            time.sleep(1)
            
            pagination_selectors = [
                f'//button[text()="{target_page}"]',
                f'//a[text()="{target_page}"]',
                f'[aria-label="{target_page}"]',
                '[aria-label="next"]',
                '.pagination-next',
                '[class*="pagination"] button:last-child'
            ]
            
            next_button = None
            
            for selector in pagination_selectors:
                try:
                    if selector.startswith('//'):
                        next_button = self.driver.find_element(By.XPATH, selector)
                    else:
                        next_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if next_button.is_enabled() and next_button.is_displayed():
                        break
                    else:
                        next_button = None
                except:
                    continue
            
            if not next_button:
                try:
                    pagination_containers = self.driver.find_elements(By.CSS_SELECTOR, '[class*="pagination"], [class*="Pagination"]')
                    
                    for container in pagination_containers:
                        buttons = container.find_elements(By.CSS_SELECTOR, 'button, a')
                        
                        for button in buttons:
                            button_text = button.text.strip()
                            
                            if button_text == str(target_page) and button.is_enabled():
                                next_button = button
                                break
                        
                        if next_button:
                            break
                except:
                    pass
            
            if next_button:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_button)
                time.sleep(1)
                
                try:
                    next_button.click()
                except:
                    self.driver.execute_script("arguments[0].click();", next_button)
                
                time.sleep(5)
                return True
                
        except Exception as e:
            print(f"Ошибка перехода: {str(e)[:100]}")
        
        return False
    
    def wait_for_table_and_select_language(self):
        print("Ожидаем загрузки страницы...")
        
        time.sleep(5)
        
        language_selected = self.select_russian_language()
        
        if not language_selected:
            print("ВНИМАНИЕ: Не удалось переключить на русский язык")
            print("Пробуем продолжить, но названия могут быть не на русском")
        
        max_wait = 120
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
                if not rows:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
                
                data_rows = [row for row in rows if row.text and len(row.text) > 20]
                
                if len(data_rows) > 0:
                    print(f"Таблица загружена, найдено {len(data_rows)} записей")
                    
                    if data_rows[0].text:
                        sample_text = data_rows[0].text[:200]
                        if any(eng in sample_text for eng in ['FARM', 'MEDICAL', 'CENTER', 'COMPANY']):
                            print("ВНИМАНИЕ: Данные на английском/узбекском языке!")
                            print("Пытаемся еще раз переключить язык...")
                            self.select_russian_language()
                            time.sleep(3)
                    
                    return True
                
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0 and elapsed > 0:
                    print(f"Ждем загрузки... {elapsed} сек")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"Ошибка при ожидании: {str(e)[:50]}")
                time.sleep(2)
        
        print("Таймаут ожидания загрузки таблицы")
        return False
    
    def parse_data_limited(self, start_page=1, max_pages=2):
        
        page_num = start_page
        pages_processed = 0
        
        try:
            while pages_processed < max_pages:
                print(f"\n{'='*50}")
                print(f"СТРАНИЦА {page_num} (из {max_pages} страниц)")
                print(f"{'='*50}")
                
                time.sleep(2)
                
                rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
                if not rows:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
                
                data_rows = []
                for row in rows:
                    text = row.text
                    if text and len(text) > 20:
                        data_rows.append(row)
                
                print(f"Найдено записей на странице: {len(data_rows)}")
                
                if data_rows and page_num == 1:
                    sample_text = data_rows[0].text
                    if any(eng in sample_text for eng in ['FARM', 'MEDICAL', 'CENTER']):
                        print("\nКРИТИЧЕСКАЯ ОШИБКА: Данные не на русском языке!")
                        print("Пример данных:", sample_text[:100])
                        print("\nОСТАНОВКА ПАРСИНГА!")
                        print("Перезапустите парсер, он попробует еще раз выбрать русский язык")
                        return
                
                page_dental_count = 0
                page_duplicates = 0
                
                for i, row in enumerate(data_rows):
                    try:
                        print(f"\nЗапись {i+1}/{len(data_rows)}:")
                        self.processed_count += 1
                        
                        row_text = row.text
                        
                        org_name_preview = ""
                        inn_preview = ""
                        lines = row_text.split('\n')
                        if len(lines) >= 4:
                            inn_preview = lines[2]
                            org_name_preview = lines[3]
                        
                        print(f"Организация: {org_name_preview[:50]}...")
                        
                        self.close_modal_window()
                        time.sleep(0.5)
                        
                        view_button = None
                        try:
                            cells = row.find_elements(By.TAG_NAME, 'td')
                            if cells:
                                last_cell = cells[-1]
                                view_button = last_cell.find_element(By.CSS_SELECTOR, 'svg, button, a')
                        except:
                            view_button = row
                        
                        modal = self.open_modal_with_retries(row, view_button)
                        
                        if not modal:
                            print("Пропускаем - не удалось открыть")
                            self.skipped_count += 1
                            continue
                        
                        try:
                            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", modal)
                            time.sleep(2)
                        except:
                            pass
                        
                        modal_text = modal.text
                        
                        record = self.extract_info(modal, modal_text)
                        
                        status = record.get('Статус', '').lower()
                        if 'активн' not in status and 'faol' not in status:
                            print(f"Статус не активный: {record.get('Статус', '')}")
                            self.close_modal_window()
                            continue
                        
                        specializations = self.extract_specializations(modal)
                        
                        if self.check_dental(modal_text, org_name_preview, specializations):
                            print(f"СТОМАТОЛОГИЯ найдена!")
                            
                            record['Специализации'] = specializations if specializations else 'Стоматология'
                            
                            if self.add_to_google_sheets(record):
                                page_dental_count += 1
                            else:
                                page_duplicates += 1
                        else:
                            print("НЕ стоматология")
                        
                        self.close_modal_window()
                        
                    except KeyboardInterrupt:
                        raise
                        
                    except Exception as e:
                        print(f"Ошибка обработки: {str(e)[:100]}")
                        self.skipped_count += 1
                        self.close_modal_window()
                
                print(f"\n{'='*50}")
                print(f"ИТОГИ СТРАНИЦЫ {page_num}:")
                print(f"Обработано: {len(data_rows)}")
                print(f"Новых стоматологических: {page_dental_count}")
                print(f"Пропущено дубликатов: {page_duplicates}")
                print(f"Всего стоматологических в таблице: {self.dental_count}")
                print(f"{'='*50}")
                
                pages_processed += 1
                
                if pages_processed < max_pages:
                    print(f"\nПереход на страницу {page_num + 1}...")
                    if self.go_to_next_page(page_num + 1):
                        page_num += 1
                        time.sleep(3)
                    else:
                        print("Не удалось перейти на следующую страницу")
                        break
                else:
                    print(f"\nОбработано {max_pages} страниц - завершаем парсинг")
                    break
                    
        except KeyboardInterrupt:
            print("\n\nПАРСИНГ ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ")
        
        self.print_final_stats()
    
    def print_final_stats(self):
        print("\n" + "="*60)
        print("ПАРСИНГ ЗАВЕРШЕН!")
        print("="*60)
        print(f"Всего обработано записей: {self.processed_count}")
        print(f"Найдено стоматологических: {self.dental_count}")
        print(f"Пропущено дубликатов: {self.duplicates_count}")
        print(f"Пропущено с ошибками: {self.skipped_count}")
        
        if self.processed_count > 0:
            print(f"Процент стоматологических: {self.dental_count/self.processed_count*100:.1f}%")
            success_rate = (self.processed_count - self.skipped_count) / self.processed_count * 100
            print(f"Процент успешной обработки: {success_rate:.1f}%")
        
        print("="*60)
        print(f"\nДанные сохранены в Google Sheets:")
        print(f"https://docs.google.com/spreadsheets/d/{self.google_sheet_id}")
    
    def run(self):
        print("\n" + "="*60)
        print("АВТОМАТИЧЕСКИЙ ПАРСЕР (ПЕРВЫЕ 2 СТРАНИЦЫ)")
        print("СТОМАТОЛОГИЧЕСКИХ ЛИЦЕНЗИЙ УЗБЕКИСТАНА")
        print("="*60)
        
        try:
            self.setup_google_sheets()
        except Exception as e:
            print(f"\nКритическая ошибка: {str(e)}")
            print("\nПроверьте:")
            print("1. Правильность пути к JSON файлу")
            print("2. Доступ сервисного аккаунта к таблице")
            print("3. ID таблицы Google Sheets")
            return
        
        self.setup_driver()
        
        start_page = 1
        
        print(f"\nСТАТИСТИКА:")
        print(f"Записей в таблице: {len(self.existing_records)}")
        print(f"ВСЕГДА начинаем с страницы: 1")
        print(f"Будем обрабатывать только страницы: 1 и 2")
        
        print("\nОткрываем сайт с предустановленными фильтрами...")
        print(f"URL: https://license.gov.uz/registry?filter...")
        
        self.driver.get("https://license.gov.uz/registry?filter%5Bdocument_id%5D=2908&filter%5Bdocument_type%5D=LICENSE")
        
        print("\nАвтоматическая обработка запущена...")
        print("Выбор русского языка")
        print("Ожидание загрузки данных")
        print("Фильтры УЖЕ применены в URL:")
        print("- Тип документа: Лицензия")
        print("- Услуга: Лицензия на медицинскую деятельность (ID: 2908)")
        
        if not self.wait_for_table_and_select_language():
            print("\nНе удалось загрузить данные или выбрать русский язык")
            print("Браузер закроется через 5 секунд...")
            time.sleep(5)
            self.driver.quit()
            return
        
        print("\nВСЕ ГОТОВО К ПАРСИНГУ!")
        print("Запуск автоматического парсинга первых 2 страниц...")
        print("\nДля остановки нажмите: Ctrl+C")
        print("="*60)
        
        time.sleep(3)
        
        self.parse_data_limited(start_page=1, max_pages=2)
        
        print("\nРабота завершена!")
        print("Браузер закроется через 5 секунд...")
        time.sleep(5)
        self.driver.quit()

if __name__ == "__main__":
    parser = GoogleSheetsParser()
    try:
        parser.run()
    except KeyboardInterrupt:
        print("\n\nПРОГРАММА ОСТАНОВЛЕНА")
    except Exception as e:
        print(f"\n\nОШИБКА: {str(e)}")
        print("Браузер закроется через 5 секунд...")
        time.sleep(5)