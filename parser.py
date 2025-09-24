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
        
        # Путь к вашему JSON файлу с credentials
        self.credentials_file = r"C:\Users\Vasiliy\Desktop\Парсер_лицензий\stomatologyscraper-7f64e5b6d7b7.json"
        
        # URL с нужными фильтрами - ПРАВИЛЬНЫЙ URL!
        self.target_url = "https://license.gov.uz/registry?filter%5Bdocument_id%5D=2908&filter%5Bdocument_type%5D=LICENSE"
        
        # Кэш существующих записей для проверки дубликатов
        self.existing_records = set()
    
    def setup_google_sheets(self):
        """Настройка подключения к Google Sheets"""
        print("Подключение к Google Sheets...")
        
        try:
            # Проверяем наличие файла
            if not os.path.exists(self.credentials_file):
                print(f"❌ Файл не найден: {self.credentials_file}")
                raise Exception("Файл credentials не найден")
            
            print(f"✓ Найден файл credentials: {os.path.basename(self.credentials_file)}")
            
            # Создаем credentials из JSON файла
            credentials = service_account.Credentials.from_service_account_file(
                self.credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets',
                        'https://www.googleapis.com/auth/drive']
            )
            
            # Подключаемся к Google Sheets
            gc = gspread.authorize(credentials)
            
            # Открываем таблицу
            spreadsheet = gc.open_by_key(self.google_sheet_id)
            
            # Получаем первый лист
            self.worksheet = spreadsheet.get_worksheet(0)
            
            print(f"✓ Подключено к Google Sheets")
            print(f"  Таблица: {spreadsheet.title}")
            print(f"  Лист: {self.worksheet.title}")
            print(f"  Ссылка: https://docs.google.com/spreadsheets/d/{self.google_sheet_id}")
            
            # Проверяем заголовки
            headers = self.worksheet.row_values(1)
            if not headers or 'RegNumber_label' not in headers:
                # Создаем заголовки если их нет или они неправильные
                new_headers = [
                    'RegNumber_label',
                    'Дата',
                    'ИНН',
                    'Флаг Сети',
                    'Название',
                    'Дата окончания лицензии',  # ИЗМЕНЕНО с "Статус организации"!
                    'Адрес',
                    'Специализации в лицензии',
                    'ВидДеятельности'
                ]
                self.worksheet.update('A1:I1', [new_headers])
                print("  ✓ Заголовки созданы/обновлены")
            else:
                print(f"  ✓ Заголовки существуют")
                # Проверяем и обновляем заголовок столбца F если нужно
                if len(headers) > 5 and headers[5] == 'Статус организации':
                    print("  📝 Обновляем заголовок столбца F на 'Дата окончания лицензии'...")
                    self.worksheet.update('F1', 'Дата окончания лицензии')
                    print("  ✓ Заголовок обновлен")
            
            # Загружаем существующие записи для проверки дубликатов
            self.load_existing_records()
                
        except Exception as e:
            print(f"❌ Ошибка подключения к Google Sheets: {str(e)}")
            raise
    
    def load_existing_records(self):
        """Загрузка существующих записей для проверки дубликатов"""
        try:
            print("  Загружаем существующие записи для проверки дубликатов...")
            
            # Получаем все данные из таблицы
            all_values = self.worksheet.get_all_values()
            
            if len(all_values) <= 1:  # Только заголовки или пустая таблица
                print("  ✓ Таблица пустая или содержит только заголовки")
                return
            
            # Пропускаем первую строку (заголовки)
            for row in all_values[1:]:
                if len(row) >= 3:  # Минимум должно быть 3 колонки
                    license_num = str(row[0]).strip()  # RegNumber_label - первая колонка
                    inn = str(row[2]).strip()  # ИНН - третья колонка
                    
                    if inn and license_num and inn != '' and license_num != '':
                        unique_key = f"{inn}_{license_num}"
                        self.existing_records.add(unique_key)
                        
            print(f"  ✓ Загружено {len(self.existing_records)} существующих записей")
            
            # Показываем примеры загруженных ключей для отладки
            if self.existing_records:
                sample_keys = list(self.existing_records)[:3]
                print(f"  Примеры ключей: {sample_keys}")
            
        except Exception as e:
            print(f"  ⚠ Не удалось загрузить существующие записи: {str(e)}")
            print("  Продолжаем работу без проверки дубликатов")
    
    def check_duplicate(self, inn, license_num):
        """Проверка на дубликат записи"""
        if not inn or not license_num:
            return False
            
        unique_key = f"{inn}_{license_num}"
        is_duplicate = unique_key in self.existing_records
        
        if is_duplicate:
            print(f"    🔍 Проверка: ИНН={inn}, Лицензия={license_num} - ДУБЛИКАТ")
        else:
            print(f"    🔍 Проверка: ИНН={inn}, Лицензия={license_num} - новая запись")
            
        return is_duplicate
    
    def setup_driver(self):
        """Настройка браузера"""
        print("Запуск браузера...")
        
        self.driver = Driver(
            browser="chrome",
            uc=True,
            headless=False,
            locale_code="ru"
        )
        
        print("✓ Браузер запущен")
    
    def get_last_processed_page(self):
        """Получает номер последней обработанной страницы"""
        try:
            if os.path.exists(self.last_page_file):
                with open(self.last_page_file, 'r') as f:
                    return int(f.read().strip())
        except:
            pass
        return 0
    
    def save_last_processed_page(self, page_num):
        """Сохраняет номер последней обработанной страницы"""
        try:
            os.makedirs(os.path.dirname(self.last_page_file), exist_ok=True)
            with open(self.last_page_file, 'w') as f:
                f.write(str(page_num))
        except:
            pass
    
    def add_to_google_sheets(self, record):
        """Добавление записи в Google Sheets с проверкой дубликатов"""
        try:
            # Извлекаем данные для проверки
            inn = str(record.get('ИНН', '')).strip()
            license_num = str(record.get('Номер документа', '')).strip()
            
            # ОБЯЗАТЕЛЬНАЯ проверка на дубликат
            if inn and license_num:
                if self.check_duplicate(inn, license_num):
                    self.duplicates_count += 1
                    return False  # Дубликат - НЕ добавляем
            
            # Формируем строку данных в соответствии со структурой таблицы
            row_data = [
                license_num,                                # RegNumber_label (колонка A)
                record.get('Дата выдачи', ''),             # Дата (колонка B)
                inn,                                        # ИНН (колонка C)
                '',                                         # Флаг Сети (колонка D - пустое)
                record.get('Наименование лицензиата', '').replace('"', '').strip(), # Название (колонка E)
                record.get('Дата окончания', ''),          # Дата окончания лицензии (колонка F) - ИЗМЕНЕНО!
                record.get('Адрес деятельности', ''),      # Адрес (колонка G)
                record.get('Специализации', ''),           # Специализации в лицензии (колонка H)
                ''                                          # ВидДеятельности (колонка I - пустое)
            ]
            
            # Добавляем строку в таблицу
            self.worksheet.append_row(row_data, value_input_option='USER_ENTERED')
            
            # ВАЖНО: Добавляем в кэш существующих записей сразу после сохранения
            if inn and license_num:
                unique_key = f"{inn}_{license_num}"
                self.existing_records.add(unique_key)
                print(f"    ✅ НОВАЯ запись сохранена в Google Sheets")
                if record.get('Дата окончания'):
                    print(f"    📅 Дата окончания: {record.get('Дата окончания')}")
            
            self.dental_count += 1
            print(f"    📊 Всего стоматологических в таблице: {self.dental_count}")
            return True
            
        except Exception as e:
            print(f"    ❌ Ошибка сохранения: {str(e)[:100]}")
            
            # Пробуем переподключиться и сохранить снова
            try:
                time.sleep(2)
                print("    Переподключаемся к Google Sheets...")
                self.setup_google_sheets()
                
                # После переподключения пробуем сохранить снова
                self.worksheet.append_row(row_data, value_input_option='USER_ENTERED')
                
                # Добавляем в кэш
                if inn and license_num:
                    unique_key = f"{inn}_{license_num}"
                    self.existing_records.add(unique_key)
                
                self.dental_count += 1
                print(f"    ✅ Сохранено после переподключения")
                return True
            except:
                print(f"    ❌ Не удалось сохранить даже после переподключения")
                return False
    
    def select_russian_language(self):
        """Жёстко включаем RU через все возможные методы."""
        print("\n🌐 ПОПЫТКА ПЕРЕКЛЮЧЕНИЯ НА РУССКИЙ ЯЗЫК...")
        
        try:
            # Шаг 1: Проверяем текущее состояние localStorage
            print("  📍 Шаг 1: Проверка текущего языка...")
            current_lang = self.driver.execute_script("return localStorage.getItem('i18nextLng')")
            print(f"    Текущий язык в localStorage: {current_lang}")
            
            # Шаг 2: Принудительная установка через localStorage и cookies
            print("  📍 Шаг 2: Установка языка через localStorage и cookies...")
            self.driver.execute_script("""
                // Устанавливаем все возможные ключи языка
                localStorage.setItem('i18nextLng', 'ru');
                localStorage.setItem('i18nextLng', 'ru-RU');
                localStorage.setItem('lang', 'ru');
                localStorage.setItem('language', 'ru');
                localStorage.setItem('locale', 'ru');
                localStorage.setItem('userLanguage', 'ru');
                
                // Устанавливаем cookies
                document.cookie = 'i18nextLng=ru; path=/; domain=.license.gov.uz';
                document.cookie = 'lang=ru; path=/; domain=.license.gov.uz';
                document.cookie = 'language=ru; path=/; domain=.license.gov.uz';
                
                console.log('Язык установлен в localStorage и cookies');
            """)
            
            # Проверяем что установилось
            new_lang = self.driver.execute_script("return localStorage.getItem('i18nextLng')")
            print(f"    Новый язык в localStorage: {new_lang}")
            
            # Шаг 3: Обновляем страницу для применения
            print("  📍 Шаг 3: Обновление страницы...")
            self.driver.refresh()
            time.sleep(5)  # Даём время на загрузку
            
            # Шаг 4: Проверяем результат
            print("  📍 Шаг 4: Проверка результата...")
            if self.check_interface_language():
                print("  ✅ Русский язык успешно установлен через localStorage!")
                return True
            
            # Шаг 5: Если не помогло - пробуем через клик по кнопке
            print("  📍 Шаг 5: Попытка через клик по переключателю языка...")
            
            # Сначала ищем видимый переключатель языка
            language_buttons = self.driver.find_elements(By.CSS_SELECTOR, """
                [class*='anguage'], [class*='lang'], 
                button[aria-label*='language' i], 
                button[aria-label*='язык' i],
                div[class*='Switcher'], 
                button:has-text('UZ'), button:has-text('RU'), button:has-text('EN')
            """)
            
            print(f"    Найдено элементов переключателя: {len(language_buttons)}")
            
            # Пробуем найти и кликнуть по переключателю
            for btn in language_buttons:
                try:
                    if btn.is_displayed():
                        btn_text = btn.text
                        print(f"    Найдена кнопка: '{btn_text}'")
                        
                        # Если это уже RU - пробуем кликнуть для обновления
                        if 'RU' in btn_text or 'РУ' in btn_text:
                            print("      Кликаем по RU...")
                            self.driver.execute_script("arguments[0].click();", btn)
                            time.sleep(2)
                            
                        # Если это другой язык - открываем меню
                        elif any(lang in btn_text for lang in ['UZ', 'EN', 'OZ', "O'Z"]):
                            print("      Открываем меню языков...")
                            self.driver.execute_script("arguments[0].click();", btn)
                            time.sleep(1)
                            
                            # Ищем RU в выпадающем списке
                            ru_options = self.driver.find_elements(By.XPATH, """
                                //*[normalize-space(text())='RU' or 
                                    normalize-space(text())='РУ' or 
                                    normalize-space(text())='Русский' or
                                    normalize-space(text())='Russian']
                            """)
                            
                            for ru_opt in ru_options:
                                if ru_opt.is_displayed():
                                    print("      Найден пункт RU, кликаем...")
                                    self.driver.execute_script("arguments[0].click();", ru_opt)
                                    time.sleep(3)
                                    break
                            break
                except Exception as e:
                    print(f"    Ошибка при клике: {str(e)[:50]}")
                    continue
            
            # Шаг 6: Последняя проверка
            time.sleep(3)
            print("  📍 Шаг 6: Финальная проверка...")
            
            # Пробуем найти русские слова на странице
            russian_elements = self.driver.find_elements(By.XPATH, """
                //*[contains(text(),'Поиск') or 
                    contains(text(),'Реестр') or 
                    contains(text(),'Лицензия') or
                    contains(text(),'Фильтр') or
                    contains(text(),'Медицина')]
            """)
            
            if russian_elements:
                print(f"  ✅ Найдено {len(russian_elements)} русских элементов на странице")
                return True
            else:
                print("  ⚠️ ВНИМАНИЕ: Не удалось переключить на русский язык!")
                print("     Продолжаем работу с текущим языком интерфейса")
                return False
                
        except Exception as e:
            print(f"  ❌ Критическая ошибка при переключении языка: {str(e)}")
            print("     Продолжаем работу с текущим языком")
            return False
    
    def check_interface_language(self):
        """Проверка языка интерфейса (не названий компаний!)"""
        try:
            # Метод 1: Проверяем заголовки страницы
            page_text = self.driver.find_element(By.TAG_NAME, 'body').text[:1000]  # Первые 1000 символов
            
            # Ключевые русские слова интерфейса
            russian_interface_words = [
                'Поиск', 'Реестр', 'Фильтр', 'Лицензия',
                'Медицина', 'Тип документа', 'Услуга',
                'медицинская деятельность', 'Показать'
            ]
            
            # Ключевые узбекские слова
            uzbek_interface_words = [
                'Qidiruv', 'Reestr', 'Filter', 'Litsenziya',
                'Tibbiyot', 'Hujjat turi', 'Xizmat',
                'tibbiy faoliyat', "Ko'rsatish"
            ]
            
            russian_count = sum(1 for word in russian_interface_words if word in page_text)
            uzbek_count = sum(1 for word in uzbek_interface_words if word in page_text)
            
            print(f"    Найдено русских слов: {russian_count}, узбекских: {uzbek_count}")
            
            if russian_count > uzbek_count:
                return True
            elif uzbek_count > russian_count:
                return False
            
            # Метод 2: Проверяем кнопки и меню
            try:
                buttons = self.driver.find_elements(By.CSS_SELECTOR, 'button, a[role="button"]')
                button_texts = [btn.text for btn in buttons[:10]]  # Первые 10 кнопок
                
                # Проверяем тексты кнопок
                russian_button_words = ['Поиск', 'Фильтр', 'Очистить', 'Показать', 'Экспорт']
                for text in button_texts:
                    if any(word in text for word in russian_button_words):
                        print(f"    Найдена русская кнопка: {text}")
                        return True
            except:
                pass
            
            # Метод 3: Проверяем таблицу если она есть
            try:
                first_row = self.driver.find_element(By.CSS_SELECTOR, 'tbody tr')
                row_text = first_row.text
                
                # Проверяем системные слова в таблице
                if 'Медицина' in row_text or 'Лицензия' in row_text:
                    return True
                elif 'Tibbiyot' in row_text or 'Litsenziya' in row_text:
                    return False
            except:
                pass
            
            # Если ничего не нашли - считаем что не русский
            print("    ⚠️ Не удалось определить язык интерфейса")
            return False
                
        except Exception as e:
            print(f"    ❌ Ошибка проверки языка: {str(e)[:100]}")
            return False
    
    def wait_for_table_and_navigate(self, target_page):
        """Ожидание загрузки таблицы и переход на нужную страницу"""
        print("Ожидаем загрузки страницы с фильтрами...")
        
        # Сначала пробуем выбрать русский язык
        self.select_russian_language()
        
        # Ждем появления таблицы
        max_wait = 180  # 3 минуты максимум
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                # Проверяем наличие таблицы
                rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
                if not rows:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
                
                # Фильтруем строки с данными
                data_rows = [row for row in rows if row.text and ('Медицина' in row.text or 'Лицензия' in row.text)]
                
                if len(data_rows) > 0:
                    print(f"✓ Таблица загружена, найдено {len(data_rows)} записей")
                    
                    # Если нужно перейти на другую страницу
                    if target_page > 1:
                        print(f"Переход на страницу {target_page}...")
                        
                        # Ищем пагинацию
                        for page in range(2, target_page + 1):
                            success = self.go_to_page_number(page)
                            if not success:
                                print(f"⚠ Не удалось перейти на страницу {page}")
                                return False
                            time.sleep(3)  # Ждем загрузки страницы
                        
                        print(f"✓ Перешли на страницу {target_page}")
                    
                    return True
                
                # Показываем прогресс
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0 and elapsed > 0:
                    print(f"  Ждем загрузки... {elapsed} сек")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"  Ошибка при ожидании: {str(e)[:50]}")
                time.sleep(2)
        
        print("⚠ Таймаут ожидания загрузки таблицы")
        return False
    
    def go_to_page_number(self, page_number):
        """Переход на конкретную страницу по номеру"""
        try:
            # Ищем кнопку с номером страницы
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
            
            # Если не нашли прямую кнопку, пробуем кнопку "Далее"
            return self.click_next_button()
            
        except Exception as e:
            print(f"  Ошибка перехода на страницу {page_number}: {str(e)[:50]}")
            return False
    
    def click_next_button(self):
        """Клик на кнопку 'Следующая страница'"""
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
        """Закрытие модального окна"""
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
        """Извлечение специализаций из модального окна"""
        specializations = []
        
        try:
            # Поиск специализаций по CSS селекторам
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
            
            # Поиск через заголовок "Специализации"
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
            
            # Поиск по тексту модального окна
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
            print(f"    Ошибка извлечения специализаций: {str(e)[:100]}")
        
        # Очистка и форматирование
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
        """Проверка на стоматологию"""
        # Ключевые слова для поиска стоматологии
        dental_keywords = [
            'стоматолог', 'стома', 'зуб', 'dental', 'dent',
            'ортодонт', 'пародонт', 'имплант', 'протез',
            'кариес', 'пульпит', 'периодонт', 'эндодонт',
            'челюст', 'полост рта', 'зубн', 'десн', 'прикус'
        ]
        
        # Проверяем название организации
        if org_name:
            org_lower = org_name.lower()
            if any(kw in org_lower for kw in dental_keywords[:5]):  # Основные термины
                return True
        
        # Проверяем специализации
        if specializations:
            spec_lower = specializations.lower()
            if any(kw in spec_lower for kw in dental_keywords):
                return True
        
        # Проверяем весь текст модального окна
        if modal_text:
            text_lower = modal_text.lower()
            if any(kw in text_lower for kw in dental_keywords):
                return True
        
        return False
    
    def extract_info(self, modal, modal_text):
        """Извлечение информации из модального окна"""
        record = {}
        
        try:
            lines = modal_text.split('\n')
            
            for j in range(len(lines) - 1):
                line = lines[j].strip()
                line_lower = line.lower()
                
                # Статус - проверяем разные варианты
                if any(status_word in line_lower for status_word in ['статус', 'status', 'holat']):
                    next_line = lines[j + 1].strip()
                    record['Статус'] = next_line
                    # Если статус пустой, пробуем взять через 2 строки
                    if not next_line and j + 2 < len(lines):
                        record['Статус'] = lines[j + 2].strip()
                        
                # ДАТА ОКОНЧАНИЯ - НОВОЕ ПОЛЕ! Проверяем разные варианты
                elif any(end_word in line_lower for end_word in ['дата окончания', 'срок действия', 'действителен до', 
                                                                   'amal qilish muddati', 'tugash sanasi', 'expiry date', 
                                                                   'valid until', 'амал қилиш муддати']):
                    next_line = lines[j + 1].strip()
                    if next_line:
                        record['Дата окончания'] = next_line
                        print(f"    📅 Найдена дата окончания: {next_line}")
                    # Если дата пустая, пробуем взять через 2 строки
                    elif j + 2 < len(lines):
                        record['Дата окончания'] = lines[j + 2].strip()
                        
                # Наименование - проверяем разные варианты
                elif any(name_word in line_lower for name_word in ['наименование', 'номи', 'nomi', 'name']):
                    if 'лицензиата' in line_lower or 'litsenziat' in line_lower:
                        record['Наименование лицензиата'] = lines[j + 1].strip()
                        
                # ИНН - универсально на всех языках
                elif 'инн' in line_lower or 'inn' in line_lower or 'stir' in line_lower:
                    if 'лицензиата' in line_lower or 'litsenziat' in line_lower or j == 0:
                        next_line = lines[j + 1].strip()
                        # Проверяем что это действительно ИНН (9 цифр)
                        if next_line and (len(next_line) == 9 or len(next_line.replace(' ', '')) == 9):
                            record['ИНН'] = next_line.replace(' ', '')
                            
                # Номер документа
                elif any(doc_word in line_lower for doc_word in ['номер документа', 'hujjat raqami', 'document number']):
                    record['Номер документа'] = lines[j + 1].strip()
                elif 'рақами' in line_lower and 'ҳужжат' in line_lower:
                    record['Номер документа'] = lines[j + 1].strip()
                    
                # Дата выдачи
                elif any(date_word in line_lower for date_word in ['дата выдачи', 'berilgan sana', 'issue date']):
                    record['Дата выдачи'] = lines[j + 1].strip()
                elif 'берилган' in line_lower and 'сана' in line_lower:
                    record['Дата выдачи'] = lines[j + 1].strip()
                    
                # Адрес деятельности  
                elif any(addr_word in line_lower for addr_word in ['адрес деятельности', 'faoliyat manzili', 'activity address']):
                    record['Адрес деятельности'] = lines[j + 1].strip()
                elif 'фаолият' in line_lower and 'манзили' in line_lower:
                    record['Адрес деятельности'] = lines[j + 1].strip()
            
            # Если статус не найден, ищем по шаблону (обычно "Активный" или "Faol")
            if 'Статус' not in record or not record['Статус']:
                for line in lines:
                    line = line.strip()
                    if line in ['Активный', 'Активная', 'Active', 'Faol', 'Фаол']:
                        record['Статус'] = line
                        break
                        
            # Если все еще нет статуса, считаем активным по умолчанию для медицинских лицензий
            if 'Статус' not in record or not record['Статус']:
                print("    ⚠ Статус не найден, проверяем по другим признакам...")
                # Если есть ИНН и номер документа, скорее всего лицензия активна
                if record.get('ИНН') and record.get('Номер документа'):
                    record['Статус'] = 'Активный'
                    print("    ✓ Предполагаем активный статус (есть ИНН и номер)")
            
            # Если дата окончания не найдена, пробуем найти в тексте по шаблону даты
            if 'Дата окончания' not in record:
                import re
                # Ищем даты в формате DD.MM.YYYY или YYYY-MM-DD после слов о сроке
                for i, line in enumerate(lines):
                    if any(word in line.lower() for word in ['до', 'until', 'gacha', 'гача']):
                        # Проверяем следующие строки на наличие даты
                        for k in range(i, min(i+3, len(lines))):
                            date_match = re.search(r'\d{2}\.\d{2}\.\d{4}|\d{4}-\d{2}-\d{2}', lines[k])
                            if date_match:
                                record['Дата окончания'] = date_match.group()
                                print(f"    📅 Найдена дата окончания (по шаблону): {date_match.group()}")
                                break
                    
        except Exception as e:
            print(f"    Ошибка извлечения данных: {str(e)[:50]}")
        
        # Выводим найденные данные для отладки
        if record:
            print(f"    Извлечено: Статус={record.get('Статус', 'НЕ НАЙДЕН')}, Дата окончания={record.get('Дата окончания', 'НЕ НАЙДЕНА')}, ИНН={record.get('ИНН', 'НЕТ')}")
        
        return record
    
    def open_modal_with_retries(self, row, view_button, max_retries=2):
        """Открытие модального окна с повторными попытками"""
        
        for retry in range(max_retries):
            try:
                if retry > 0:
                    time.sleep(3)
                    
                    # Обновляем ссылки на элементы
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
                
                # Скроллим к элементу
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", view_button)
                time.sleep(1)
                
                # Пробуем кликнуть
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
                
                # Ждем появления модального окна
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
                        print(f"     ...ждем {elapsed} сек...")
                        
                    time.sleep(1)
                    
            except Exception as e:
                print(f"   Попытка {retry + 1} не удалась: {str(e)[:100]}")
        
        # Последняя попытка после обновления страницы
        print("   Обновляем страницу...")
        try:
            self.driver.refresh()
            time.sleep(5)
            
            # Ждем загрузки таблицы
            time.sleep(5)
            
            # Находим строку заново и пробуем открыть
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
                        
                        # Ждем модальное окно
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
        """Переход на следующую страницу"""
        try:
            print(f"  Переход на страницу {target_page}...")
            
            self.close_modal_window()
            time.sleep(1)
            
            # Поиск кнопки пагинации
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
            
            # Поиск в контейнере пагинации
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
            print(f"  Ошибка перехода: {str(e)[:100]}")
        
        return False
    
    def print_final_stats(self):
        """Вывод финальной статистики"""
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
        """Основной метод запуска парсера - ВСЕГДА С 1 СТРАНИЦЫ, ТОЛЬКО 2 СТРАНИЦЫ"""
        print("\n" + "="*60)
        print("🤖 АВТОМАТИЧЕСКИЙ ПАРСЕР (ПЕРВЫЕ 2 СТРАНИЦЫ)")
        print("СТОМАТОЛОГИЧЕСКИХ ЛИЦЕНЗИЙ УЗБЕКИСТАНА")
        print("="*60)
        
        # Настройка Google Sheets
        try:
            self.setup_google_sheets()
        except Exception as e:
            print(f"\n❌ Критическая ошибка: {str(e)}")
            print("\nПроверьте:")
            print("1. Правильность пути к JSON файлу")
            print("2. Доступ сервисного аккаунта к таблице")
            print("3. ID таблицы Google Sheets")
            return
        
        # Настройка браузера
        self.setup_driver()
        
        # ВСЕГДА начинаем с 1 страницы!
        start_page = 1
        
        print(f"\n📊 СТАТИСТИКА:")
        print(f"  • Записей в таблице: {len(self.existing_records)}")
        print(f"  • ВСЕГДА начинаем с страницы: 1")
        print(f"  • Будем обрабатывать только страницы: 1 и 2")
        
        # Попытка 1: Пробуем открыть с параметром языка в URL
        print("\n🌐 Открываем сайт с предустановленными фильтрами...")
        print("  📍 Попытка 1: URL с параметром языка...")
        
        # Пробуем URL с явным указанием языка (если поддерживается)
        urls_to_try = [
            "https://license.gov.uz/registry?lang=ru&filter%5Bdocument_id%5D=2908&filter%5Bdocument_type%5D=LICENSE",
            "https://license.gov.uz/ru/registry?filter%5Bdocument_id%5D=2908&filter%5Bdocument_type%5D=LICENSE",
            "https://license.gov.uz/registry?filter%5Bdocument_id%5D=2908&filter%5Bdocument_type%5D=LICENSE&locale=ru",
            "https://license.gov.uz/registry?filter%5Bdocument_id%5D=2908&filter%5Bdocument_type%5D=LICENSE"
        ]
        
        for url in urls_to_try:
            print(f"    Пробуем: {url[:50]}...")
            self.driver.get(url)
            time.sleep(3)
            
            # Проверяем загрузилась ли страница
            if self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr, [class*="Table"]'):
                print("    ✓ Страница загружена")
                break
        
        print("\n⏳ Настройка языка и ожидание загрузки...")
        
        # Сначала устанавливаем язык через localStorage ДО основной загрузки
        print("  📍 Предварительная установка языка...")
        self.driver.execute_script("""
            localStorage.setItem('i18nextLng', 'ru');
            localStorage.setItem('lang', 'ru');
            localStorage.setItem('language', 'ru');
            document.cookie = 'i18nextLng=ru; path=/';
            document.cookie = 'lang=ru; path=/';
        """)
        
        # Обновляем страницу чтобы применился язык
        print("  📍 Обновление страницы для применения языка...")
        self.driver.refresh()
        time.sleep(5)
        
        # Теперь пробуем переключить язык через интерфейс
        self.select_russian_language()
        
        print("\n📋 Информация о странице:")
        print(f"  • URL: {self.driver.current_url[:80]}...")
        print(f"  • Заголовок: {self.driver.title}")
        
        # Ждем загрузки страницы и проверяем язык
        if not self.wait_for_table_and_select_language():
            print("\n❌ Не удалось загрузить данные")
            
            # Последняя попытка - продолжаем с текущим языком
            print("\n⚠️ ВНИМАНИЕ: Работаем с узбекским/английским интерфейсом")
            print("   Парсер адаптирован для работы с любым языком")
            
            # Проверяем есть ли таблица
            if not self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr'):
                print("\n❌ Таблица не найдена. Завершаем работу.")
                time.sleep(5)
                self.driver.quit()
                return
        
        print("\n✅ ГОТОВО К ПАРСИНГУ!")
        print("🚀 Запуск автоматического парсинга первых 2 страниц...")
        print("\n⚠️ Для остановки нажмите: Ctrl+C")
        print("="*60)
        
        # Небольшая пауза перед началом
        time.sleep(3)
        
        # Запускаем парсинг ТОЛЬКО 2 СТРАНИЦ
        self.parse_data_limited(start_page=1, max_pages=2)
        
        print("\n🏁 Работа завершена!")
        print("Браузер закроется через 5 секунд...")
        time.sleep(5)
        self.driver.quit()
    
    def wait_for_table_and_select_language(self):
        """Ожидание загрузки таблицы и обязательный выбор русского языка"""
        print("Ожидаем загрузки страницы...")
        
        # Ждем начальной загрузки
        time.sleep(5)
        
        # ОБЯЗАТЕЛЬНО выбираем русский язык
        language_selected = self.select_russian_language()
        
        if not language_selected:
            print("⚠ ВНИМАНИЕ: Не удалось переключить на русский язык")
            print("  Продолжаем работу - названия компаний могут быть на любом языке")
        
        # Ждем загрузки таблицы после смены языка
        max_wait = 120
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                # Проверяем наличие таблицы
                rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
                if not rows:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
                
                # Фильтруем строки с данными
                data_rows = [row for row in rows if row.text and len(row.text) > 20]
                
                if len(data_rows) > 0:
                    print(f"✓ Таблица загружена, найдено {len(data_rows)} записей")
                    
                    # Проверяем ИНТЕРФЕЙС, а не названия компаний
                    if not self.check_interface_language():
                        print("⚠ Интерфейс не на русском, пытаемся переключить еще раз...")
                        self.select_russian_language()
                        time.sleep(3)
                    
                    return True
                
                # Показываем прогресс
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0 and elapsed > 0:
                    print(f"  Ждем загрузки... {elapsed} сек")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"  Ошибка при ожидании: {str(e)[:50]}")
                time.sleep(2)
        
        print("⚠ Таймаут ожидания загрузки таблицы")
        return False
    
    def parse_data_limited(self, start_page=1, max_pages=2):
        """Парсинг данных - ТОЛЬКО УКАЗАННОЕ КОЛИЧЕСТВО СТРАНИЦ"""
        
        page_num = start_page
        pages_processed = 0
        
        try:
            while pages_processed < max_pages:
                print(f"\n{'='*50}")
                print(f"СТРАНИЦА {page_num} (из {max_pages} страниц)")
                print(f"{'='*50}")
                
                # Ждем стабилизации таблицы после перехода
                time.sleep(2)
                
                # Получаем строки таблицы
                rows = self.driver.find_elements(By.CSS_SELECTOR, 'tr.Table_row__329lz, tr[class*="Table_row"]')
                if not rows:
                    rows = self.driver.find_elements(By.CSS_SELECTOR, 'tbody tr')
                
                # Фильтруем строки с данными
                data_rows = []
                for row in rows:
                    text = row.text
                    if text and len(text) > 20:  # Минимальная длина для валидной строки
                        data_rows.append(row)
                
                print(f"Найдено записей на странице: {len(data_rows)}")
                
                # НЕ проверяем названия компаний - они могут быть на любом языке!
                # Проверяем только интерфейс при необходимости
                
                page_dental_count = 0
                page_duplicates = 0
                
                # Обрабатываем каждую строку
                for i, row in enumerate(data_rows):
                    try:
                        print(f"\nЗапись {i+1}/{len(data_rows)}:")
                        self.processed_count += 1
                        
                        row_text = row.text
                        
                        # Извлекаем предварительные данные
                        org_name_preview = ""
                        inn_preview = ""
                        lines = row_text.split('\n')
                        if len(lines) >= 4:
                            inn_preview = lines[2]  # ИНН обычно на 3-й строке
                            org_name_preview = lines[3]  # Название на 4-й
                        
                        print(f"  Организация: {org_name_preview[:50]}...")
                        
                        # Закрываем открытые модальные окна
                        self.close_modal_window()
                        time.sleep(0.5)
                        
                        # Находим кнопку просмотра
                        view_button = None
                        try:
                            cells = row.find_elements(By.TAG_NAME, 'td')
                            if cells:
                                last_cell = cells[-1]
                                view_button = last_cell.find_element(By.CSS_SELECTOR, 'svg, button, a')
                        except:
                            view_button = row
                        
                        # Открываем модальное окно
                        modal = self.open_modal_with_retries(row, view_button)
                        
                        if not modal:
                            print("  ⚠ Пропускаем - не удалось открыть")
                            self.skipped_count += 1
                            continue
                        
                        # Прокручиваем модальное окно для загрузки контента
                        try:
                            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", modal)
                            time.sleep(2)
                        except:
                            pass
                        
                        # Получаем текст модального окна
                        modal_text = modal.text
                        
                        # Извлекаем информацию
                        record = self.extract_info(modal, modal_text)
                        
                        # Проверяем статус - обрабатываем только активные
                        status = record.get('Статус', '').lower()
                        
                        # Расширенная проверка активности на разных языках
                        inactive_keywords = ['неактив', 'inactive', 'nofaol', 'bekor', 'бекор', 'отменен', 'cancelled']
                        active_keywords = ['актив', 'active', 'faol', 'фаол']
                        
                        is_inactive = any(keyword in status for keyword in inactive_keywords)
                        is_active = any(keyword in status for keyword in active_keywords)
                        
                        # Если статус явно неактивный, пропускаем
                        if is_inactive and not is_active:
                            print(f"  ⚠ Статус не активный: {record.get('Статус', 'ПУСТОЙ')}")
                            self.close_modal_window()
                            continue
                            
                        # Если статус не определен, но есть основные данные - обрабатываем
                        if not status and not (record.get('ИНН') and record.get('Номер документа')):
                            print(f"  ⚠ Недостаточно данных для обработки")
                            self.close_modal_window()
                            continue
                        
                        # Извлекаем специализации
                        specializations = self.extract_specializations(modal)
                        
                        # Проверяем на стоматологию
                        if self.check_dental(modal_text, org_name_preview, specializations):
                            print(f"  ✓ СТОМАТОЛОГИЯ найдена!")
                            
                            record['Специализации'] = specializations if specializations else 'Стоматология'
                            
                            # Убеждаемся что дата окончания есть в записи
                            if not record.get('Дата окончания'):
                                print("    ⚠ Дата окончания не найдена в записи")
                            
                            # Сохраняем в Google Sheets (с проверкой дубликатов)
                            if self.add_to_google_sheets(record):
                                page_dental_count += 1
                            else:
                                page_duplicates += 1
                        else:
                            print("  ❌ НЕ стоматология")
                        
                        # Закрываем модальное окно
                        self.close_modal_window()
                        
                    except KeyboardInterrupt:
                        raise
                        
                    except Exception as e:
                        print(f"  ❌ Ошибка обработки: {str(e)[:100]}")
                        self.skipped_count += 1
                        self.close_modal_window()
                
                # Статистика страницы
                print(f"\n{'='*50}")
                print(f"📊 ИТОГИ СТРАНИЦЫ {page_num}:")
                print(f"  • Обработано: {len(data_rows)}")
                print(f"  • Новых стоматологических: {page_dental_count}")
                print(f"  • Пропущено дубликатов: {page_duplicates}")
                print(f"  • Всего стоматологических в таблице: {self.dental_count}")
                print(f"{'='*50}")
                
                pages_processed += 1
                
                # Проверяем, нужно ли переходить на следующую страницу
                if pages_processed < max_pages:
                    print(f"\n➡️ Переход на страницу {page_num + 1}...")
                    if self.go_to_next_page(page_num + 1):
                        page_num += 1
                        time.sleep(3)
                    else:
                        print("❌ Не удалось перейти на следующую страницу")
                        break
                else:
                    print(f"\n✅ Обработано {max_pages} страниц - завершаем парсинг")
                    break
                    
        except KeyboardInterrupt:
            print("\n\n⚠️ ПАРСИНГ ОСТАНОВЛЕН ПОЛЬЗОВАТЕЛЕМ")
        
        # Финальная статистика
        self.print_final_stats()

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
