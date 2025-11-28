import json
import re
import time
from typing import List, Dict, Any, Optional
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException


def parse_price(price_text: str) -> float:
    price_text = price_text.replace('от', '').replace('₽', '').replace(' ', '').replace('\xa0', '')
    try:
        return float(price_text)
    except ValueError:
        return 0.0


def parse_duration(duration_text: str) -> Dict[str, int]:
    hours = 0
    minutes = 0
    if 'ч' in duration_text:
        hours_match = re.search(r'(\d+)\s*ч', duration_text)
        if hours_match:
            hours = int(hours_match.group(1))
    if 'м' in duration_text:
        minutes_match = re.search(r'(\d+)\s*м', duration_text)
        if minutes_match:
            minutes = int(minutes_match.group(1))
    return {'hours': hours, 'minutes': minutes}


def parse_seat_from_svg(place_element) -> Optional[Dict[str, Any]]:
    place_id = place_element.get_attribute('id')
    if not place_id:
        return None
    
    place_number_match = re.search(r'place-(\d+)', place_id)
    if not place_number_match:
        return None
    
    place_number = int(place_number_match.group(1))
    
    classes = place_element.get_attribute('class') or ''
    is_reserved = 'Place_reserved' in classes
    is_available = 'Place_available' in classes
    is_cheapest = 'Place_theme_cheapestPrice' in classes
    is_upper_average = 'Place_theme_upperAveragePrice' in classes
    
    seat_number = place_number
    
    try:
        text_elements = place_element.find_elements(By.TAG_NAME, 'text')
        for text_elem in text_elements:
            text_id = text_elem.get_attribute('id')
            if text_id and text_id.isdigit():
                seat_number = int(text_id)
                break
            text_content = text_elem.text.strip()
            if text_content.isdigit():
                seat_number = int(text_content)
                break
            tspan_elements = text_elem.find_elements(By.TAG_NAME, 'tspan')
            for tspan in tspan_elements:
                tspan_text = tspan.text.strip()
                if tspan_text.isdigit():
                    seat_number = int(tspan_text)
                    break
            if seat_number != place_number:
                break
    except (NoSuchElementException, Exception):
        pass
    
    is_upper = None
    try:
        place_g_elements = place_element.find_elements(By.XPATH, './/g[@id="place"]')
        if place_g_elements:
            for place_g in place_g_elements:
                rect_elements = place_g.find_elements(By.TAG_NAME, 'rect')
                for rect_elem in rect_elements:
                    rect_id = rect_elem.get_attribute('id')
                    if rect_id and ('place' in rect_id or 'trigger' not in rect_id):
                        y_attr = rect_elem.get_attribute('y')
                        if y_attr:
                            try:
                                y_pos = float(y_attr)
                                if y_pos < 50:
                                    is_upper = True
                                elif y_pos >= 50:
                                    is_upper = False
                                break
                            except ValueError:
                                continue
        else:
            rect_elements = place_element.find_elements(By.TAG_NAME, 'rect')
            for rect_elem in rect_elements:
                rect_id = rect_elem.get_attribute('id')
                if rect_id and 'place' in rect_id:
                    y_attr = rect_elem.get_attribute('y')
                    if y_attr:
                        try:
                            y_pos = float(y_attr)
                            if y_pos < 50:
                                is_upper = True
                            elif y_pos >= 50:
                                is_upper = False
                            break
                        except ValueError:
                            continue
    except (NoSuchElementException, Exception):
        pass
    
    return {
        'seat_number': seat_number,
        'is_reserved': is_reserved,
        'is_available': is_available,
        'is_upper': is_upper,
        'is_cheapest': is_cheapest,
        'is_upper_average': is_upper_average
    }


def parse_car_info(car_li_element) -> Dict[str, Any]:
    car_number = None
    try:
        car_number_element = car_li_element.find_element(By.CSS_SELECTOR, 'span.l6DWZ')
        car_number_text = car_number_element.text.strip()
        car_number_match = re.search(r'(\d+)', car_number_text)
        if car_number_match:
            car_number = int(car_number_match.group(1))
    except NoSuchElementException:
        pass
    
    carrier = None
    try:
        carrier_elements = car_li_element.find_elements(By.XPATH, ".//div[contains(text(), 'Перевозчик:')]")
        if carrier_elements:
            carrier = carrier_elements[0].text.replace('Перевозчик:', '').strip()
    except NoSuchElementException:
        pass
    
    car_class = None
    try:
        class_elements = car_li_element.find_elements(By.CSS_SELECTOR, 'span.zYboa')
        if class_elements:
            car_class = class_elements[0].text.strip()
    except NoSuchElementException:
        pass
    
    details = None
    try:
        details_elements = car_li_element.find_elements(By.CSS_SELECTOR, 'div[data-qa="content"]')
        if details_elements:
            details = details_elements[0].text.strip()
    except NoSuchElementException:
        pass
    
    seats = []
    try:
        section_elements = car_li_element.find_elements(By.CSS_SELECTOR, 'section.q0scy')
        svg_element = None
        
        if section_elements:
            svg_elements = section_elements[0].find_elements(By.CSS_SELECTOR, 'svg')
            if svg_elements:
                svg_element = svg_elements[0]
        
        if not svg_element:
            svg_elements = car_li_element.find_elements(By.CSS_SELECTOR, 'svg')
            if svg_elements:
                svg_element = svg_elements[0]
        
        if svg_element:
            places_container = None
            try:
                places_container = svg_element.find_element(By.CSS_SELECTOR, 'g#places')
            except NoSuchElementException:
                try:
                    places_container = svg_element.find_element(By.XPATH, './/g[@id="places"]')
                except NoSuchElementException:
                    pass
            
            if places_container:
                place_elements = places_container.find_elements(By.CSS_SELECTOR, 'g[id^="tier/place-"]')
            else:
                place_elements = svg_element.find_elements(By.CSS_SELECTOR, 'g[id^="tier/place-"]')
                if not place_elements:
                    place_elements = svg_element.find_elements(By.XPATH, './/g[starts-with(@id, "tier/place-")]')
            
            for place_elem in place_elements:
                seat_info = parse_seat_from_svg(place_elem)
                if seat_info:
                    seats.append(seat_info)
    except (NoSuchElementException, Exception):
        pass
    
    return {
        'car_number': car_number,
        'carrier': carrier,
        'car_class': car_class,
        'details': details,
        'seats': seats
    }


def parse_train_info(train_element) -> Dict[str, Any]:
    train_data = {}
    
    try:
        departure_date_elems = train_element.find_elements(By.CSS_SELECTOR, '.S73fV .date')
        departure_time_elems = train_element.find_elements(By.CSS_SELECTOR, '.S73fV .time')
        departure_station_elems = train_element.find_elements(By.CSS_SELECTOR, '.S73fV .mvIIq')
        
        if departure_date_elems:
            train_data['departure_date'] = departure_date_elems[0].text.strip()
        if departure_time_elems:
            train_data['departure_time'] = departure_time_elems[0].text.strip()
        if departure_station_elems:
            train_data['departure_station'] = departure_station_elems[0].text.strip()
    except NoSuchElementException:
        pass
    
    try:
        arrival_date_elems = train_element.find_elements(By.CSS_SELECTOR, '.R1XTJ .date')
        arrival_time_elems = train_element.find_elements(By.CSS_SELECTOR, '.R1XTJ .time')
        arrival_station_elems = train_element.find_elements(By.CSS_SELECTOR, '.R1XTJ .mvIIq')
        
        if arrival_date_elems:
            train_data['arrival_date'] = arrival_date_elems[0].text.strip()
        if arrival_time_elems:
            train_data['arrival_time'] = arrival_time_elems[0].text.strip()
        if arrival_station_elems:
            train_data['arrival_station'] = arrival_station_elems[0].text.strip()
    except NoSuchElementException:
        pass
    
    try:
        duration_elems = train_element.find_elements(By.CSS_SELECTOR, '.KcB98 > div:first-child')
        if duration_elems:
            train_data['duration'] = parse_duration(duration_elems[0].text.strip())
    except NoSuchElementException:
        pass
    
    try:
        train_number_elems = train_element.find_elements(By.CSS_SELECTOR, '.KcB98 span.kdDAS')
        if train_number_elems:
            train_data['train_number'] = train_number_elems[0].text.strip()
    except NoSuchElementException:
        pass
    
    try:
        train_name_elems = train_element.find_elements(By.CSS_SELECTOR, '.KcB98 span.ZAQC3 span')
        if train_name_elems:
            train_data['train_name'] = train_name_elems[0].text.strip()
    except NoSuchElementException:
        pass
    
    try:
        carrier_elems = train_element.find_elements(By.CSS_SELECTOR, '.KcB98 span.dNANh span')
        if carrier_elems:
            train_data['carrier'] = carrier_elems[0].text.strip()
    except NoSuchElementException:
        pass
    
    coach_types = []
    try:
        coach_elements = train_element.find_elements(By.CSS_SELECTOR, '.o_WNQ')
        for coach_elem in coach_elements:
            try:
                coach_type_elems = coach_elem.find_elements(By.CSS_SELECTOR, 'span.MlEeH')
                price_elems = coach_elem.find_elements(By.CSS_SELECTOR, 'span[data-qa="price"]')
                places_elems = coach_elem.find_elements(By.CSS_SELECTOR, 'div.mokft')
                
                coach_type = None
                price_from = 0.0
                places_info = None
                
                if coach_type_elems:
                    coach_type = coach_type_elems[0].text.strip()
                if price_elems:
                    price_from = parse_price(price_elems[0].text.strip())
                if places_elems:
                    places_info = places_elems[0].text.strip()
                
                if coach_type:
                    coach_types.append({
                        'type': coach_type,
                        'price_from': price_from,
                        'places_info': places_info
                    })
            except NoSuchElementException:
                continue
    except NoSuchElementException:
        pass
    
    train_data['coach_types'] = coach_types
    
    try:
        min_price_elems = train_element.find_elements(By.CSS_SELECTOR, 'span.GBwiq[data-qa="price"]')
        if min_price_elems:
            train_data['min_price'] = parse_price(min_price_elems[0].text.strip())
    except NoSuchElementException:
        pass
    
    try:
        order_link_elems = train_element.find_elements(By.CSS_SELECTOR, 'a[href*="/trains/order/"]')
        if order_link_elems:
            order_link = order_link_elems[0].get_attribute('href')
            if order_link:
                if not order_link.startswith('http'):
                    order_link = 'https://travel.yandex.ru' + order_link
                train_data['order_link'] = order_link
            else:
                train_data['order_link'] = None
        else:
            train_data['order_link'] = None
    except NoSuchElementException:
        train_data['order_link'] = None
    
    return train_data


def get_trains_with_seats(url: str, headless: bool = True, wait_timeout: int = 30, num_trains: int = 3) -> List[Dict[str, Any]]:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, wait_timeout)
        
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-seo="transport_snippet"]')))
        
        train_elements = driver.find_elements(By.CSS_SELECTOR, '[data-seo="transport_snippet"]')
        
        if not train_elements:
            return []
        
        num_trains_to_parse = min(num_trains, len(train_elements))
        result = []
        
        for train_index in range(num_trains_to_parse):
            driver.get(url)
            wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '[data-seo="transport_snippet"]')))
            
            train_elements_refresh = driver.find_elements(By.CSS_SELECTOR, '[data-seo="transport_snippet"]')
            
            if train_index >= len(train_elements_refresh):
                break
            
            try:
                WebDriverWait(driver, 10).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, '[data-seo="transport_snippet"]')) > train_index and
                              (d.find_elements(By.CSS_SELECTOR, '[data-seo="transport_snippet"]')[train_index]
                               .find_elements(By.CSS_SELECTOR, 'a[href*="/trains/order/"]') or
                               d.find_elements(By.CSS_SELECTOR, '[data-seo="transport_snippet"]')[train_index]
                               .find_elements(By.CSS_SELECTOR, 'a[href*="order"]'))
                )
            except TimeoutException:
                pass
            
            train_elements_final = driver.find_elements(By.CSS_SELECTOR, '[data-seo="transport_snippet"]')
            if train_index >= len(train_elements_final):
                continue
            train_element = train_elements_final[train_index]
            train_info = parse_train_info(train_element)
            
            if not train_info.get('order_link'):
                result.append(train_info)
                continue
            
            order_url = train_info['order_link']
            driver.get(order_url)
            wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')
            
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li.ZjgCW')))
                
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'section.q0scy svg')))
                
                car_elements = driver.find_elements(By.CSS_SELECTOR, 'li.ZjgCW')
                
                cars = []
                for car_elem in car_elements:
                    car_info = parse_car_info(car_elem)
                    cars.append(car_info)
                
                train_info['cars'] = cars
            except TimeoutException:
                train_info['cars'] = []
            
            result.append(train_info)
        
        return result
    
    except TimeoutException:
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        driver.quit()

