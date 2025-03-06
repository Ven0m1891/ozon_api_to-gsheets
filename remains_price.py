import gspread
from gspread.utils import rowcol_to_a1
import requests
import json
from datetime import datetime

import pytz
import inspect


with open("tg_config.json", "r", encoding="utf-8") as file:
        data = json.load(file)
        token = data["token"]
        chat_id = data["chat_id"]


def send_telegram_notification_error(message):
    '''Функция отправки уведомления администратору в Телеграмм'''
    try:
        url1 = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            'chat_id': chat_id,
            'text': str(message)
        }
        print("Админу отправлено успешно!")
        response = requests.post(url1, data=payload)
        if response.status_code == 200:
            print("Админу отправлено успешно!")
        else:
            print(f"Ошибка отправки сообщения: {response.status_code}")
    except Exception as e:
        print(f"Ошибка отправки сообщения в Telegram: {e}")


def log_error(project_name, function_name, error_code):
    '''Функция записи ошибок в лог-файл'''

    # Устанавливаем московский часовой пояс
    moscow_tz = pytz.timezone('Europe/Moscow')
    # Получаем текущее время в Москве
    current_time = datetime.now(moscow_tz).strftime('%Y-%m-%d %H:%M:%S')
    
    # Формируем строку сообщения
    message = f"{current_time}, Кабинет: {project_name}, Функция: {function_name}, Ошибка: {error_code}\n"
    
    # Определяем имя файла как имя проекта
    file_name = f"{project_name}.txt"
    
    # Записываем сообщение в файл с именем проекта
    with open(file_name, 'a', encoding='utf-8') as file:
        file.write(message)


def get_request(method, head, body, project_name, bot_type):
    '''Универсальная функция отправки HTTP запроса по Ozon Seller API'''

    try:
        response = requests.post(method, headers=head, data=body)
        if response.status_code == 200:
            response = response.json()
            # print(response)
            try:
                if 'result' not in response:
                    raise KeyError(f"Ошибка ключа в боте {bot_type} {project_name}: Ключ 'result' отсутствует в ответе сервера")
                result = response.get('result') 
                return result
                
            except KeyError as e:
                error_message = f"Ошибка ключа в боте {bot_type} {project_name}: {str(e)}"
                print(error_message)
                send_telegram_notification_error(error_message)

            except IndexError as e:
                error_message = f"Ошибка индекса в боте {bot_type} {project_name}: {str(e)}"
                print(error_message)
                send_telegram_notification_error(error_message)

            except Exception as e:
                # Общий блок для перехвата остальных ошибок
                error_message = f"Возникла другая ошибка боте {bot_type} {project_name}: {str(e)}"
                print(error_message)
                send_telegram_notification_error(error_message)
        else:
            print(response.json())
            print(f"Ошибка отправки сообщения: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print("Response text:", e)
        error_message = f"Возникла ошибка обработки запроса в боте {bot_type} {project_name}: {e}"
        send_telegram_notification_error(error_message)
        log_error(project_name, function_name, error_message)
    except Exception as e:
        function_name = inspect.currentframe().f_code.co_name
        error_message = f"Другая ошибка в функции {function_name} {bot_type} для проекта '{project_name}': {e}"
        send_telegram_notification_error(error_message)
        log_error(project_name, function_name, error_message)
        print(error_message)
   

def get_request2(method, head, body, project_name, bot_type):
    '''Универсальная функция отправки HTTP запроса по Ozon Seller API'''

    try:
        response = requests.post(method, headers=head, data=body)
        if response.status_code == 200:
            response = response.json()
            # print(response)
            try:
                if 'items' not in response:
                    raise KeyError(f"Ошибка ключа в боте {bot_type} {project_name}: Ключ 'items' отсутствует в ответе сервера")
                result = response.get('items') 
                return result
                
            except KeyError as e:
                error_message = f"Ошибка ключа в боте {bot_type} {project_name}: {str(e)}"
                print(error_message)
                send_telegram_notification_error(error_message)

            except IndexError as e:
                error_message = f"Ошибка индекса в боте {bot_type} {project_name}: {str(e)}"
                print(error_message)
                send_telegram_notification_error(error_message)

            except Exception as e:
                # Общий блок для перехвата остальных ошибок
                error_message = f"Возникла другая ошибка боте {bot_type} {project_name}: {str(e)}"
                print(error_message)
                send_telegram_notification_error(error_message)
        else:
            print(response.json())
            print(f"Ошибка отправки сообщения: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print("Response text:", e)
        error_message = f"Возникла ошибка обработки запроса в боте {bot_type} {project_name}: {e}"
        send_telegram_notification_error(error_message)
        log_error(project_name, function_name, error_message)
    except Exception as e:
        function_name = inspect.currentframe().f_code.co_name
        error_message = f"Другая ошибка в функции {function_name} {bot_type} для проекта '{project_name}': {e}"
        log_error(project_name, function_name, error_message)
        print(error_message)


def get_items(head: dict, project_name: str, max_retries: int = 3):
    '''Функция получения списка product_id товаров с меткой "в продаже" необходимых для работы функции получения остатков'''

    method = "https://api-seller.ozon.ru/v3/product/list"
    body = {
        "filter": {
            "offer_id": [],
            "product_id": [],
            "visibility": "IN_SALE"
        },
        "last_id": "",
        "limit": 1000
    }

    body = json.dumps(body)
    
    retries = 0
    while retries < max_retries:
        try:
            items = get_request(method=method, head=head, body=body, project_name=project_name, bot_type="items")
            # print(f'Items: {items}')
            
            if items is not None:
                items_list = [i.get('product_id') for i in items.get('items')]
                # print(items_list)
                return items_list
            else:
                retries += 1
                print(f"Attempt {retries} failed. Retrying...")

        except requests.RequestException as e:
            function_name = inspect.currentframe().f_code.co_name
            error_message = f"Ошибка получения данных {function_name} {project_name}"
            print(error_message)
            log_error(project_name, function_name, error_message)
            retries += 1
            print(f"Attempt {retries} failed. Retrying...")

    print("Max retries reached. Returning None.")
    return None


def get_remains(product_ids: list, head: dict, project_name: str):
    '''Функция получения данных остатков товаров на складах'''

    method = "https://api-seller.ozon.ru/v3/product/info/list"
    body = {       
            "product_id": product_ids
            }

    body = json.dumps(body)
    try:
        ozon_answer = get_request2(method=method, head=head, body=body, project_name=project_name, bot_type="remains")
        # print(f'ozon_answer: {ozon_answer}')
        if ozon_answer is not None:
            remains = [{"sku": int(i.get("sources")[0].get("sku")) if isinstance(i.get("sources"), list) and len(i.get("sources")) > 0 else None,
                        "remains": next((stock.get("present") for stock in i.get("stocks", {}).get("stocks", []) if stock.get("source") == "fbo"), 0)}
                        for i in ozon_answer if isinstance(i, dict)]
            # for sku, remain in remains.items():
            #     print(f'{sku}: {remain}')
            return remains

    except requests.RequestException as e:
        function_name = inspect.currentframe().f_code.co_name
        error_message = f"Ошибка получения данных {function_name} {project_name}"
        print(error_message)
        log_error(project_name, function_name, error_message)
        return None
    
    except Exception as e:
        function_name = inspect.currentframe().f_code.co_name
        error_message = f"Ошибка получения данных {function_name} {project_name}: {e}"
        print(error_message)
        log_error(project_name, function_name, error_message)
        return None


def get_price(product_ids: list, head: dict, project_name: str):
    '''Функция получения данных цен товаров'''
    method = "https://api-seller.ozon.ru/v5/product/info/prices"
    body = {
            "cursor": "",
            "filter": {
            "offer_id": [],
            "product_id": product_ids,
            "visibility": "IN_SALE"
            },
            "limit": 1000
            }

    body = json.dumps(body)
    try:
        ozon_answer = get_request2(method=method, head=head, body=body, project_name=project_name, bot_type="price")
        # print(f'ozon_answer: {ozon_answer}')
        if ozon_answer is not None:
            price = [{'product_id': str(i.get('product_id')), 'Реальная цена': i.get('price').get('marketing_seller_price'), 
                     'Цена с СПП': i.get('price').get('marketing_price')} for i in ozon_answer]
            print(f'price: {price}')
            return price
        else:
            return None
        
    except requests.RequestException as e:
        function_name = inspect.currentframe().f_code.co_name
        error_message = f"Ошибка получения данных {function_name} {project_name}"
        print(error_message)
        log_error(project_name, function_name, error_message)
        return None
    
    except Exception as e:
        function_name = inspect.currentframe().f_code.co_name
        error_message = f"Ошибка получения данных {function_name} {project_name}: {e}"
        print(error_message)
        log_error(project_name, function_name, error_message)
        return None
    

def gsheets_output(date_str_dmy: str, data_to_gsheet: dict|list, account, sheet_id, worksheet):
    '''Функция записи полученных через ozon api данных в учетную гугл таблицу'''

    gc = gspread.service_account(filename=account)
    sh = gc.open_by_key(sheet_id)  # открываем таблицу по ключу
    worksheet = sh.worksheet(worksheet)  # выбираем рабочий лист в таблице
    try:    
        if isinstance(data_to_gsheet, dict): 

            sku_list = [int(i) if i.isdigit() else i for i in worksheet.col_values(2)]  # получение списка строковых значений столбца с артикулами

            cell = worksheet.find("Остатки")  # находим координаты ячейки остатков
            a1 = rowcol_to_a1(cell.row, cell.col) # перводим координаты ячейки в А1 нотацию
            remains_a1 = a1[:len(a1) - 1] # обрезаем координаты в A1 нотации до буквы столбца

            lst_update = []
            for key, value in data_to_gsheet.items():
                if key in sku_list:
                    lst_update.append({'range': f'{remains_a1}{sku_list.index(key) + 1}', 'values': [[value]]})

            # print(lst_update)
            worksheet.batch_update(lst_update)
            print("\nЗапись остатков товаров в таблицу успешно выполнена")

        elif isinstance(data_to_gsheet, list):

            id_lst = worksheet.col_values(4)  # получение списка значений столбца 4

            cells = worksheet.findall(date_str_dmy)

            cell = cells[1] if len(cells) > 1 else cells[0]   # находим данные ячейки с датой
            a1 = rowcol_to_a1(cell.row, cell.col)  # переводим в А1 нотацию
            date_str_a1 = a1[:len(a1) - 1]  # оставляем только букву столбца

            lst_update = []
            for i in data_to_gsheet:
                if i.get("product_id") in id_lst:
                    price_spp = f"{date_str_a1}{id_lst.index(i.get('product_id')) + 12}"
                    price_sel = f"{date_str_a1}{id_lst.index(i.get('product_id')) + 14}"
                    lst_update.append({'range': f'{price_spp}', 'values': [[i.get("Цена с СПП")]]})
                    lst_update.append({'range': f'{price_sel}', 'values': [[i.get("Реальная цена")]]})

            # print(lst_update)
            worksheet.batch_update(lst_update)
            print(f"Запись цен  товаров за {date_str_dmy} в таблицу успешно выполнена")
        
        else:
            print("Данные для записи в таблицу отсутствуют")
    except Exception as e:
        function_name = inspect.currentframe().f_code.co_name
        error_message = f"Ошибка загрузки данных в гугл таблицу {sheet_id}: {e}"
        print(error_message)
        log_error(sheet_id, function_name, error_message)


def remains_price(config_dict, gsheet_dict):
    '''Основная функция получения данных остатков через ozon api и записи их в учетную гугл таблицу для каждого проекта'''

    date_request = datetime.today()
    date_str_dmy = date_request.strftime("%d.%m.%Y")

    print(f'Дата выгрузки: {date_str_dmy}')
    
    head = config_dict["head"]
    project_name = config_dict["project"]
    google_account = gsheet_dict["google_account"]
    sheet_id = gsheet_dict["sheet"]
    worksheet = gsheet_dict["worksheet"]

    product_ids = get_items(head, project_name)
    remains = get_remains(product_ids, head, project_name)
    gsheets_output(date_str_dmy, remains, google_account, sheet_id, worksheet)

    price = get_price(product_ids, head, project_name)
    gsheets_output(date_str_dmy, price, google_account, sheet_id, worksheet)


def main():
    with open("config.json", "r", encoding="utf-8") as file:
        data = json.load(file)
        ik_1 = data['ik_1']
        ik_2 = data['ik_2']
        p_1 = data["project1"]
        p_2 = data["project2"]

    # Запускаем основную функцию для каждого кабинета  
    remains_price(p_1, ik_1)
    remains_price(p_2, ik_2)


if __name__ == "__main__":
    main()

