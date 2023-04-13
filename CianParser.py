import json

import fake_user_agent
import requests
import concurrent.futures
import os
import threading
import cfscrape
import requests
from time import sleep
import math
import numpy as np
import csv
import fake_headers


PAUSE_TIME = 7
CSV_PATH = os.path.normpath(os.path.join(os.getcwd(), "Csv"))
PROXIES_PATH = os.path.normpath(os.path.join(os.getcwd(), "ProxyList/proxies.txt"))


def get_session(header, user_agent):
    session = requests.Session()
    header['user-agent'] = user_agent
    session.headers = header
    return cfscrape.create_scraper(sess=session)


def GetJson(proxy, batch, csv_number, header, user_agent):
    try:
        session = get_session(header, user_agent)
        dataset = [['total_area', 'kitchen_area', 'living_area', 'build_date', 'floor', 'floors_number', 'decoration',
                    'rooms_area', 'rooms_count', 'balcony', 'offer_date', 'bedrooms_count', 'longitude', 'latitude',
                    'url',
                    'description', 'cost', 'building_type', 'passenger_elevator', 'cargo_elevator',
                    'material', 'heating', 'parking', 'metro', 'district', 'metro_distance', 'metro_transport',
                    'region', 'is_apartments', 'flat_type', 'is_auction']]

        for cur_page in range(1, 8):
            json_data = {
                'jsonQuery': {
                    '_type': 'flatsale',
                    'engine_version': {
                        'type': 'term',
                        'value': 2,
                    },
                    'region': {
                        'type': 'terms',
                        'value': [
                            1,
                        ],
                    },
                    'price': {
                        'type': 'range',
                        'value': {
                            'gte': batch[0],
                            'lte': batch[1],
                        },
                    },
                    'currency': {
                        'type': 'term',
                        'value': 2,
                    },
                    'room': {
                        'type': 'terms',
                        'value': [
                            1,
                            2,
                            3,
                            4,
                            5,
                            6,
                            7,
                            9,
                        ],
                    },
                    'page': {
                        'type': 'term',
                        'value': cur_page,
                    },
                },
            }
            #print(session.get('https://ifconfig.me/ip',
            #                  proxies={'https': f'socks5://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}'}
            #                  ).text)
            response = session.post('https://api.cian.ru/search-offers/v2/search-offers-desktop/',
                                    json=json_data,
                                    proxies={'https': f'socks5://{proxy[2]}:{proxy[3]}@{proxy[0]}:{proxy[1]}'}
                                    )

            data = response.json()
            for item in data['data']['offersSerialized']:
                dataset.append(
                    [item['totalArea']
                        , item['kitchenArea']
                        , item['livingArea']
                        , item['building']['buildYear']
                        , item['floorNumber']
                        , item['building']['floorsCount']
                        , item['decoration']
                        , item['roomArea']
                        , item['roomsCount']
                        , item['balconiesCount']
                        , item['creationDate']
                        , item['bedroomsCount']
                        , item['geo']['coordinates']['lng']
                        , item['geo']['coordinates']['lat'],
                     item['fullUrl'],
                     item['description'],
                     item['bargainTerms']['priceRur'],
                     item['building']['type'],
                     item['building']['passengerLiftsCount'],
                     item['building']['cargoLiftsCount'],
                     item['building']['materialType'],
                     item['building']['heatingType'],
                     item['building']['parking'],
                     ','.join([str(x['name']) for x in item['geo']['undergrounds']]),
                     ','.join([str(x['time']) for x in item['geo']['undergrounds']]),
                     ','.join([str(x['transportType']) for x in item['geo']['undergrounds']]),
                     ','.join([str(x['name']) for x in item['geo']['districts']]),
                     'Москва',
                     item['isApartments'],
                     item['flatType'], item['isAuction']])
            sleep(PAUSE_TIME)
        with open(os.path.join(CSV_PATH, f'{csv_number}.csv'), mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for row in dataset:
                writer.writerow(row)
        file.close()
        print(csv_number)
        sleep(PAUSE_TIME)
    except:
        print(f'fail{csv_number}')
        sleep(PAUSE_TIME)


def ParallelExecute(max_workers, batches):
    loaded_batches = GetLoadedBatches()
    file_index = [x for x in range(1, len(batches) + 1)]
    for idx in sorted(loaded_batches, reverse=True):
        if idx < len(batches):
            batches.pop(idx)
            file_index.pop(idx)
    proxy_list_ = GetProxyList(PROXIES_PATH)
    ua_list = [fake_user_agent.user_agent('Chrome') for x in proxy_list_]
    ua_list = ua_list * (math.trunc(len(batches) / len(proxy_list_)) + 1)
    ua_list = ua_list[:len(batches)]
    header_list = [fake_headers.make_header() for x in proxy_list_]
    header_list = header_list * (math.trunc(len(batches) / len(proxy_list_)) + 1)
    header_list = header_list[:len(batches)]
    proxy_list = proxy_list_ * (math.trunc(len(batches) / len(proxy_list_)) + 1)
    proxy_list = proxy_list[:len(batches)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(GetJson, proxy_list, batches, file_index, header_list, ua_list)


def LoadDataBatches():
    batches = np.load('batches.npz')['batches'].tolist()

    left_borders = [x[0] for x in batches]
    right_borders = [x[1] for x in batches]

    left_seen = set()
    left_unique = [x for x in left_borders if not (x in left_seen or left_seen.add(x))]

    right_seen = set()
    right_unique = [x for x in right_borders if not (x in right_seen or right_seen.add(x))]

    return [[x, y] for x, y in zip(left_unique, right_unique)]


def GetProxyList(proxy_file):
    proxy_list = []
    try:
        with open(proxy_file, 'r') as f:
            content = f.read().split('\n')[:-1]
            for proxy in content:
                proxy_list.append(proxy.split(':'))
    except:
        print("Что-то не так с файлом с прокси")

    return proxy_list


def GetLoadedBatches():
    used_numbers = []
    for file in os.listdir(CSV_PATH):
        used_numbers.append(int(os.path.basename(os.path.join(CSV_PATH, file)).split('.')[0]) - 1)

    return used_numbers


ParallelExecute(4, LoadDataBatches())
# GetJson(GetProxyList(PROXIES_PATH)[4], [4292090, 4440112], 4,
#        fake_headers.make_header(), fake_user_agent.user_agent())
