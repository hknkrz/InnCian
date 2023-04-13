import concurrent.futures
import os
import threading

from selenium import webdriver
import requests
from time import sleep
import math
from selenium.webdriver.common.by import By
import numpy as np

MIN_BATCH_BORDER = 3700000
MAX_BATCH_BORDER = 610000000
INF_BORDER = 9000000000
MAX_BATCH_SIZE = 199


def GetPriceBatches(proxy):
    batch_borders = []
    try:
        options = webdriver.FirefoxOptions()
        options.set_preference('network.proxy.type', 1)
        options.set_preference('network.proxy.socks', proxy[0])
        options.set_preference('network.proxy.socks_port', proxy[1])
        options.set_preference("network.proxy.socks_version", 5)
        options.set_preference("network.proxy.socks_username", proxy[2])
        options.set_preference("network.proxy.socks_password", proxy[3])
        driver = webdriver.Firefox(options=options)

        driver.get('https://www.cian.ru/')
        sleep(1)
        checkbox_button = driver.find_element('xpath',
                                              '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/div[2]/button')
        checkbox_button.click()

        checkbox_1 = driver.find_element('xpath',
                                         '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/div[2]/div/ul[1]/li[3]/button')
        checkbox_2 = driver.find_element('xpath',
                                         '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/div[2]/div/ul[1]/li[4]/button')
        checkbox_3 = driver.find_element('xpath',
                                         '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/div[2]/div/ul[1]/li[5]/button')
        checkbox_4 = driver.find_element('xpath',
                                         '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/div[2]/div/ul[1]/li[6]/button')
        checkbox_5 = driver.find_element('xpath',
                                         '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/div[2]/div/ul[2]/li[1]/label/span')
        checkbox_6 = driver.find_element('xpath',
                                         '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[2]/div/div[2]/div/ul[2]/li[2]/label/span')
        checkbox_1.click()
        checkbox_2.click()
        checkbox_3.click()
        checkbox_4.click()
        checkbox_5.click()
        checkbox_6.click()

        search_button = driver.find_element('xpath',
                                            '/html/body/main/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/div/div[3]/span/span[2]/a')
        search_button.click()
        sleep(1)

        price_button = driver.find_element('xpath', '/html/body/div[1]/div/div[1]/div/div/div/div/div[1]/div[4]/div/button')
        price_button.click()

        price_from_field = driver.find_element('xpath',
                                               '/html/body/div[1]/div/div[1]/div/div/div/div/div[1]/div[4]/div/div/div[1]/div/div[1]/div/input')

        price_to_field = driver.find_element('xpath',
                                             '/html/body/div[1]/div/div[1]/div/div/div/div/div[1]/div[4]/div/div/div[1]/div/div[2]/div/input')
        price_to_field.send_keys(MIN_BATCH_BORDER)
        sleep(3)
        quantity_element = driver.find_element('xpath',
                                               '/html/body/div[1]/div/div[1]/div/div/div/div/div[1]/div[4]/div/div/div/div[2]/div')
        quantity = quantity_element.get_attribute('innerText').split()[1]

        batch_borders = [[0, MIN_BATCH_BORDER, True], [MIN_BATCH_BORDER, MAX_BATCH_BORDER, False],
                         [MAX_BATCH_BORDER, INF_BORDER, True]]
        price_from_field.clear()
        price_to_field.clear()
        condition = True
        while condition:
            condition = False
            for i, batch in enumerate(batch_borders):
                if batch[2] == False:
                    condition = True
                    left_batch = [batch[0], (batch[0] + batch[1]) // 2, False]
                    right_batch = [1 + (batch[0] + batch[1]) // 2, batch[1], False]

                    # Check left batch
                    price_from_field.send_keys(left_batch[0])
                    price_to_field.send_keys(left_batch[1])
                    sleep(2.1)
                    if int(quantity_element.get_attribute('innerText').split()[1]) < MAX_BATCH_SIZE:
                        left_batch[2] = True
                    price_from_field.clear()
                    price_to_field.clear()

                    # Check right batch
                    sleep(1)
                    price_from_field.send_keys(right_batch[0])
                    price_to_field.send_keys(right_batch[1])
                    sleep(2.1)
                    if int(quantity_element.get_attribute('innerText').split()[1]) < MAX_BATCH_SIZE:
                        right_batch[2] = True
                    price_from_field.clear()
                    price_to_field.clear()
                    sleep(1)

                    batch_borders.insert(i, right_batch)
                    batch_borders.insert(i, left_batch)
                    batch_borders.remove(batch)

        driver.close()
        return batch_borders
    except:
        return batch_borders




#batches = GetPriceBatches('188.74.210.21:6100:asrwfucf:ofwtdftds5xe')
#batches_cut = []
#for batch in batches:
#    batches_cut.append([batch[0], batch[1]])
#np.savez('batches.npz',batches=batches_cut)

