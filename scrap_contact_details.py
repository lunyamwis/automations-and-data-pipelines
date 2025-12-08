from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import re
from urllib.parse import unquote
# Load the dataset
df = pd.read_csv('google_maps_places.csv')  
number_dataset = []  
for url in df['url']:
   options = webdriver.ChromeOptions()
   options.add_argument("--start-maximized")
   driver = webdriver.Chrome(options=options)
   driver.get(url)
   #time.sleep(7)
     
   try:
      #number = driver.find_element(B
      wait = WebDriverWait(driver, 10)
      phone_elem = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//button[starts-with(@data-item-id,'phone:tel:')]")
      ))

      data_item = phone_elem.get_attribute("data-item-id")
      # Extract only the phone number
      number = data_item.split(":")[-1]  # splits 'phone:tel:07248807
      # 07'

      print("Phone Number:", number)

   except:
      number = ""
      print("Phone Number:", number)

   try:
      match = re.search(r'/place/([^/]+)', url)
      if match:
         raw_name = match.group(1)
         business_name = unquote(raw_name).replace("+", " ")
         print(business_name)
      print("Business Name:", business_name)
   except:
      business_name = ""
   print(number)
   number_dataset.append({"url":url,"number":number,"business_name":business_name})
   driver.quit()

# Save the results to a new CSV file
result_df = pd.DataFrame(number_dataset)
result_df.to_csv('number_dataset.csv', index=False)