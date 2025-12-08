from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd

# 1. Setup Chrome WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)

# 2. Open Google Maps
driver.get("https://www.google.com/maps")

# 3. Search for a place or business
search_query = "hotels near ukunda"
search_box = driver.find_element(By.ID, "searchboxinput")
search_box.send_keys(search_query)
search_box.send_keys(Keys.ENTER)
time.sleep(5)  # wait for results to load

# 4. Scroll the results panel to load more places
scrollable_div = driver.find_element(By.XPATH, '//div[@role="feed"]')
for _ in range(50):  # scroll n times
    driver.execute_script('arguments[0].scrollTop = arguments[0].scrollHeight', scrollable_div)
    time.sleep(2)
#import pdb;pdb.set_trace()
# 5. Extract names and addresses
places = driver.find_elements(By.XPATH, '//div[@role="article"]')
data = []
for place in places:
    try:
        link_element = place.find_element(By.TAG_NAME, "a")
        link_url = link_element.get_attribute("href")
    except:
        link_url = ''

    try:
        name = place.text
    except:
        name = ""

    data.append({"name": name, "url": link_url})

# 6. Save to CSV
df = pd.DataFrame(data)
df.to_csv("google_maps_places.csv", index=False)

driver.quit()
print("Scraping complete! Saved to google_maps_places.csv")
