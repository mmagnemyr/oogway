from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time

relative_path = '../documentation/index.html'

html_file_path = os.path.abspath(relative_path)
url = 'file://' + html_file_path

print(url)
options = webdriver.ChromeOptions()
options.add_argument('--disable-infobars')
options.add_experimental_option("useAutomationExtension", False)
options.add_experimental_option("excludeSwitches",["enable-automation"])

# Start Chrome browser
driver = webdriver.Chrome(options=options)

# Try to navigate to the URL
try:
    driver.get(url)
    # Wait for an element on the page to be present (adjust the timeout as needed)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
except:
    # If the tab is not open, open it
    driver = webdriver.Chrome()
    driver.get(url)

# Refresh the page
driver.refresh()

# Allow some time for the page to be visible
#time.sleep(10)  # You can adjust the sleep duration as needed

# Close the browser window
input("Press Enter to exit.")
