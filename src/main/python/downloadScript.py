from bs4 import BeautifulSoup
import os, requests
import re

# Osfuscate -> separate file
directory = "../data/"
with open("scriptUrl.txt", "r") as cfg:
	url = cfg.read()
	m = re.search('(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9]\.[^\s]{2,})', url)
	url = m.group(0)
extension = 'json'

def listFiles(url, ext=''):
	page = requests.get(url).text
	# debug
	# print(page)
	soup = BeautifulSoup(page, 'html.parser')
	return [(url + node.get('href'), node.get('href')) for node in soup.find_all('a') if node.get('href').endswith(ext)]

if not os.path.exists(directory):
	print('make dir')
	os.makedirs(directory)

for link, name in listFiles(url, extension):
    print(link)
    r = requests.get(link)
    with open(directory + name, "wb") as file:
        file.write(r.content)
