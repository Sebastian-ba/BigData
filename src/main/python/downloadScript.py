from bs4 import BeautifulSoup
import os, requests
import re

# Osfuscate -> separate file
router_dir = "../../../data/routers/"
room_dir = "../../../data/rooms/"
meta_dir = "../../../data/meta/"
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

if not os.path.exists(router_dir) and not os.path.exists(room_dir) and not os.path.exists(meta_dir):
	print('make dirs')
	os.makedirs(router_dir)
	os.makedirs(room_dir)
	os.makedirs(meta_dir)

for link, name in listFiles(url, extension):
	print(link)
	r = requests.get(link)
	if(re.match('^([\d]+-[\d]+-[\d]{4}.json)$', name)):
		with open(router_dir + name, "wb") as file:
			file.write(r.content)
	elif(re.match('^(rooms-[\d]{4}-[\d]{2}-[\d]{2}.json)$', name)):
		with open(room_dir + name, "wb") as file:
			file.write(r.content)
	else:
		with open(meta_dir + name, "wb") as file:
			file.write(r.content)