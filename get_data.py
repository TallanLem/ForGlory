import asyncio, nest_asyncio, re, logging
import json, glob, os, aiohttp, sys, requests, traceback

from bs4 import BeautifulSoup
from datetime import datetime

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s [%(levelname)s] %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S',
	handlers=[
		logging.StreamHandler()
	]
)


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

with open(os.path.join(SCRIPT_DIR, 'static', 'cfg.json'), encoding="utf-8") as f:
	raw = json.load(f)
	cookies = {c["name"]: c["value"] for c in raw}
	dom = 'https://{}/'.format(raw[0]['domain'])

headers = {
	"User-Agent": (
		"Mozilla/5.0 (Linux; Android 10; SM-G973F) "
		"AppleWebKit/537.36 (KHTML, like Gecko) "
		"Chrome/119.0.0.0 Mobile Safari/537.36"
	)
}


def parse_hero(html, hero_id):
	soup = BeautifulSoup(html, "html.parser")
	data = {"ID": hero_id, "Чат":0}

	name_tag = soup.find("p", class_="text-center text-xl")
	if name_tag:
		data["Имя"] = name_tag.text.strip()

	stat_blocks = soup.select("div#stats div.grid.grid-cols-profileStat")

	for block in stat_blocks:
		spans = block.find_all("span")
		if len(spans) < 2:
			continue

		icon = spans[0].find("img")
		text = spans[1].text.strip()

		if ":" in text:
			key, value = map(str.strip, text.split(":", 1))
		else:
			key, value = text, ""

		if key in ("Награбил", "Потерял") and icon and icon.get("src"):
			src = icon["src"]
			if "silver" in src:
				key += " (серебро)"
			elif "crystal" in src:
				key += " (кристаллы)"

		try:
			value = int(value)
		except ValueError:
			value = value.strip()

		data[key] = value

	return data

async def fetch_hero(session, hero_id, sem):
	url = "{}hero/detail?player={}".format(dom, hero_id)
	async with sem:
		try:
			async with session.get(url, timeout=15) as response:
				text = await response.text()
				if "Что-то пошло не так" in text:
					uprint(f"[{hero_id}] Страница отсутствует")
					return hero_id, None
				if response.status == 200 and "text-center text-xl" in text:
					hero_data = parse_hero(text, hero_id)
					uprint(f"[{hero_id}] OK — {hero_data.get('Имя', 'Неизвестно')}")
					return hero_id, hero_data
				else:
					uprint(f"[{hero_id}] Пропущен (статус {response.status})")
					current_time = datetime.now().strftime('%d.%m.%Y %H:%M:%S')
					with open("errors.log", "a", encoding="utf-8") as log:
						log.write("{} [{}] Пропуск\n".format(hero_id, current_time))
		except Exception as e:
			uprint(f"[{hero_id}] Ошибка")
			traceback.print_exc()


	return hero_id, None

async def parse_chat(results):
	from bs4 import BeautifulSoup
	import glob

	date_file = "last_checked_date_chat.txt"

	last_checked = None
	if os.path.exists(date_file):
		with open(date_file, "r") as f:
			try:
				last_checked = datetime.strptime(f.read().strip(), "%Y-%m-%d %H:%M:%S")
			except:
				pass

	json_files = sorted(glob.glob(os.path.join(DATA_DIR, "heroes_*.json")), key=os.path.getmtime, reverse=True)
	previous_chat_counts = {}
	if json_files:
		with open(json_files[0], encoding="utf-8") as f:
			previous_data = json.load(f)
			for pid, pdata in previous_data.items():
				previous_chat_counts[int(pid)] = int(pdata.get("Чат", 0))

	new_counts = dict(previous_chat_counts)
	most_recent_date = last_checked

	async def fetch_page(session, page_num):
		async with session.get(f"{dom}chat?page={page_num}") as response:
			return await response.text()

	async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
		first_html = await fetch_page(session, 1)
		soup = BeautifulSoup(first_html, "html.parser")
		last_page_tag = soup.select_one('a[rel="last"]')
		max_page = int(last_page_tag["href"].split("=")[-1]) if last_page_tag else 1

		stop = False
		for page in range(1, max_page + 1):
			#~ uprint(f"Чтение страницы {page}")
			html = await fetch_page(session, page)
			soup = BeautifulSoup(html, "html.parser")
			messages = soup.select('div[role="message"]')

			for msg in messages:
				date_tag = msg.select_one("span.text-xs.flex.space-x-2")
				if not date_tag:
					continue

				try:
					raw = date_tag.text.strip().split()[0:2]
					msg_date = datetime.strptime(" ".join(raw), "%H:%M:%S %d.%m").replace(year=datetime.now().year)
				except:
					continue

				if last_checked and msg_date <= last_checked:
					stop = True
					break

				link = msg.find("a", class_="hero-link")
				if link and "player=" in link["href"]:
					player_id = int(link["href"].split("player=")[-1])
					new_counts[player_id] = new_counts.get(player_id, 0) + 1

				if not most_recent_date or msg_date > most_recent_date:
					most_recent_date = msg_date

			if stop:
				break

	if most_recent_date:
		with open(date_file, "w") as f:
			f.write(most_recent_date.strftime("%Y-%m-%d %H:%M:%S"))

	for pid, new_value in new_counts.items():
		if pid in results:
			results[pid]["Чат"] = new_value
	return results


async def main(hero_ids, concurrent_limit):
	sem = asyncio.Semaphore(concurrent_limit)
	results = {}

	async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
		tasks = [fetch_hero(session, hero_id, sem) for hero_id in hero_ids]
		tasks += [fetch_hero(session, hero_id, sem) for hero_id in range(hero_ids[-1], hero_ids[-1]+300)]
		for task in asyncio.as_completed(tasks):
			hero_id, data = await task
			if data:
				results[hero_id] = data

	logging.info('Параметры героев считаны')

	await parse_chat(results)

	logging.info('Чат проверен')

	timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
	filename = os.path.join(DATA_DIR, f"heroes_{timestamp}.json")
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(results, f, indent=2, ensure_ascii=False)



def final_ids():
	folder = DATA_DIR
	pattern = re.compile(r"heroes_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json")
	files_with_dates = []
	for filename in os.listdir(folder):
		match = pattern.match(filename)
		if match:
			date_str = match.group(1)
			files_with_dates.append((filename, date_str))

	files_with_dates.sort(key=lambda x: x[1], reverse=True)

	if files_with_dates:
		full_path = '{}/{}'.format(folder, files_with_dates[0][0])
		with open(full_path, "r", encoding="utf-8") as f:
			data = json.load(f)
			dict_keys = [int(x) for x in list(data.keys())]
			return dict_keys
	else:
		return None

if __name__ == "__main__":
	hero_ids = final_ids()
	nest_asyncio.apply()
	asyncio.run(main(hero_ids, concurrent_limit=5))

	json_files = sorted(glob.glob(os.path.join(DATA_DIR, "heroes_*.json")), key=os.path.getmtime, reverse=True)
	if json_files:
		latest_file = json_files[0]
		with open(latest_file, encoding="utf-8") as f:
			data = json.load(f)
		sorted_data = dict(sorted(data.items(), key=lambda x: int(x[0])))
		with open(latest_file, "w", encoding="utf-8") as f:
			json.dump(sorted_data, f, indent=2, ensure_ascii=False)
