import asyncio, nest_asyncio, re, logging
import json, glob, os, aiohttp, sys, requests, traceback

from bs4 import BeautifulSoup
from datetime import datetime
from time import sleep


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


def save_cookies(session, path):
	cookies = [{"name": c.key, "value": c.value} for c in session.cookie_jar]
	with open(path, "w", encoding="utf-8") as f:
		json.dump(cookies, f, indent=2, ensure_ascii=False)
		logging.info(f"Saved {len(cookies)} cookies.")

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
			async with session.get(url, timeout=10) as response:
				text = await response.text()
				if "Что-то пошло не так" in text:
					return hero_id, None
				if response.status == 200 and "text-center text-xl" in text:
					hero_data = parse_hero(text, hero_id)
					return hero_id, hero_data
		except Exception as e:
			traceback.print_exc()


	return hero_id, None


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

def check_site_ready(url, max_attempts=3, delay=1800):
	for attempt in range(1, max_attempts + 1):
		try:
			logging.info(f"Checking site (attempt {attempt})")
			resp = requests.get(url, cookies=cookies, headers=headers, timeout=10)
			resp.raise_for_status()
			if "img/icons/hero.png" in resp.text:
				logging.info("Checking site Success")
				return True
			else:
				logging.error("Content not as expected")
				print(resp.text)
		except requests.exceptions.RequestException as e:
			logging.error(f"Request failed: {e}")

		if attempt < max_attempts:
			logging.info(f"Waiting {delay} seconds before next attempt")
			sleep(delay)

	logging.error("Site check failed after all attempts. Aborting")
	return False

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

		save_cookies(session, os.path.join(SCRIPT_DIR, 'static', 'cfg.json'))

	logging.info('HEROES CHECK')

	timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
	filename = os.path.join(DATA_DIR, f"heroes_{timestamp}.json")
	with open(filename, "w", encoding="utf-8") as f:
		json.dump(results, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
	if not check_site_ready(dom):
		sys.exit(1)

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
