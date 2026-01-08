import asyncio, nest_asyncio, re, logging, gzip
import json, glob, os, aiohttp, sys, requests, traceback

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from time import sleep
from gzip import open as gzopen


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

def load_env_file(path: str = ".env") -> dict:
	env = {}
	p = os.path.join(SCRIPT_DIR, path)
	if not os.path.exists(p):
		return env
	with open(p, "r", encoding="utf-8") as f:
		for line in f:
			line = line.strip()
			if not line or line.startswith("#") or "=" not in line:
				continue
			k, v = line.split("=", 1)
			env[k.strip()] = v.strip()
	return env

_ENV = load_env_file(".env")

def env_get(key: str, default: str = "") -> str:
	return os.getenv(key, _ENV.get(key, default))

cookies_json = env_get("COOKIES_JSON", "").strip()
if cookies_json:
	raw = json.loads(cookies_json)
else:
	cookies_file = env_get("COOKIES_FILE", os.path.join(SCRIPT_DIR, "static", "cfg.json"))
	with open(cookies_file, encoding="utf-8") as f:
		raw = json.load(f)

cookies = {c.get("name"): c.get("value") for c in raw if c.get("name") and c.get("value")}

domain = env_get("WK_DOMAIN", "").strip().lstrip(".")
if not domain:
	for c in raw:
		if c.get("name") == "wekings_session" and c.get("domain"):
			domain = str(c["domain"]).lstrip(".")
			break

dom = f"https://{domain}/" if domain else "https://playwekings.mobi/"


headers = {
	"User-Agent": (
		"Mozilla/5.0 (Linux; Android 10; SM-G973F) "
		"AppleWebKit/537.36 (KHTML, like Gecko) "
		"Chrome/119.0.0.0 Mobile Safari/537.36"
	)
}

def load_json_any(full_path: str):
	if full_path.endswith(".gz"):
		with gzip.open(full_path, "rt", encoding="utf-8") as f:
			return json.load(f)
	with open(full_path, "r", encoding="utf-8") as f:
		return json.load(f)


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
		content = spans[1]

		if "Клан:" in content.text:
			link = content.find("a", href=re.compile(r"/clan/info\?id=\d+"))
			if link:
				clan_id = re.search(r"id=(\d+)", link["href"]).group(1)
				data["Клан"] = link.text.strip()
				data["clan_id"] = int(clan_id)
			else:
				data["Клан"] = "не состоит"
				data["clan_id"] = 0
			continue

		if "Братство:" in content.text:
			link = content.find("a", href=re.compile(r"/brotherhood/info\?id=\d+"))
			if link:
				bh_id = re.search(r"id=(\d+)", link["href"]).group(1)
				data["Братство"] = link.text.strip()
				data["brotherhood_id"] = int(bh_id)
			else:
				data["Братство"] = "не состоит"
				data["brotherhood_id"] = 0
			continue

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

def parse_kill_beasts(html, hero_id):
	soup = BeautifulSoup(html, 'html.parser')

	achievements = soup.find_all('div', class_='flex flex-col p-2 leading-5')

	for achievement in achievements:

		name_tag = achievement.find('div', class_='font-bold item-header pb-1')
		if name_tag and name_tag.text.strip() == 'Повелитель Зверей':
			spans = achievement.select('span:has(b.font-semibold)')[:2]
			status_match = re.search(r'(\d+)\s+из\s+(\d+)', spans[0].text)
			level_match = re.search(r'(\d+)', spans[1].text)
			level =  int(level_match.group(1))
			current, total = int(status_match.group(1)), int(status_match.group(2))
			kills = level * (4 * level - 2) // 2 + current
			return kills


async def fetch_hero(session, hero_id, sem):
	url = "{}hero/detail?player={}".format(dom, hero_id)
	achievements_url = f"{dom}achievements?player={hero_id}"
	async with sem:
		try:
			async with session.get(url, timeout=15) as response:
				text = await response.text()
				if str(response.url) != url:
					logging.warning(f"[{hero_id}] Redirected to different URL: {response.url}")
					return hero_id, None
				if "Что-то пошло не так" in text:
					logging.warning(f"[{hero_id}] Страница отсутствует")
					return hero_id, None
				if response.status == 200 and "text-center text-xl" in text:
					hero_data = parse_hero(text, hero_id)

					#ачивки
					async with session.get(achievements_url, timeout=15) as ach_response:
						ach_text = await ach_response.text()
						kills = parse_kill_beasts(ach_text, hero_id)
						if kills is not None:
							hero_data["Убито зверей"] = kills

					logging.info(f"[{hero_id}] OK — {hero_data.get('Имя', 'Неизвестно')}")
					if hero_data.get("ID") != hero_id:
						logging.warning(f"ID mismatch: expected {hero_id}, got {hero_data.get('ID')}")
						return hero_id, None
					return hero_id, hero_data
				else:
					logging.warning(f"[{hero_id}] Пропущен (статус {response.status})")
		except Exception as e:
			traceback.print_exc()


	return hero_id, None


def final_ids():
	folder = DATA_DIR
	pattern = re.compile(r"^heroes_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})\.json(?:\.gz)?$")

	files_with_dates = []
	for filename in os.listdir(folder):
		m = pattern.match(filename)
		if m:
			date_str = m.group(1)
			files_with_dates.append((filename, date_str))

	files_with_dates.sort(key=lambda x: x[1], reverse=True)

	if not files_with_dates:
		return None

	latest_name = files_with_dates[0][0]
	full_path = os.path.join(folder, latest_name)
	data = load_json_any(full_path)

	return [int(x) for x in data.keys()]


def check_site_ready(url, max_attempts=15, delay=600):
	for attempt in range(1, max_attempts + 1):
		try:
			logging.info(f"Checking site (attempt {attempt})")
			resp = requests.get(url, cookies=cookies, headers=headers, timeout=10)
			resp.raise_for_status()
			if 'hero/profile' in resp.text:
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

def compress_existing_jsons(keep_days=0):
	import gzip, shutil, time
	now = time.time()
	for path in glob.glob(os.path.join(DATA_DIR, "heroes_*.json")):
		if keep_days > 0:
			mtime = os.path.getmtime(path)
			if now - mtime < keep_days * 86400:
				continue
		gz_path = path + ".gz"
		if os.path.exists(gz_path):
			os.remove(path)
			continue
		with open(path, "rb") as src, gzip.open(gz_path, "wb") as dst:
			shutil.copyfileobj(src, dst)
		os.remove(path)
		logging.info(f"Compressed & removed: {path} -> {gz_path}")

async def main(hero_ids, concurrent_limit):
	sem = asyncio.Semaphore(concurrent_limit)
	results = {}

	async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
		tasks = [fetch_hero(session, hero_id, sem) for hero_id in hero_ids]
		tasks += [fetch_hero(session, hero_id, sem) for hero_id in range(hero_ids[-1], hero_ids[-1]+300)]

		for task in asyncio.as_completed(tasks):
			hero_id, data = await task
			if data:
				real_id = data.get("ID")
				if real_id in results:
					logging.warning(f"Duplicate real ID: {real_id} (already saved)")
					continue
				results[real_id] = data


		#~ save_cookies(session, os.path.join(SCRIPT_DIR, 'static', 'cfg.json'))

	logging.info('HEROES CHECK')

	moscow_time = datetime.utcnow() + timedelta(hours=3)
	timestamp = moscow_time.strftime("%Y-%m-%d_%H-%M-%S")
	sorted_data = dict(sorted(results.items(), key=lambda x: int(x[0])))

	gz_path = os.path.join(DATA_DIR, f"heroes_{timestamp}.json.gz")
	with gzopen(gz_path, "wt", encoding="utf-8") as f:
		json.dump(sorted_data, f, ensure_ascii=False)
	logging.info(f"Saved compressed snapshot: {gz_path}")


if __name__ == "__main__":
	if not check_site_ready(dom):
		sys.exit(1)

	hero_ids = final_ids()
	nest_asyncio.apply()
	asyncio.run(main(hero_ids, concurrent_limit=10))

	#~ compress_existing_jsons(keep_days=0)
