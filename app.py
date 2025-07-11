import logging, os, json, glob, re

from flask import Flask, render_template, request
from datetime import datetime


app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

data_folder = "data"

stat_keys = ["Сила", "Защита", "Ловкость", "Мастерство", "Живучесть"]

param_options = [
	"Слава", "Побед", "Поражений", "Побед над Драконом", "Побед над Змеем",
	"По уровню", "Сила", "Защита", "Ловкость", "Мастерство", "Живучесть",
	"Награбил (серебро)", "Потерял (серебро)",
	"Награбил (кристаллы)", "Потерял (кристаллы)",
	"Братства по славе", "Кланы по славе", "Кланы по статам"
]


def extract_datetime_from_filename(filename):
	match = re.search(r"heroes_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})", filename)
	if match:
		dt = datetime.strptime("-".join(match.groups()), "%Y-%m-%d-%H-%M-%S")
		return dt
	return None


def get_all_json_files():
	files = glob.glob(os.path.join(data_folder, "heroes_*.json"))
	return sorted(files, key=lambda f: extract_datetime_from_filename(f), reverse=True)


def load_data(filepath):
	with open(filepath, encoding="utf-8") as f:
		return json.load(f)


def build_rating(data, param, previous_data=None):
	if param == "Кланы по славе":
		return build_group_rating(data, "Клан", "Слава", previous_data)
	elif param == "Братства по славе":
		return build_group_rating(data, "Братство", "Слава", previous_data)
	elif param == "Кланы по статам":
		return build_group_rating(data, "Клан", stat_keys, previous_data)
	else:
		rating = []
		for pid, hero in data.items():
			if hero.get("Имя", "").lower():
				value = hero.get(param, 0)
				name = hero.get("Имя", "Безымянный")
				level = hero.get("Уровень", "?")
				delta = None
				if previous_data and pid in previous_data:
					value_old = previous_data[pid].get(param, 0)
					if value_old:
						delta = value - value_old
					else:
						delta = 0

				rating.append((pid, name, level, value, delta))

		rating.sort(key=lambda x: x[3], reverse=1)
		rating = rating[:1000]
		return rating


def build_growth_rating(data1, data2, param):
	rating = []
	for pid, hero2 in data2.items():
		hero1 = data1.get(pid, {})
		name = hero2.get("Имя", "Безымянный")
		level = hero2.get("Уровень", "?")
		v2 = hero2.get(param, 0)
		v1 = hero1.get(param, 0)
		diff = v2 - v1
		extra = None
		if param.startswith("Награбил"):
			victories2 = hero2.get("Побед", 0)
			victories1 = hero1.get("Побед", 0)
			fights = victories2 - victories1
			if fights > 0:
				extra = round(diff / fights)
		elif param.startswith("Потерял"):
			defeats2 = hero2.get("Поражений", 0)
			defeats1 = hero1.get("Поражений", 0)
			fights = defeats2 - defeats1
			if fights > 0:
				extra = round(diff / fights)

		rating.append((pid, name, level, diff, extra))
	rating.sort(key=lambda x: x[3], reverse=1)
	rating = rating[:1000]
	return rating

def get_level_ratings(data):
	from collections import defaultdict

	grouped = defaultdict(list)
	for hero in data.values():
		if "Уровень" in hero and "Сила" in hero:
			grouped[hero["Уровень"]].append(hero)

	for level in grouped:
		grouped[level].sort(
					key=lambda h: (
						h.get("Сила", 0),
						h.get("Защита", 0),
						h.get("Ловкость", 0),
						h.get("Мастерство", 0),
						h.get("Живучесть", 0)
					),
					reverse=True
				)

	sorted_levels = sorted(grouped.keys(), reverse=True)

	return [
		{
			"level": level,
			"players": [
				{
					"ID": hero.get("ID") or hero.get("id"),
					"name": hero.get("Имя", "Безымянный"),
					"level": level,
					"strength": hero.get("Сила", 0),
					"defense": hero.get("Защита", 0),
					"dexterity": hero.get("Ловкость", 0),
					"mastery": hero.get("Мастерство", 0),
					"vitality": hero.get("Живучесть", 0),
				}
				for hero in grouped[level]
			]
		}
		for level in sorted_levels
	]

def build_group_rating(data, group_key, param, previous_data=None):
	from operator import itemgetter
	import collections

	def group_sum(dataset):
		groups = collections.defaultdict(int)
		for hero in dataset.values():
			group = hero.get(group_key)
			if not group or "не состоит" in group.lower():
				continue

			if isinstance(param, str):
				value = hero.get(param, 0)
			elif isinstance(param, list):
				value = sum(hero.get(p, 0) for p in param)
			else:
				value = 0

			groups[group] += value
		return groups

	current_groups = group_sum(data)

	prev_groups = group_sum(previous_data) if previous_data else {}

	members_by_group = collections.defaultdict(list)
	for hero in data.values():
		group = hero.get(group_key)
		if not group or "не состоит" in group.lower():
			continue

		value = hero.get(param, 0) if isinstance(param, str) else sum(hero.get(p, 0) for p in param)
		hero["value"] = value
		members_by_group[group].append(hero)

	for members in members_by_group.values():
		members.sort(key=lambda h: h["value"], reverse=True)
		for i, h in enumerate(members, 1):
			h["_rank"] = i

	result = []
	all_groups = set(current_groups) | set(prev_groups)
	for g in all_groups:
		score_now  = current_groups.get(g, 0)
		score_prev = prev_groups.get(g, 0)
		result.append({
			"name":   g,
			"score":  score_now,
			"delta":  score_now - score_prev,
			"members": members_by_group.get(g, [])
		})

	result.sort(key=itemgetter("score"), reverse=True)
	return result


@app.route("/", methods=["GET", "POST"])
def index():
	json_files = get_all_json_files()
	selected_param = "Слава"
	selected_file = json_files[0] if json_files else None
	file1 = file2 = None
	rating = []
	level_ratings = []
	filename_display = ""
	selected_level = request.form.get("level")
	files_display = [(f, extract_datetime_from_filename(f).strftime("%d.%m.%Y %H:%M")) for f in json_files]

	mode = request.form.get("mode") or "Общий"

	selected_param = request.form.get("param", selected_param)

	column2_name = "Игрок"
	column3_name = selected_param
	if selected_param == "Братства по славе":
		if mode == "Прирост":
			selected_param = 'Слава'
		column2_name = "Братство"
		column3_name = "Слава"
	elif selected_param == "Кланы по славе":
		if mode == "Прирост":
			selected_param = 'Слава'
		column2_name = "Клан"
		column3_name = "Слава"
	elif selected_param == "Кланы по статам":
		if mode == "Прирост":
			selected_param = 'Слава'
		column2_name = "Клан"
		column3_name = "Сумма статов"
	elif selected_param == "По уровню":
		if mode == "Прирост":
			selected_param = 'Слава'

	if mode == "Общий":
		selected_file = request.form.get("file", selected_file)

		data = load_data(selected_file)

		file_index = json_files.index(selected_file)
		if file_index + 1 < len(json_files):
			prev_file = load_data(json_files[file_index + 1])
		else:
			prev_file = None

		if selected_param == "По уровню":
			rating = []
			level_ratings = get_level_ratings(data)
		else:
			if selected_level and selected_level != "Все":
				selected_level = int(selected_level)
				data = {pid: h for pid, h in data.items() if h.get("Уровень") == selected_level}

			rating = build_rating(data, selected_param, prev_file)

		dt = extract_datetime_from_filename(selected_file)
		diff_hours = None
		if dt and prev_file:
			prev_dt = extract_datetime_from_filename(json_files[file_index + 1])
			if prev_dt:
				diff_hours = round((dt - prev_dt).total_seconds()/3600, 1)
		filename_display = dt.strftime("%d.%m.%Y %H:%M") if dt else selected_file

	elif mode == "Прирост":
		file1 = request.form.get("file1")
		file2 = request.form.get("file2")
		if (not file1 or not file2) and len(json_files) >= 2:
			file2 = json_files[0]
			file1 = json_files[1]
		elif len(json_files) == 1:
			file1 = file2 = json_files[0]

		if file1 and file2 and os.path.isfile(file1) and os.path.isfile(file2):
			data1 = load_data(file1)
			data2 = load_data(file2)

			if selected_level and selected_level != "Все":
				selected_level = int(selected_level)
				data2 = {pid: h for pid, h in data2.items() if h.get("Уровень") == selected_level}

			rating = build_growth_rating(data1, data2, selected_param)
			dt1 = extract_datetime_from_filename(file1)
			dt2 = extract_datetime_from_filename(file2)
			if dt1 and dt2:
				diff_hours = round((dt2 - dt1).total_seconds()/3600, 1)
			filename_display = f"{dt1.strftime('%d.%m.%Y %H:%M')} → {dt2.strftime('%d.%m.%Y %H:%M')}" if dt1 and dt2 else ""
	else:
		selected_param = request.args.get("param", param_options[0])

	all_levels = sorted({hero.get("Уровень") for d in [load_data(selected_file)] if d for hero in d.values() if isinstance(hero.get("Уровень"), int)}, reverse=True)

	param_selectable = [
			p for p in param_options
			if not (
				mode == "Прирост"
				and (p.startswith("Кланы") or p.startswith("Братства") or p == "По уровню")
			)
		]


	return render_template("index.html",
						   rating=rating,
						   level_ratings=level_ratings,
						   param=selected_param,
						   mode=mode,
						   filename_display=filename_display,
						   json_files=json_files,
						   selected_file=selected_file,
						   file1=file1,
						   file2=file2,
						   param_options=param_options,
						   column2_name=column2_name,
						   column3_name=column3_name,
						   param_selectable=param_selectable,
						   files_display=files_display,
						   extract_datetime_from_filename=extract_datetime_from_filename,
						   enumerate=enumerate,
						   diff_hours=diff_hours,
						   all_levels=all_levels,
						   selected_level=selected_level)



if __name__ == "__main__":
	app.run()
	#~ app.run(debug=True)
