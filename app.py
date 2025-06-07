from flask import Flask, render_template, request
import os
import json
import glob
import re
from datetime import datetime

app = Flask(__name__)

data_folder = "data"

stat_keys = ["Сила", "Защита", "Ловкость", "Мастерство", "Живучесть"]

param_options = [
	"Слава", "Сила", "Побед", "Поражений",
	"Награбил (серебро)", "Потерял (серебро)",
	"Награбил (кристаллы)", "Потерял (кристаллы)",
	"Чат",
	"Братства по славе", "Кланы по славе", "Кланы по статам"
]

sort_options = ["desc", "asc"]


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


def build_rating(data, param, sort_dir, filter_value=""):
	if param == "Кланы по славе":
		return build_group_rating(data, "Клан", "Слава", sort_dir)
	elif param == "Братства по славе":
		return build_group_rating(data, "Братство", "Слава", sort_dir)
	elif param == "Кланы по статам":
		return build_group_rating(data, "Клан", stat_keys, sort_dir)
	else:
		rating = []
		for pid, hero in data.items():
			if filter_value.lower() in hero.get("Имя", "").lower():
				value = hero.get(param, 0)
				name = hero.get("Имя", "Безымянный")
				level = hero.get("Уровень", "?")
				rating.append((pid, name, level, value))
		reverse = sort_dir == "desc"
		rating.sort(key=lambda x: x[3], reverse=reverse)
		return rating


def build_growth_rating(data1, data2, param, sort_dir, filter_value=""):
	rating = []
	for pid, hero2 in data2.items():
		if filter_value.lower() not in hero2.get("Имя", "").lower():
			continue
		hero1 = data1.get(pid, {})
		name = hero2.get("Имя", "Безымянный")
		level = hero2.get("Уровень", "?")
		v2 = hero2.get(param, 0)
		v1 = hero1.get(param, 0)
		diff = v2 - v1
		rating.append((pid, name, level, diff))
	reverse = sort_dir == "desc"
	rating.sort(key=lambda x: x[3], reverse=reverse)
	return rating


def build_group_rating(data, group_key, param_key, sort_dir):
	groups = {}
	for pid, hero in data.items():
		group = hero.get(group_key, "").strip()
		if not group or "не состоит" in group.lower():
			continue
		value = 0
		if isinstance(param_key, list):
			value = sum(hero.get(k, 0) for k in param_key)
		else:
			value = hero.get(param_key, 0)
		name = hero.get("Имя", "Безымянный")
		level = hero.get("Уровень", "?")
		groups.setdefault(group, []).append((pid, name, level, value))

	results = []
	for group_name, members in groups.items():
		total = sum(x[3] for x in members)
		results.append((group_name, total, members))
	reverse = sort_dir == "desc"
	results.sort(key=lambda x: x[1], reverse=reverse)
	return results


@app.route("/", methods=["GET", "POST"])
def index():
	json_files = get_all_json_files()
	selected_param = "Слава"
	sort_dir = "desc"
	mode = "Общий"
	filter_value = ""
	selected_file = json_files[0] if json_files else None
	file1 = file2 = None
	rating = []
	filename_display = ""
	files_display = [(f, extract_datetime_from_filename(f).strftime("%d.%m.%Y %H:%M")) for f in json_files]

	if request.method == "POST":
		mode = request.form.get("mode", mode)
		selected_param = request.form.get("param", selected_param)
		sort_dir = request.form.get("sort", sort_dir)
		filter_value = request.form.get("filter", "").strip()

		if mode == "Общий":
			selected_file = request.form.get("file", selected_file)
			data = load_data(selected_file)
			rating = build_rating(data, selected_param, sort_dir, filter_value)
			dt = extract_datetime_from_filename(selected_file)
			filename_display = dt.strftime("%d.%m.%Y %H:%M") if dt else selected_file

		elif mode == "Прирост":
			file1 = request.form.get("file1")
			file2 = request.form.get("file2")
			#~ if file1 and file2 and not ("Клан" in selected_param or "Братство" in selected_param):
			if file1 and file2 and os.path.isfile(file1) and os.path.isfile(file2):
				data1 = load_data(file1)
				data2 = load_data(file2)
				rating = build_growth_rating(data1, data2, selected_param, sort_dir, filter_value)
				dt1 = extract_datetime_from_filename(file1)
				dt2 = extract_datetime_from_filename(file2)
				filename_display = f"{dt1.strftime('%d.%m.%Y %H:%M')} → {dt2.strftime('%d.%m.%Y %H:%M')}" if dt1 and dt2 else ""
			else:
				rating = []
				filename_display = "Нужно выбрать разные даты"

	else:
		data = load_data(selected_file)
		rating = build_rating(data, selected_param, sort_dir)
		dt = extract_datetime_from_filename(selected_file)
		filename_display = dt.strftime("%d.%m.%Y %H:%M") if dt else selected_file

	param_selectable = [p for p in param_options if mode != "Прирост" or ("Клан" not in p and "Братства" not in p)]

	return render_template("index.html",
						   rating=rating,
						   param=selected_param,
						   sort_dir=sort_dir,
						   mode=mode,
						   filename_display=filename_display,
						   json_files=json_files,
						   selected_file=selected_file,
						   file1=file1,
						   file2=file2,
						   param_options=param_options,
						   param_selectable=param_selectable,
						   sort_options=sort_options,
						   filter_value=filter_value,
						   files_display=files_display,
						   extract_datetime_from_filename=extract_datetime_from_filename,
						   enumerate=enumerate)



if __name__ == "__main__":
	app.run()
