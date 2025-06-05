import os, json, glob, re

from datetime import datetime
from flask import Flask, render_template, request


app = Flask(__name__)

data_folder = "data"
param_options = ['Слава', 'Сила', 'Побед', 'Поражений', 'Награбил (серебро)', 'Потерял (серебро)', 'Награбил (кристаллы)', 'Потерял (кристаллы)', 'Чат']
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

def build_rating(data, param, sort_dir):
	rating = []
	for pid, hero in data.items():
		value = hero.get(param, 0)
		name = hero.get("Имя", "Безымянный")
		level = hero.get("Уровень", "?")
		rating.append((pid, name, level, value))
	reverse = sort_dir == "desc"
	rating.sort(key=lambda x: x[3], reverse=reverse)
	return rating

def build_growth_rating(data1, data2, param, sort_dir):
	growth = []
	for pid, hero2 in data2.items():
		hero1 = data1.get(pid, {})
		name = hero2.get("Имя", "Безымянный")
		level = hero2.get("Уровень", "?")
		v2 = hero2.get(param, 0)
		v1 = hero1.get(param, 0)
		diff = v2 - v1
		growth.append((pid, name, level, diff))
	reverse = sort_dir == "desc"
	growth.sort(key=lambda x: x[3], reverse=reverse)
	return growth

@app.route("/", methods=["GET", "POST"])
def index():
	json_files = get_all_json_files()
	selected_param = "Слава"
	sort_dir = "desc"
	mode = "Общий"
	selected_file = json_files[0] if json_files else None
	file1, file2 = None, None
	rating = []
	filter_value = ""

	if request.method == "POST":
		mode = request.form.get("mode") or mode
		selected_param = request.form.get("param") or selected_param
		sort_dir = request.form.get("sort") or sort_dir
		filter_value = request.form.get("filter", "").lower().strip()

		if mode == "Общий":
			selected_file = request.form.get("file") or selected_file
			data = load_data(selected_file)
			rating = build_rating(data, selected_param, sort_dir)
			filename_display = extract_datetime_from_filename(selected_file).strftime("%d.%m.%Y %H:%M:%S")

		elif mode == "Прирост":
			file1 = request.form.get("file1")
			file2 = request.form.get("file2")
			if file1 and file2:
				data1 = load_data(file1)
				data2 = load_data(file2)
				rating = build_growth_rating(data1, data2, selected_param, sort_dir)
				dt1 = extract_datetime_from_filename(file1).strftime("%d.%m.%Y %H:%M:%S")
				dt2 = extract_datetime_from_filename(file2).strftime("%d.%m.%Y %H:%M:%S")
				filename_display = f"{dt1} → {dt2}"
			else:
				filename_display = "Файлы не выбраны"

		if filter_value:
			rating = [row for row in rating if filter_value in row[1].lower()]
	else:
		data = load_data(selected_file)
		rating = build_rating(data, selected_param, sort_dir)
		filename_display = extract_datetime_from_filename(selected_file).strftime("%d.%m.%Y %H:%M:%S")

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
						   sort_options=sort_options,
						   filter_value=filter_value,
						   extract_datetime_from_filename=extract_datetime_from_filename)

if __name__ == "__main__":
	app.run()