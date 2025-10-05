# duplicate-triplicate-finder
# Duplicate & Triplicate Finder (Tkinter)

Графическое приложение для Windows (работает и на macOS/Linux), которое ищет **дубликаты** и **тройные копии** файлов, сравнивая **имя файла + SHA-256**. Для похожих изображений/видео поддерживаются **перцептивные хеши** (опционально).

## Возможности
- ⚡ Многопроцессорное хеширование (SHA-256) — быстро на многоядерных ЦП
- 🧰 Фильтры: маски/расширения (включить/исключить) и минимальный размер
- 🧠 Перцептивные хеши (Pillow+ImageHash): aHash/pHash для изображений, aHash кадра для видео (OpenCV, опционально)
- 🧯 Безопасно: перемещение копий в Карантин (журнал CSV + лог), пакетный откат
- 📊 Онлайн-показ «Суммарный объём дубликатов» (суммируются только копии, без повторного учёта)
- 🪟 Два окна путей: **Оригиналы** (таблица с нумерацией групп) и **Копии** (список). Выбор оригинала подсвечивает соответствующие копии
- 📋 Экспорт отчёта в CSV
- 🟦 Прогресс-бар (тёмно-синий), 0–100%

## Логика выбора «Оставить»
Если найдено ≥2 файла с **совпадающим именем** и **SHA-256** — оставляется тот, чья родительская папка создана **раньше** (по `ctime` на Windows). Остальные — «Копии».

## Установка и запуск
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
pip install -r requirements.txt
python duplicate_finder_gui.py

Для похожих изображений/видео нужны:
pip install pillow imagehash и для видео: pip install opencv-python

Карантин и откат

Каждая «разборка» создаёт батч-папку в Карантине с operations.csv и actions.log

«Откат» восстанавливает файлы из батча (при занятых путях добавляется суффикс (restored N))


GUI to find duplicate/triplicate files (name + SHA-256), quarantine/undo, perceptual hashes

---

## Файл: `README_EN.md`

```markdown
# Duplicate & Triplicate Finder (Tkinter)

A desktop GUI for Windows (also runs on macOS/Linux) to find **duplicate** and **triplicate** files by comparing **file name + SHA-256**. Optional **perceptual hashing** for similar images/videos.

## Features
- ⚡ Multi-process SHA-256 hashing — fast on multi-core CPUs
- 🧰 Filters: include/exclude masks and extensions, minimum file size
- 🧠 Perceptual hashing (Pillow+ImageHash): aHash/pHash for images; middle-frame aHash for videos (OpenCV optional)
- 🧯 Safe quarantine (CSV journal + text log) with batch undo
- 📊 Live **Total duplicate size** (sums *only copies*, de-duplicated by path)
- 🪟 Two panes of paths: **Originals** (Treeview with group index) and **Copies** (Listbox). Selecting an Original auto-selects its Copies
- 📋 CSV report export
- 🟦 Dark-blue progress bar with 0–100% updates

## Keep/Remove rule
When ≥2 files have the same **file name** and **SHA-256**, the one kept is the file whose parent folder has the **earliest creation time** (Windows `ctime`). The others are considered **Copies**.

## Install & Run
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
pip install -r requirements.txt
python duplicate_finder_gui.py
For similarity:
pip install pillow imagehash and (for video) pip install opencv-python.

Quarantine & Undo

Each cleanup creates a timestamped batch folder with operations.csv and actions.log

“Undo” restores files from that batch; if the target path is occupied, a (restored N) suffix is added.
