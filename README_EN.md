
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
