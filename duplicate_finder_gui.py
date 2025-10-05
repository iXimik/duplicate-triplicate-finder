#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Duplicate & Triplicate Finder (Tkinter) — v2.6

Новое в v2.6:
  ✓ Окно «Оригиналы (оставить)» заменено на таблицу (Treeview) с колонками:
      № (нумерация групп, как в окне сканирования) и Путь.
  ✓ Выбор оригинала в этой таблице по-прежнему подсвечивает соответствующие «Копии».
  ✓ Остальной функционал версии v2.5 сохранён:
      — суммарный объём дубликатов (онлайн);
      — многопроцессорный SHA-256, фильтры, карантин/откат;
      — перцептивные хеши (опц.);
      — прогресс 0–100% (тёмно-синий);
      — «Копии» — отдельный список со скроллбаром.
"""

import os
import sys
import csv
import fnmatch
import time
import hashlib
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Tuple, Optional

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# --- Windows console UTF-8 fix ---
if sys.platform.startswith("win"):
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
    except Exception:
        pass
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ---- Опциональные зависимости для перцептивных хешей ----
try:
    from PIL import Image
    import imagehash
    HAS_IMAGEHASH = True
except Exception:
    HAS_IMAGEHASH = False

try:
    import cv2  # opencv-python, опционально для видео
    HAS_CV2 = True
except Exception:
    HAS_CV2 = False

APP_TITLE = "Duplicate & Triplicate Finder — v2.6"
DEFAULT_QUARANTINE_ROOT = os.path.join(os.path.expanduser("~"), "Duplicate_Quarantine")
HASH_ALGO = "sha256"
READ_BLOCK = 1024 * 1024  # 1 MiB

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}
VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".m4v"}

# ---------- Вспомогательные ----------

def human_size(num: int) -> str:
    """Человекочитаемый размер: Б, кБ, МБ, ГБ, ТБ …"""
    units = ["", "к", "М", "Г", "Т", "П", "Э", "З", "И"]
    n = float(num)
    for u in units:
        if abs(n) < 1024.0:
            return f"{n:3.1f}{u}Б"
        n /= 1024.0
    return f"{n:.1f}ИБ"


def file_hash(path: str, algo: str = HASH_ALGO, block_size: int = READ_BLOCK) -> str:
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()


def folder_ctime(path: str) -> float:
    try:
        return os.path.getctime(path)
    except Exception:
        return os.path.getmtime(path)


def safe_relpath(path: str, start: str) -> str:
    try:
        return os.path.relpath(path, start)
    except Exception:
        return os.path.basename(path)


def ext_of(path: str) -> str:
    return os.path.splitext(path)[1].lower()

# ---------- Перцептивные хеши (опционально) ----------

def ahash_image(path: str):
    if not HAS_IMAGEHASH:
        return None
    try:
        with Image.open(path) as im:
            im = im.convert("RGB")
            return imagehash.average_hash(im)
    except Exception:
        return None


def phash_image(path: str):
    if not HAS_IMAGEHASH:
        return None
    try:
        with Image.open(path) as im:
            im = im.convert("RGB")
            return imagehash.phash(im)
    except Exception:
        return None


def ahash_video_center_frame(path: str):
    if not HAS_CV2 or not HAS_IMAGEHASH:
        return None
    try:
        cap = cv2.VideoCapture(path)
        if not cap.isOpened():
            return None
        frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) or 0
        idx = max(frames // 2, 0)
        cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
        ok, frame = cap.read()
        cap.release()
        if not ok or frame is None:
            return None
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        im = Image.fromarray(frame)
        return imagehash.average_hash(im.convert("RGB"))
    except Exception:
        return None

# ---------- Параллельное хеширование ----------

def _worker_sha256(path: str) -> Tuple[str, Optional[str]]:
    """Возвращает (path, sha256 или None в случае ошибки)."""
    try:
        return path, file_hash(path)
    except Exception:
        return path, None

# ---------- Структуры данных ----------

@dataclass
class Group:
    hash: str
    size: int
    keep: str
    others: List[str]
    kind: str = "exact"  # "exact" | "perceptual"

# ---------- Поток-сканер ----------

class Scanner(threading.Thread):
    def __init__(
        self,
        root_folder: str,
        stop_event: threading.Event,
        on_progress,
        on_group,
        on_done,
        include_masks: List[str],
        exclude_masks: List[str],
        include_exts: List[str],
        exclude_exts: List[str],
        min_size_bytes: int,
        perceptual: bool,
        perceptual_metric: str,
        perceptual_threshold: int,
        max_workers: int,
    ):
        super().__init__(daemon=True)
        self.root_folder = root_folder
        self.stop_event = stop_event
        self.on_progress = on_progress
        self.on_group = on_group
        self.on_done = on_done
        self.include_masks = include_masks
        self.exclude_masks = exclude_masks
        self.include_exts = {e.lower().strip() for e in include_exts if e.strip()}
        self.exclude_exts = {e.lower().strip() for e in exclude_exts if e.strip()}
        self.min_size = min_size_bytes
        self.perceptual = perceptual
        self.perc_metric = perceptual_metric
        self.perc_thr = perceptual_threshold
        self.max_workers = max_workers

    def _pass_filters(self, path: str, size: int) -> bool:
        if size < self.min_size:
            return False
        ext = ext_of(path)
        if self.include_exts and (ext not in self.include_exts):
            return False
        if self.exclude_exts and (ext in self.exclude_exts):
            return False
        base = os.path.basename(path)
        if self.include_masks:
            ok = any(fnmatch.fnmatch(base, m) for m in self.include_masks)
            if not ok:
                return False
        if self.exclude_masks:
            if any(fnmatch.fnmatch(base, m) for m in self.exclude_masks):
                return False
        return True

    def run(self):
        t0 = time.time()
        files: List[Tuple[str, int]] = []
        for dirpath, dirnames, filenames in os.walk(self.root_folder):
            if self.stop_event.is_set():
                break
            for name in filenames:
                full = os.path.join(dirpath, name)
                try:
                    if os.path.islink(full):
                        continue
                    st = os.stat(full)
                    if not os.path.isfile(full):
                        continue
                    if self._pass_filters(full, st.st_size):
                        files.append((full, st.st_size))
                except Exception:
                    continue

        # Группировка по размеру
        by_size: Dict[int, List[str]] = {}
        for p, sz in files:
            by_size.setdefault(sz, []).append(p)

        # Подсчет кандидатов для хеширования (только группы с >1 файлом)
        candidates = sum(len(v) for v in by_size.values() if len(v) > 1)
        self.on_progress(("start", candidates))

        processed = 0
        # Параллельное SHA256
        for size_val, paths in by_size.items():
            if self.stop_event.is_set():
                break
            if len(paths) < 2:
                processed += len(paths)
                self.on_progress(("progress", processed, candidates))
                continue

            by_hash: Dict[str, List[str]] = {}
            with ProcessPoolExecutor(max_workers=self.max_workers) as ex:
                futures = {ex.submit(_worker_sha256, p): p for p in paths}
                for fut in as_completed(futures):
                    if self.stop_event.is_set():
                        break
                    p, hv = fut.result()
                    if hv:
                        by_hash.setdefault(hv, []).append(p)
                    processed += 1
                    self.on_progress(("progress", processed, candidates))

            # Точные дубликаты: и хэш, и имя файла совпадают
            for hval, same in by_hash.items():
                if len(same) < 2:
                    continue
                by_name: Dict[str, List[str]] = {}
                for p in same:
                    name_key = os.path.basename(p).lower()
                    by_name.setdefault(name_key, []).append(p)
                for _, same_name in by_name.items():
                    if len(same_name) < 2:
                        continue
                    keep_path = min(same_name, key=lambda p: folder_ctime(os.path.dirname(p)))
                    others = sorted([p for p in same_name if p != keep_path])
                    self.on_group(
                        Group(hash=hval, size=size_val, keep=keep_path, others=others, kind="exact").__dict__
                    )

        # Перцептивная стадия (только если включена)
        if self.perceptual and not self.stop_event.is_set():
            perc_items: List[Tuple[str, str, Optional[object]]] = []  # (path, type, hashobj)
            for p, _ in files:
                ext = ext_of(p)
                if ext in IMAGE_EXTS and HAS_IMAGEHASH:
                    hv = phash_image(p) if self.perc_metric == "phash" else ahash_image(p)
                    if hv is not None:
                        perc_items.append((p, "img", hv))
                elif ext in VIDEO_EXTS and HAS_CV2 and HAS_IMAGEHASH:
                    hv = ahash_video_center_frame(p)
                    if hv is not None:
                        perc_items.append((p, "vid", hv))

            buckets: Dict[str, List[Tuple[str, object]]] = {}
            for p, _, hv in perc_items:
                key = str(hv)[:8]
                buckets.setdefault(key, []).append((p, hv))

            seen = set()
            for key, items in buckets.items():
                n = len(items)
                if n < 2:
                    continue
                for i in range(n):
                    pi, hi = items[i]
                    if pi in seen:
                        continue
                    cluster = [pi]
                    for j in range(i + 1, n):
                        pj, hj = items[j]
                        if pj in seen:
                            continue
                        try:
                            dist = hi - hj  # Hamming distance
                        except Exception:
                            continue
                        if dist <= self.perc_thr:
                            cluster.append(pj)
                    if len(cluster) >= 2:
                        keep_path = min(cluster, key=lambda p: folder_ctime(os.path.dirname(p)))
                        others = sorted([p for p in cluster if p != keep_path])
                        try:
                            size_val = os.path.getsize(keep_path)
                        except Exception:
                            size_val = 0
                        self.on_group(
                            Group(hash=f"perc:{key}", size=size_val, keep=keep_path, others=others, kind="perceptual").__dict__
                        )
                        for p in cluster:
                            seen.add(p)

        self.on_done(time.time() - t0)

# ---------- GUI-приложение ----------

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1250x780")
        self.minsize(1020, 620)

        self.stop_event = threading.Event()
        self.scanner: Optional[Scanner] = None
        self.groups: List[dict] = []

        # Соответствия keep -> [dups]
        self.keep_to_dups: Dict[str, List[str]] = {}

        # Суммарный объём найденных копий
        self.dup_total_bytes: int = 0
        self.counted_dups: set[str] = set()
        self.total_waste_var = tk.StringVar(value="0 Б")

        # Настройки
        self.root_folder_var = tk.StringVar()
        self.quarantine_root_var = tk.StringVar(value=DEFAULT_QUARANTINE_ROOT)
        self.delete_instead_var = tk.BooleanVar(value=False)
        self.min_size_mb_var = tk.DoubleVar(value=0.0)
        self.include_masks_var = tk.StringVar(value="*")
        self.exclude_masks_var = tk.StringVar(value="")
        self.include_exts_var = tk.StringVar(value="")  # пример: .jpg,.png,.mp4
        self.exclude_exts_var = tk.StringVar(value=".sys,.dll")
        self.workers_var = tk.IntVar(value=max(1, os.cpu_count() or 4))

        # Перцептивные
        self.perceptual_var = tk.BooleanVar(value=False)
        self.perc_metric_var = tk.StringVar(value="ahash")  # ahash | phash
        self.perc_thr_var = tk.IntVar(value=8)  # 0..64

        # Логирование
        self.last_batch_dir: Optional[str] = None

        self._build_ui()

    # --- UI ---
    def _build_ui(self):
        # Тёмно-синий прогресс
        style = ttk.Style(self)
        try:
            style.theme_use('clam')
        except Exception:
            pass
        style.configure(
            "DarkBlue.Horizontal.TProgressbar",
            troughcolor="#e6e6e6",
            background="#0b3d91",
            thickness=18,
        )

        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="Папка для сканирования:").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.root_folder_var, width=80).grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(top, text="Выбрать…", command=self.choose_root).grid(row=0, column=2)

        ttk.Label(top, text="Карантин:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(top, textvariable=self.quarantine_root_var, width=80).grid(row=1, column=1, sticky="we", padx=6, pady=(6, 0))
        ttk.Button(top, text="Изменить…", command=self.choose_quarantine).grid(row=1, column=2, pady=(6, 0))

        self.delete_chk = ttk.Checkbutton(top, text="Удалять копии без карантина (опасно)", variable=self.delete_instead_var)
        self.delete_chk.grid(row=2, column=1, sticky="w", pady=(6, 0))

        # Фильтры
        filt = ttk.LabelFrame(self, text="Фильтры", padding=10)
        filt.pack(fill="x", padx=10, pady=6)
        ttk.Label(filt, text="Мин. размер (МБ):").grid(row=0, column=0, sticky="w")
        ttk.Entry(filt, textvariable=self.min_size_mb_var, width=8).grid(row=0, column=1, sticky="w", padx=(4, 12))
        ttk.Label(filt, text="Включить маски (через запятую):").grid(row=0, column=2, sticky="e")
        ttk.Entry(filt, textvariable=self.include_masks_var, width=40).grid(row=0, column=3, sticky="we", padx=(4, 12))
        ttk.Label(filt, text="Исключить маски:").grid(row=0, column=4, sticky="e")
        ttk.Entry(filt, textvariable=self.exclude_masks_var, width=30).grid(row=0, column=5, sticky="we", padx=(4, 12))

        ttk.Label(filt, text="Включить расширения (.jpg,.png):").grid(row=1, column=2, sticky="e", pady=(6, 0))
        ttk.Entry(filt, textvariable=self.include_exts_var, width=40).grid(row=1, column=3, sticky="we", padx=(4, 12), pady=(6, 0))
        ttk.Label(filt, text="Исключить расширения:").grid(row=1, column=4, sticky="e", pady=(6, 0))
        ttk.Entry(filt, textvariable=self.exclude_exts_var, width=30).grid(row=1, column=5, sticky="we", padx=(4, 12), pady=(6, 0))

        ttk.Label(filt, text="Потоки (процессы):").grid(row=0, column=6, sticky="e")
        ttk.Entry(filt, textvariable=self.workers_var, width=6).grid(row=0, column=7, sticky="w", padx=(4, 0))

        for c in range(8):
            filt.columnconfigure(c, weight=1 if c in (3, 5) else 0)

        # Перцептивные настройки + поле «Суммарный объём дубликатов»
        perc = ttk.LabelFrame(self, text="Похожие медиа (перцептивные хеши)", padding=10)
        perc.pack(fill="x", padx=10, pady=6)

        ttk.Checkbutton(perc, text="Искать похожие изображения/видео", variable=self.perceptual_var)\
            .grid(row=0, column=0, sticky="w")
        ttk.Label(perc, text="Метрика:").grid(row=0, column=1, sticky="e")
        ttk.Combobox(perc, textvariable=self.perc_metric_var, values=["ahash", "phash"], width=8, state="readonly")\
            .grid(row=0, column=2, sticky="w")
        ttk.Label(perc, text="Порог расстояния (Хэмминга):").grid(row=0, column=3, sticky="e")
        ttk.Spinbox(perc, from_=0, to=64, textvariable=self.perc_thr_var, width=6).grid(row=0, column=4, sticky="w")

        ttk.Label(perc, text="Суммарный объём дубликатов:").grid(row=0, column=5, sticky="e", padx=(20, 4))
        self.total_waste_entry = ttk.Entry(perc, textvariable=self.total_waste_var, width=28, state="readonly")
        self.total_waste_entry.grid(row=0, column=6, sticky="we")

        ttk.Label(perc,
                  text="Требуются Pillow+ImageHash (и OpenCV для видео). Чем выше порог — тем грубее совпадения.")\
            .grid(row=1, column=0, columnspan=7, sticky="w", pady=(6, 0))

        for c in range(7):
            perc.columnconfigure(c, weight=1 if c in (6,) else 0)

        # Кнопки управления
        ctrl = ttk.Frame(self, padding=(10, 0, 10, 5))
        ctrl.pack(fill="x")
        self.btn_scan = ttk.Button(ctrl, text="Сканировать", command=self.start_scan)
        self.btn_stop = ttk.Button(ctrl, text="Остановить", command=self.stop_scan, state="disabled")
        self.btn_auto = ttk.Button(ctrl, text="Авторазобрать всё", command=self.auto_resolve_all, state="disabled")
        self.btn_scan.pack(side="left")
        self.btn_stop.pack(side="left", padx=6)
        self.btn_auto.pack(side="left")

        # Прогресс
        prog = ttk.Frame(self, padding=(10, 0, 10, 5))
        prog.pack(fill="x")
        self.progress = ttk.Progressbar(prog, orient="horizontal", mode="determinate",
                                        style="DarkBlue.Horizontal.TProgressbar")
        self.progress.pack(fill="x")
        self.status_var = tk.StringVar(value="Готово.")
        ttk.Label(prog, textvariable=self.status_var).pack(anchor="w")

        # Таблица групп + скроллбар (высота 9 строк)
        table_frame = ttk.Frame(self)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("idx", "type", "size", "keep", "count")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=9)
        self.tree.heading("idx", text="№")
        self.tree.heading("type", text="Тип")
        self.tree.heading("size", text="Размер")
        self.tree.heading("keep", text="Оставить (самая ранняя папка)")
        self.tree.heading("count", text="Копий")

        self.tree.column("idx", width=50, anchor="center")
        self.tree.column("type", width=90, anchor="center")
        self.tree.column("size", width=110, anchor="center")
        self.tree.column("keep", width=900, anchor="w")
        self.tree.column("count", width=90, anchor="center")

        yscroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)
        self.tree.bind("<<TreeviewSelect>>", self.on_table_select)

        # СПЛИТ-ПАНЕЛЬ: «Оригиналы» (Treeview с №) и «Копии» (Listbox)
        split = ttk.Panedwindow(self, orient="horizontal")
        split.pack(fill="both", expand=False, padx=10, pady=(0, 10))

        # LEFT: Оригиналы с нумерацией
        left = ttk.Frame(split, padding=(0, 0, 6, 0))
        ttk.Label(left, text="Оригиналы (оставить):").pack(anchor="w")
        keep_box_frame = ttk.Frame(left)
        keep_box_frame.pack(fill="both", expand=True)

        self.keep_tree = ttk.Treeview(keep_box_frame, columns=("kidx", "kpath"), show="headings", height=9)
        self.keep_tree.heading("kidx", text="№")
        self.keep_tree.heading("kpath", text="Путь")
        self.keep_tree.column("kidx", width=50, anchor="center")
        self.keep_tree.column("kpath", width=1000, anchor="w")

        keep_scroll = ttk.Scrollbar(keep_box_frame, orient="vertical", command=self.keep_tree.yview)
        self.keep_tree.configure(yscrollcommand=keep_scroll.set)
        self.keep_tree.pack(side="left", fill="both", expand=True)
        keep_scroll.pack(side="right", fill="y")

        # RIGHT: Копии
        right = ttk.Frame(split, padding=(6, 0, 0, 0))
        ttk.Label(right, text="Копии (будут убраны):").pack(anchor="w")
        dup_box_frame = ttk.Frame(right)
        dup_box_frame.pack(fill="both", expand=True)
        self.others_list = tk.Listbox(dup_box_frame, height=9, exportselection=False)
        dup_scroll = ttk.Scrollbar(dup_box_frame, orient="vertical", command=self.others_list.yview)
        self.others_list.configure(yscrollcommand=dup_scroll.set)
        self.others_list.pack(side="left", fill="both", expand=True)
        dup_scroll.pack(side="right", fill="y")

        split.add(left, weight=1)
        split.add(right, weight=1)

        # События списков/таблиц
        self.keep_tree.bind("<Double-Button-1>", lambda e: self.open_selected_keep_file())
        self.keep_tree.bind("<<TreeviewSelect>>", self.on_select_keep)
        self.others_list.bind("<Double-Button-1>", lambda e: self.open_selected_duplicate())
        self.others_list.bind("<<ListboxSelect>>", self.on_select_duplicate)

        # Действия
        actions = ttk.Frame(self, padding=(10, 0, 10, 10))
        actions.pack(fill="x")
        self.btn_open_keep_file = ttk.Button(actions, text="Открыть выбранный оригинал",
                                             command=self.open_selected_keep_file, state="disabled")
        self.btn_open_keep_folder = ttk.Button(actions, text="Папка выбранного оригинала",
                                               command=self.open_selected_keep_folder, state="disabled")
        self.btn_open_dup_file = ttk.Button(actions, text="Открыть выбранную копию",
                                            command=self.open_selected_duplicate, state="disabled")
        self.btn_open_dup_folder = ttk.Button(actions, text="Папка выбранной копии",
                                              command=self.open_selected_duplicate_folder, state="disabled")
        self.btn_resolve_selected = ttk.Button(actions, text="Разобрать выбранную группу (в таблице)",
                                               command=self.resolve_selected, state="disabled")
        self.btn_open_quarantine = ttk.Button(actions, text="Открыть карантин",
                                              command=self.open_quarantine)
        self.btn_undo_last = ttk.Button(actions, text="Откатить последний батч",
                                        command=self.undo_last_batch)
        self.btn_export = ttk.Button(actions, text="Экспорт отчёта CSV…",
                                     command=self.export_csv)

        self.btn_open_keep_file.pack(side="left")
        self.btn_open_keep_folder.pack(side="left", padx=6)
        self.btn_open_dup_file.pack(side="left", padx=6)
        self.btn_open_dup_folder.pack(side="left")
        self.btn_resolve_selected.pack(side="left", padx=12)
        self.btn_open_quarantine.pack(side="left", padx=6)
        self.btn_undo_last.pack(side="left", padx=6)
        self.btn_export.pack(side="right")

        # Подсказка
        hint = (
            "Правило: из одинаковых файлов (совпадают имя и SHA-256) оставить копию в папке, созданной раньше. "
            "Остальные — в Карантин/удалить. Перцептивные — для ‘похожих’ изображений/видео."
        )
        ttk.Label(self, text=hint, padding=(10, 0, 10, 10)).pack(anchor="w")

    # --- Обработчики верхних контролов ---
    def choose_root(self):
        path = filedialog.askdirectory(title="Выберите корневую папку (ваш внешний диск)")
        if path:
            self.root_folder_var.set(path)

    def choose_quarantine(self):
        path = filedialog.askdirectory(title="Выберите папку карантина")
        if path:
            self.quarantine_root_var.set(path)

    def _parse_csv_list(self, s: str) -> List[str]:
        return [x.strip() for x in s.split(',') if x.strip()]

    def start_scan(self):
        root = self.root_folder_var.get().strip()
        if not root:
            messagebox.showwarning("Не выбрана папка", "Выберите папку для сканирования.")
            return
        if not os.path.isdir(root):
            messagebox.showerror("Ошибка", "Папка не существует.")
            return
        if self.scanner and self.scanner.is_alive():
            messagebox.showinfo("Выполняется", "Сканирование уже идёт.")
            return

        # Сброс
        self.groups.clear()
        self.keep_to_dups.clear()
        self.dup_total_bytes = 0
        self.counted_dups.clear()
        self.total_waste_var.set("0 Б")

        for iid in self.tree.get_children():
            self.tree.delete(iid)
        for iid in self.keep_tree.get_children():
            self.keep_tree.delete(iid)
        self.others_list.delete(0, tk.END)

        self.btn_auto.config(state="disabled")
        self.btn_open_keep_file.config(state="disabled")
        self.btn_open_keep_folder.config(state="disabled")
        self.btn_open_dup_file.config(state="disabled")
        self.btn_open_dup_folder.config(state="disabled")
        self.btn_resolve_selected.config(state="disabled")

        self.stop_event.clear()
        self.btn_scan.config(state="disabled")
        self.btn_stop.config(state="normal")
        self.progress.config(value=0, maximum=100)
        self.status_var.set("Подготовка к сканированию…")

        include_masks = self._parse_csv_list(self.include_masks_var.get() or "*")
        exclude_masks = self._parse_csv_list(self.exclude_masks_var.get())
        include_exts = self._parse_csv_list(self.include_exts_var.get())
        exclude_exts = self._parse_csv_list(self.exclude_exts_var.get())
        min_size_b = int(max(0.0, self.min_size_mb_var.get()) * 1024 * 1024)
        workers = max(1, int(self.workers_var.get()))

        def on_progress(msg):
            kind = msg[0]
            if kind == "start":
                total = max(0, msg[1])
                self.progress.config(maximum=100, value=0)
                self.status_var.set(f"Хеширование кандидатов: 0 / {total} (0%)")
            elif kind == "progress":
                done, total = msg[1], msg[2]
                percent = 100 if total <= 0 else int((done / max(1, total)) * 100)
                percent = 0 if percent < 0 else (100 if percent > 100 else percent)
                self.progress.config(value=percent, maximum=100)
                self.status_var.set(f"Хеширование кандидатов: {done} / {total} ({percent}%)")

        def on_group(group: dict):
            # Сохраняем группу
            self.groups.append(group)
            gindex = len(self.groups)  # 1..N для колонки №
            iid = str(gindex - 1)

            # В таблицу сканирования
            kind = "точн." if group.get("kind") == "exact" else "похож."
            self.tree.insert("", "end", iid=iid,
                             values=(gindex, kind, human_size(group["size"]), group["keep"], len(group["others"])))

            # В таблицу оригиналов (№ + путь)
            self.keep_tree.insert("", "end", values=(gindex, group["keep"]))

            # Карта соответствий keep -> dups
            self.keep_to_dups.setdefault(group["keep"], [])
            for p in group["others"]:
                self.keep_to_dups[group["keep"]].append(p)

            # СРАЗУ добавить пути копий в агрегированный список
            for p in group["others"]:
                self.others_list.insert(tk.END, p)

            # --- обновление суммарного объёма дубликатов (без двойного учёта)
            add_bytes = 0
            for p in group["others"]:
                if p in self.counted_dups:
                    continue
                try:
                    add_bytes += os.path.getsize(p)
                    self.counted_dups.add(p)
                except Exception:
                    pass
            if add_bytes:
                self.dup_total_bytes += add_bytes
                self.total_waste_var.set(human_size(self.dup_total_bytes))

        def on_done(elapsed):
            self.progress.config(value=100, maximum=100)
            self.btn_scan.config(state="normal")
            self.btn_stop.config(state="disabled")
            self.btn_auto.config(state="normal" if self.groups else "disabled")
            suffix = " (перцептивные недоступны: нет зависимостей)" if self.perceptual_var.get() and not HAS_IMAGEHASH else ""
            self.status_var.set(
                f"Готово за {elapsed:.1f} сек. Найдено групп: {len(self.groups)}. Сумма копий: {self.total_waste_var.get()}.{suffix}"
            )

        # --- запуск сканера ---
        self.scanner = Scanner(
            root_folder=root,
            stop_event=self.stop_event,
            on_progress=on_progress,
            on_group=on_group,
            on_done=on_done,
            include_masks=include_masks,
            exclude_masks=exclude_masks,
            include_exts=include_exts,
            exclude_exts=exclude_exts,
            min_size_bytes=min_size_b,
            perceptual=self.perceptual_var.get(),
            perceptual_metric=self.perc_metric_var.get(),
            perceptual_threshold=int(self.perc_thr_var.get()),
            max_workers=workers,
        )
        self.scanner.start()

    def stop_scan(self):
        self.stop_event.set()
        self.status_var.set("Остановка…")

    # Выбор строки в таблице — нужен для кнопки «Разобрать выбранную группу»
    def on_table_select(self, event=None):
        sel = self.tree.selection()
        self.btn_resolve_selected.config(state="normal" if sel else "disabled")

    # --- Карантин / логирование / откат ---
    def ensure_quarantine_batch(self) -> str:
        root = self.quarantine_root_var.get().strip() or DEFAULT_QUARANTINE_ROOT
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        dest = os.path.join(root, ts)
        os.makedirs(dest, exist_ok=True)
        with open(os.path.join(dest, "operations.csv"), "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["src", "dest", "kind", "hash", "size", "timestamp"])
        with open(os.path.join(dest, "actions.log"), "a", encoding="utf-8") as f:
            f.write(f"Batch started at {ts}\n")
        self.last_batch_dir = dest
        return dest

    def log_ops(self, batch_dir: str, ops: List[Tuple[str, str, str, str, int]]):
        if not batch_dir:
            return
        csv_path = os.path.join(batch_dir, "operations.csv")
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            now = datetime.now().isoformat(timespec="seconds")
            for src, dest, kind, hv, sz in ops:
                w.writerow([src, dest, kind, hv, sz, now])
        log_path = os.path.join(batch_dir, "actions.log")
        with open(log_path, "a", encoding="utf-8") as f:
            for src, dest, kind, hv, sz in ops:
                f.write(f"{kind}: {src} -> {dest} | {hv} | {sz} bytes\n")

    def move_to_quarantine(self, src_path: str, quarantine_root: str, scan_root: str) -> str:
        rel = safe_relpath(src_path, scan_root)
        dest_path = os.path.join(quarantine_root, rel)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        base, ext = os.path.splitext(dest_path)
        final = dest_path
        n = 1
        while os.path.exists(final):
            final = f"{base} ({n}){ext}"
            n += 1
        shutil.move(src_path, final)
        return final

    def resolve_group(self, group: dict, quarantine_root: Optional[str], delete_instead: bool, scan_root: str) -> List[Tuple[str, str, str, str, int]]:
        ops = []
        for p in group["others"]:
            try:
                sz = os.path.getsize(p)
            except Exception:
                sz = 0
            if delete_instead:
                try:
                    os.remove(p)
                    ops.append((p, "DELETED", group.get("kind", "exact"), group.get("hash", ""), sz))
                except Exception as e:
                    ops.append((p, f"ERROR:{e}", group.get("kind", "exact"), group.get("hash", ""), sz))
            else:
                try:
                    dest = self.move_to_quarantine(p, quarantine_root, scan_root)
                    ops.append((p, dest, group.get("kind", "exact"), group.get("hash", ""), sz))
                except Exception as e:
                    ops.append((p, f"ERROR:{e}", group.get("kind", "exact"), group.get("hash", ""), sz))
        return ops

    def auto_resolve_all(self):
        if not self.groups:
            messagebox.showinfo("Нет групп", "Сначала выполните сканирование и найдите дубликаты.")
            return
        delete_instead = self.delete_instead_var.get()
        if delete_instead:
            if not messagebox.askyesno("Подтвердите удаление", "Все копии будут удалены без возможности восстановления. Продолжить?"):
                return
        quarantine_root = None if delete_instead else self.ensure_quarantine_batch()
        scan_root = self.root_folder_var.get().strip()

        total_files = sum(len(g["others"]) for g in self.groups)
        removed = 0
        all_ops = []
        for g in self.groups:
            ops = self.resolve_group(g, quarantine_root, delete_instead, scan_root)
            removed += sum(1 for _, d, _, _, _ in ops if not str(d).startswith("ERROR"))
            all_ops.extend(ops)
        if not delete_instead:
            self.log_ops(quarantine_root, all_ops)
        self.status_var.set(f"Готово: убрано копий {removed} из {total_files}.")
        messagebox.showinfo("Завершено", f"Убрано копий: {removed} из {total_files}.")

    def resolve_selected(self):
        sel = self.tree.selection()
        if not sel:
            return
        idx = int(sel[0])
        group = self.groups[idx]
        if not group["others"]:
            messagebox.showinfo("Нет копий", "В выбранной группе нет копий для удаления/перемещения.")
            return
        delete_instead = self.delete_instead_var.get()
        if delete_instead:
            if not messagebox.askyesno("Подтвердите удаление", "Копии будут удалены без возможности восстановления. Продолжить?"):
                return
        quarantine_root = None if delete_instead else self.ensure_quarantine_batch()
        scan_root = self.root_folder_var.get().strip()
        ops = self.resolve_group(group, quarantine_root, delete_instead, scan_root)
        ok = sum(1 for _, d, _, _, _ in ops if not str(d).startswith("ERROR"))
        if not delete_instead:
            self.log_ops(quarantine_root, ops)
        self.status_var.set(f"Группа разобрана. Убрано копий: {ok} / {len(group['others'])}.")
        messagebox.showinfo("Готово", f"Убрано копий: {ok} / {len(group['others'])}.")

    def export_csv(self):
        if not self.groups:
            messagebox.showinfo("Нет данных", "Сначала выполните сканирование.")
            return
        path = filedialog.asksaveasfilename(title="Сохранить отчёт CSV",
                                            defaultextension=".csv",
                                            filetypes=[("CSV", ".csv")],
                                            initialfile=f"duplicates_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["type", "hash", "size_bytes", "keep_path", "others_count", "other_path"])
                for g in self.groups:
                    kind = g.get("kind", "exact")
                    if g["others"]:
                        for other in g["others"]:
                            w.writerow([kind, g.get("hash", ""), g["size"], g["keep"], len(g["others"]), other])
                    else:
                        w.writerow([kind, g.get("hash", ""), g["size"], g["keep"], 0, ""])
            messagebox.showinfo("Сохранено", f"Отчёт сохранён: {path}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сохранить отчёт: {e}")

    def open_quarantine(self):
        path = self.quarantine_root_var.get().strip() or DEFAULT_QUARANTINE_ROOT
        os.makedirs(path, exist_ok=True)
        self._reveal_in_explorer(path)

    def undo_last_batch(self):
        default_dir = self.last_batch_dir or (self.quarantine_root_var.get().strip() or DEFAULT_QUARANTINE_ROOT)
        start_dir = default_dir if os.path.isdir(default_dir) else None
        batch_dir = filedialog.askdirectory(title="Выберите папку батча для отката (внутри Карантина)", initialdir=start_dir)
        if not batch_dir:
            return
        csv_path = os.path.join(batch_dir, "operations.csv")
        if not os.path.isfile(csv_path):
            messagebox.showerror("Нет журнала", "В выбранной папке нет operations.csv — нечего откатывать.")
            return
        restored, errors = 0, 0
        with open(csv_path, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            rows = list(r)
        for row in rows:
            src = row.get("src")
            dest = row.get("dest")
            if dest and dest not in ("DELETED", "") and os.path.exists(dest):
                try:
                    os.makedirs(os.path.dirname(src), exist_ok=True)
                    base, ext = os.path.splitext(src)
                    final = src
                    n = 1
                    while os.path.exists(final):
                        final = f"{base} (restored {n}){ext}"
                        n += 1
                    shutil.move(dest, final)
                    restored += 1
                except Exception:
                    errors += 1
        messagebox.showinfo("Откат завершён", f"Восстановлено: {restored}. Ошибок: {errors}.")

    # --- связка «Оригиналы» -> «Копии» ---
    def _get_selected_keep_path(self) -> Optional[str]:
        sel = self.keep_tree.selection()
        if not sel:
            return None
        vals = self.keep_tree.item(sel[0], "values")
        if not vals:
            return None
        # columns: kidx, kpath
        return vals[1]

    def on_select_keep(self, event=None):
        keep_path = self._get_selected_keep_path()
        has_sel = keep_path is not None
        self.btn_open_keep_file.config(state="normal" if has_sel else "disabled")
        self.btn_open_keep_folder.config(state="normal" if has_sel else "disabled")

        self.others_list.selection_clear(0, tk.END)
        if not has_sel:
            self.btn_open_dup_file.config(state="disabled")
            self.btn_open_dup_folder.config(state="disabled")
            return

        dups = set(self.keep_to_dups.get(keep_path, []))
        if not dups:
            self.btn_open_dup_file.config(state="disabled")
            self.btn_open_dup_folder.config(state="disabled")
            return

        first_idx = None
        for i in range(self.others_list.size()):
            if self.others_list.get(i) in dups:
                self.others_list.selection_set(i)
                if first_idx is None:
                    first_idx = i
        if first_idx is not None:
            self.others_list.see(first_idx)
            self.btn_open_dup_file.config(state="normal")
            self.btn_open_dup_folder.config(state="normal")

    # --- действия со списком оригиналов ---
    def open_selected_keep_file(self):
        path = self._get_selected_keep_path()
        if not path:
            return
        self._open_path(path)

    def open_selected_keep_folder(self):
        path = self._get_selected_keep_path()
        if not path:
            return
        self._reveal_in_explorer(os.path.dirname(path))

    # --- действия со списком копий ---
    def on_select_duplicate(self, event=None):
        has_sel = bool(self.others_list.curselection())
        self.btn_open_dup_file.config(state="normal" if has_sel else "disabled")
        self.btn_open_dup_folder.config(state="normal" if has_sel else "disabled")

    def open_selected_duplicate(self):
        sel_idx = self.others_list.curselection()
        if not sel_idx:
            return
        path = self.others_list.get(sel_idx[0])
        self._open_path(path)

    def open_selected_duplicate_folder(self):
        sel_idx = self.others_list.curselection()
        if not sel_idx:
            return
        path = self.others_list.get(sel_idx[0])
        self._reveal_in_explorer(os.path.dirname(path))

    # --- helpers ---
    def _reveal_in_explorer(self, path: str):
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", path])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", path])
        except Exception:
            pass

    def _open_path(self, path: str):
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)
            elif sys.platform == "darwin":
                import subprocess
                subprocess.Popen(["open", path])
            else:
                import subprocess
                subprocess.Popen(["xdg-open", path])
        except Exception:
            pass


def main():
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
