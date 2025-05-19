import random, datetime as dt, mimetypes
from pathlib import Path
import customtkinter as ctk
from tkinter import ttk, messagebox, filedialog
from tkcalendar import DateEntry
from faker import Faker
import psycopg2
from psycopg2 import sql
import queries

from config import DB_CONFIG

TABLE_META = {
    "files": {"title": "Файлы (BLOB)", "pk": "id", "columns": {"file_name": "Имя файла", "mime": "MIME‑тип", "file_data": "Данные файла"}, "fkeys": {}},
    "faculties": {"title": "Факультеты", "pk": "id", "columns": {"name": "Название", "groups": "Список групп"}, "fkeys": {}},
    "students": {"title": "Студенты", "pk": "id", "columns": {"library_card_id": "№ чит. билета", "first_name": "Имя", "middle_name": "Отчество", "last_name": "Фамилия", "address": "Адрес", "phone": "Телефон", "birth_date": "Дата рождения", "faculty_id": "Факультет", "speciality": "Специальность", "group_code": "Группа", "photo_id": "Фото"}, "fkeys": {"faculty_id": ("faculties", "name"), "photo_id": ("files", "file_name")}},
    "authors": {"title": "Авторы", "pk": "id", "columns": {"first_name": "Имя", "second_name": "Отчество", "last_name": "Фамилия", "birth_date": "Дата рождения", "birth_country": "Страна"}, "fkeys": {}},
    "departments": {"title": "Отделы / залы", "pk": "id", "columns": {"building": "Корпус", "name": "Название"}, "fkeys": {}},
    "books": {"title": "Книги", "pk": "isbn", "columns": {"title": "Название", "page_number": "Страниц", "publication_year": "Год издания", "publisher": "Издательство", "language": "Язык", "description": "Аннотация", "price": "Цена"}, "fkeys": {}},
    "books_in_inventory": {"title": "Экземпляры книг", "pk": "inventory_id", "columns": {"book_id": "ISBN", "department_id": "Отдел", "receipt_date": "Дата поступления", "version": "Тип"}, "fkeys": {"book_id": ("books", "title"), "department_id": ("departments", "name")}},
    "students_books": {"title": "Выдачи книг", "pk": None, "columns": {"student_id": "Студент", "book_in_inventory_id": "Экз. книги", "books_number": "Кол-во", "issue_date": "Дата выдачи", "return_date": "Дата возврата"}, "fkeys": {"student_id": ("students", "last_name"), "book_in_inventory_id": ("books_in_inventory", "inventory_id")}},
    "books_authors": {"title": "Книги—Авторы", "pk": None, "columns": {"book_id": "ISBN", "author_id": "Автор"}, "fkeys": {"book_id": ("books", "title"), "author_id": ("authors", "last_name")}}
}

class DB:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = True
            self.cur = self.conn.cursor()
        except Exception as e:
            messagebox.showerror("Ошибка подключения", f"Не удалось подключиться к базе данных: {str(e)}")
            raise

    def execute(self, q, p=None):
        try:
            self.cur.execute(q, p or [])
        except psycopg2.Error as e:
            messagebox.showerror("Ошибка SQL", str(e))
            raise

    def fetch(self, q, p=None):
        try:
            self.cur.execute(q, p or [])
            return self.cur.fetchall()
        except psycopg2.Error as e:
            messagebox.showerror("Ошибка SQL", str(e))
            raise

    def all_rows(self, table):
        try:
            pk = TABLE_META[table]["pk"] or list(TABLE_META[table]["columns"])[0]
            self.cur.execute(sql.SQL("SELECT * FROM {} ORDER BY {}").format(sql.Identifier(table), sql.Identifier(pk)))
            return self.cur.fetchall(), [d.name for d in self.cur.description]
        except psycopg2.Error as e:
            messagebox.showerror("Ошибка SQL", str(e))
            raise

db = DB()
fake = Faker("ru_RU")
Faker.seed(0)
random.seed(0)

def generate_all():
    for tbl in ("students_books", "books_authors", "books_in_inventory", "students", "books", "authors", "departments", "faculties", "files"):
        db.execute(sql.SQL("TRUNCATE {} CASCADE").format(sql.Identifier(tbl)))
    for _ in range(500):
        db.execute("INSERT INTO files(file_name,mime,file_data) VALUES (%s,%s,%s)", (fake.file_name(), "application/octet-stream", b"\x00"*10))
    for n in range(1, 501):
        db.execute("INSERT INTO faculties(name,groups) VALUES (%s,%s)", (f"Факультет №{n}", [f"ПИ‑{random.randint(20,25)}{chr(1072+random.randint(0,2))}"]))
    for _ in range(500):
        db.execute("INSERT INTO authors(first_name,second_name,last_name,birth_date,birth_country) VALUES (%s,%s,%s,%s,%s)", (fake.first_name(), fake.first_name(), fake.last_name(), fake.date_of_birth(minimum_age=25, maximum_age=80), fake.country()))
    for _ in range(500):
        db.execute("INSERT INTO departments(building,name) VALUES (%s,%s)", (f"Корпус {random.randint(1,5)}", f"Читальный зал «{fake.word().capitalize()}»"))
    for _ in range(500):
        db.execute("INSERT INTO books(isbn,title,page_number,publication_year,publisher,language,description,price) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (fake.isbn13(separator=""), fake.sentence(nb_words=4).rstrip("."), random.randint(50, 800), random.randint(1950, 2024), fake.company(), "русский", fake.text(max_nb_chars=180), random.randint(200, 3000)))
    book_isbns = [r[0] for r in db.fetch("SELECT isbn FROM books")]
    author_ids = [r[0] for r in db.fetch("SELECT id FROM authors")]
    for isbn in book_isbns:
        for aid in random.sample(author_ids, random.randint(1, 2)):
            db.execute("INSERT INTO books_authors(book_id,author_id) VALUES (%s,%s)", (isbn, aid))
    dept_ids = [r[0] for r in db.fetch("SELECT id FROM departments")]
    for idx, isbn in enumerate(book_isbns, 1):
        db.execute("INSERT INTO books_in_inventory(inventory_id,book_id,department_id,receipt_date,version) VALUES (%s,%s,%s,%s,%s)", (f"INV{idx:05}", isbn, random.choice(dept_ids), fake.date_between("-5y", "today"), random.choice(["PRINTED", "DIGITAL"])))
    faculty_ids = [r[0] for r in db.fetch("SELECT id FROM faculties")]
    photo_ids = [r[0] for r in db.fetch("SELECT id FROM files")]
    for n in range(1, 501):
        db.execute("INSERT INTO students(library_card_id,first_name,middle_name,last_name,address,phone,birth_date,faculty_id,speciality,group_code,photo_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (f"LC{n:05}", fake.first_name(), fake.first_name(), fake.last_name(), fake.address().replace("\n", ", "), "+7" + fake.msisdn()[:10], fake.date_of_birth(minimum_age=17, maximum_age=25), random.choice(faculty_ids), fake.word().capitalize(), random.choice(["ПИ‑23в", "ЭМ‑22б", "ФИ‑24а"]), random.choice(photo_ids)))
    student_ids = [r[0] for r in db.fetch("SELECT id FROM students")]
    inv_ids = [r[0] for r in db.fetch("SELECT inventory_id FROM books_in_inventory")]
    for _ in range(500):
        issue = fake.date_between("-2y", "today")
        ret = None if random.random() < 0.25 else issue + dt.timedelta(days=random.randint(1, 60))
        db.execute("INSERT INTO students_books(student_id,book_in_inventory_id,books_number,issue_date,return_date) VALUES (%s,%s,%s,%s,%s)", (random.choice(student_ids), random.choice(inv_ids), random.randint(1, 3), issue, ret))

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Библиотека университета")
        self.geometry("1120x660")
        ctk.set_default_color_theme("blue")
        ctk.set_appearance_mode("system")
        self.frames = {}
        for F in (MenuFrame, GenerateFrame, TablesFrame, QueriesFrame):
            frame = F(self)
            self.frames[F.__name__] = frame
            frame.place(relwidth=1, relheight=1)
        self.show("MenuFrame")
    def show(self, name):
        self.frames[name].tkraise()

class MenuFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master)
        inner = ctk.CTkFrame(self)
        inner.pack(expand=True)
        ctk.CTkLabel(inner, text="Библиотека университета\nШиряев Александр ПИ‑23в", font=("Arial", 26), justify="center").pack(pady=40)
        ctk.CTkButton(inner, text="Таблицы / CRUD", height=40, command=lambda: master.show("TablesFrame")).pack(fill="x", padx=200, pady=10)
        ctk.CTkButton(inner, text="Генерация тестовых данных", height=40, command=lambda: master.show("GenerateFrame")).pack(fill="x", padx=200, pady=10)
        ctk.CTkButton(inner, text="Запросы", height=40, command=lambda: master.show("QueriesFrame")).pack(fill="x", padx=200, pady=10)

class GenerateFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master)
        box = ctk.CTkFrame(self)
        box.pack(expand=True)
        ctk.CTkLabel(box, text="Генерация тестовых данных", font=("Arial", 22)).pack(pady=25)
        self.status = ctk.CTkLabel(box, text="")
        self.status.pack(pady=10)
        ctk.CTkButton(box, text="▶ Сгенерировать 500 записей во все таблицы", command=self.generate).pack(fill="x", padx=80, pady=10)
        ctk.CTkButton(box, text="На главное меню", command=lambda: master.show("MenuFrame")).pack(pady=10)
    def generate(self):
        try:
            generate_all()
            self.status.configure(text="✔ Данные успешно сгенерированы", text_color="green")
        except Exception as e:
            self.status.configure(text=f"Ошибка: {e}", text_color="red")

class TablesFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master)
        top = ctk.CTkFrame(self)
        top.pack(fill="x", padx=10, pady=6)
        ctk.CTkLabel(top, text="Таблица:").pack(side="left", padx=(0, 6))
        self.cbo = ctk.CTkOptionMenu(top, values=[m["title"] for m in TABLE_META.values()], command=self.load_table)
        self.cbo.pack(side="left")
        ctk.CTkButton(top, text="На главное меню", command=lambda: master.show("MenuFrame")).pack(side="right")
        tree_frame = ctk.CTkFrame(self)
        tree_frame.pack(expand=True, fill="both", padx=10, pady=4)
        self.tree = ttk.Treeview(tree_frame, show="headings", selectmode="browse")
        self.tree.pack(side="left", expand=True, fill="both")
        sb_y = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        sb_y.pack(side="right", fill="y")
        sb_x = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        sb_x.pack(fill="x")
        self.tree.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)
        bar = ctk.CTkFrame(self)
        bar.pack(fill="x", pady=6)
        for txt, cmd in (("Добавить", self.add), ("Изменить", self.edit), ("Удалить", self.delete)):
            ctk.CTkButton(bar, text=txt, command=cmd).pack(side="left", padx=4, fill="x", expand=True)
        self.current_key = None
        self.rows = []
        self.cols = []
        self.col_types = {}
    def load_table(self, human_title):
        for k, meta in TABLE_META.items():
            if meta["title"] == human_title:
                self.current_key = k
                break
        if not self.current_key:
            return
        self.rows, self.cols = db.all_rows(self.current_key)
        type_rows = db.fetch("SELECT column_name, data_type, udt_name FROM information_schema.columns WHERE table_name = %s", (self.current_key,))
        self.col_types = {c: (d, u) for c, d, u in type_rows}
        meta = TABLE_META[self.current_key]
        pk = meta["pk"]
        rus = meta["columns"]
        show_cols = [c for c in self.cols if c != pk]
        self.tree["columns"] = show_cols
        for c in show_cols:
            self.tree.heading(c, text=rus.get(c, c))
            self.tree.column(c, width=120, anchor="center")
        self.tree.delete(*self.tree.get_children())
        for r in self.rows:
            self.tree.insert("", "end", values=[v for (c, v) in zip(self.cols, r) if c != pk])
    def _editor(self, record=None):
        meta = TABLE_META[self.current_key]
        pk = meta["pk"]
        is_books = self.current_key == "books"
        is_students = self.current_key == "students"
        pk_is_serial = False
        if pk and not is_books:
            pk_type = db.fetch("SELECT data_type FROM information_schema.columns WHERE table_name = %s AND column_name = %s", (self.current_key, pk))
            if pk_type and pk_type[0][0] == 'integer':
                pk_is_serial = True
        if pk and not pk_is_serial and not is_books:
            cols = [c for c in self.cols]
        else:
            cols = [c for c in self.cols if c != pk or (is_books and c == pk and record is not None)]
        win = ctk.CTkToplevel(self)
        win.minsize(500, 350)
        win.geometry("500x350")
        scroll = ctk.CTkScrollableFrame(win, width=480, height=200)
        scroll.pack(expand=True, fill="both", padx=22, pady=22)
        widgets = {}
        photo_btn = None
        photo_id_val = None
        for col in cols:
            ctk.CTkLabel(scroll, text=meta["columns"].get(col, col)).pack(anchor="w")
            if col in meta["fkeys"]:
                ref_tbl, ref_disp = meta["fkeys"][col]
                ref_pk = TABLE_META[ref_tbl]["pk"]
                rows = db.fetch(sql.SQL("SELECT {}, {} FROM {} ORDER BY {}"\
                ).format(sql.Identifier(ref_pk), sql.Identifier(ref_disp), sql.Identifier(ref_tbl), sql.Identifier(ref_disp)))
                mapping = {f"{name} (id={rid})": rid for rid, name in rows}
                cb = ttk.Combobox(scroll, values=list(mapping), state="readonly")
                cb.pack(fill="x", pady=3)
                widgets[col] = ("fk", cb, mapping)
                if is_students and col == "photo_id":
                    def show_photo():
                        val = None
                        for k, v in mapping.items():
                            if cb.get() == k:
                                val = v
                        if not val:
                            if record:
                                val = record[self.cols.index("photo_id")]
                        if not val:
                            messagebox.showinfo("Фото", "Фото не выбрано")
                            return
                        res = db.fetch("SELECT file_data, file_name, mime FROM files WHERE id = %s", (val,))
                        if not res:
                            messagebox.showinfo("Фото", "Файл не найден")
                            return
                        file_data, file_name, mime = res[0]
                        import tempfile, os
                        ext = os.path.splitext(file_name)[-1] or ".bin"
                        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp:
                            tmp.write(file_data)
                            tmp_path = tmp.name
                        os.startfile(tmp_path)
                    photo_btn = ctk.CTkButton(scroll, text="Посмотреть фото", command=show_photo)
                    photo_btn.pack(fill="x", pady=3)
            elif self.col_types[col][0] == "ARRAY":
                ent = ctk.CTkEntry(scroll, placeholder_text="значения через запятую")
                ent.pack(fill="x", pady=3)
                widgets[col] = ("array", ent)
            elif col.endswith("_date"):
                de = DateEntry(scroll, date_pattern="yyyy-mm-dd")
                de.pack(fill="x", pady=3)
                widgets[col] = ("date", de)
            elif self.current_key == "files" and col == "file_data":
                holder = {"bytes": None}
                def choose_file():
                    path = filedialog.askopenfilename(title="Выбрать файл")
                    if not path:
                        return
                    with open(path, "rb") as f:
                        holder["bytes"] = f.read()
                    btn.configure(text=f"✓ {Path(path).name}")
                    if "file_name" in widgets and widgets["file_name"][0] == "text":
                        w = widgets["file_name"][1]
                        if not w.get().strip():
                            w.insert(0, Path(path).name)
                    if "mime" in widgets and widgets["mime"][0] == "text":
                        w = widgets["mime"][1]
                        if not w.get().strip():
                            w.insert(0, mimetypes.guess_type(path)[0] or "application/octet-stream")
                btn = ctk.CTkButton(scroll, text="Выбрать файл…", command=choose_file)
                btn.pack(fill="x", pady=3)
                widgets[col] = ("blob", holder)
            else:
                ent = ctk.CTkEntry(scroll)
                ent.pack(fill="x", pady=3)
                widgets[col] = ("text", ent)
        if record:
            d = dict(zip(self.cols, record))
            for col in cols:
                kind, *rest = widgets[col]
                val = d[col]
                if kind == "fk":
                    cb, mp = rest
                    for k, v in mp.items():
                        if v == val:
                            cb.set(k)
                            break
                elif kind == "array":
                    rest[0].insert(0, ", ".join(val or []))
                elif kind == "date":
                    if val:
                        rest[0].set_date(val)
                elif kind == "text":
                    rest[0].insert(0, str(val) if val is not None else "")
        def save():
            fields = []
            values = []
            if is_books and record is None:
                import faker
                fake = faker.Faker()
                isbn = fake.isbn13(separator="")
                fields.append("isbn")
                values.append(isbn)
            for col in cols:
                kind, *rest = widgets[col]
                if kind == "fk":
                    cb, mp = rest
                    if cb.get() not in mp:
                        messagebox.showwarning("Важно", f"Заполните поле «{meta['columns'][col]}»")
                        return
                    fields.append(col)
                    values.append(mp[cb.get()])
                elif kind == "array":
                    arr = [s.strip() for s in rest[0].get().strip().split(",") if s.strip()]
                    fields.append(col)
                    values.append(arr if arr else None)
                elif kind == "date":
                    fields.append(col)
                    values.append(rest[0].get_date())
                elif kind == "blob":
                    if not holder["bytes"]:
                        messagebox.showwarning("Важно", "Выберите файл")
                        return
                    fields.append(col)
                    values.append(holder["bytes"])
                else:
                    val = rest[0].get().strip()
                    if self.col_types[col][1] in ('isbn13', 'e164_phone') or 'NOT NULL' in str(self.col_types[col]):
                        if not val and not (is_books and col == "isbn"):
                            messagebox.showwarning("Важно", f"Поле «{meta['columns'][col]}» обязательно для заполнения")
                            return
                        if self.col_types[col][1] == 'isbn13' and not (is_books and record is None):
                            if not val.replace('-', '').replace(' ', '').isdigit() or len(val.replace('-', '').replace(' ', '')) != 13:
                                messagebox.showwarning("Ошибка", "ISBN должен содержать 13 цифр")
                                return
                        if self.col_types[col][1] == 'e164_phone':
                            if not val.startswith('+') or not val[1:].isdigit() or len(val) < 10:
                                messagebox.showwarning("Ошибка", "Телефон должен начинаться с + и содержать минимум 10 цифр")
                                return
                        if self.col_types[col][1] == 'positive_int':
                            try:
                                num = int(val)
                                if num <= 0:
                                    messagebox.showwarning("Ошибка", f"Поле «{meta['columns'][col]}» должно быть положительным числом")
                                    return
                            except ValueError:
                                messagebox.showwarning("Ошибка", f"Поле «{meta['columns'][col]}» должно быть числом")
                                return
                    fields.append(col)
                    values.append(val if val else None)
            try:
                if record:
                    pk_val = record[self.cols.index(pk)] if pk else None
                    set_clause = sql.SQL(", ").join(
                        sql.SQL("{} = {}").format(sql.Identifier(f), sql.Placeholder())
                        for f in fields if f != pk
                    )
                    db.execute(
                        sql.SQL("UPDATE {} SET {} WHERE {} = {}"\
                        ).format(
                            sql.Identifier(self.current_key),
                            set_clause,
                            sql.Identifier(pk),
                            sql.Placeholder()
                        ),
                        [v for f, v in zip(fields, values) if f != pk] + [pk_val]
                    )
                else:
                    db.execute(
                        sql.SQL("INSERT INTO {} ({}) VALUES ({})"\
                        ).format(
                            sql.Identifier(self.current_key),
                            sql.SQL(", ").join(map(sql.Identifier, fields)),
                            sql.SQL(", ").join(sql.Placeholder() * len(values))
                        ),
                        values
                    )
                win.destroy()
                self.load_table(meta["title"])
            except Exception as e:
                messagebox.showerror("Ошибка", str(e))
        ctk.CTkButton(scroll, text="Сохранить", command=save).pack(pady=12, fill="x")
    def add(self):
        self._editor()
    def edit(self):
        sel = self.tree.focus()
        if sel:
            self._editor(self.rows[self.tree.index(sel)])
    def delete(self):
        sel = self.tree.focus()
        if not sel:
            messagebox.showwarning("Предупреждение", "Выберите запись для удаления")
            return
        if not messagebox.askyesno("Подтвердите", "Удалить выбранную запись?"):
            return
        meta = TABLE_META[self.current_key]
        pk = meta["pk"]
        if pk is None:
            messagebox.showinfo("Нельзя", "Удаление для этой таблицы не реализовано")
            return
        try:
            rec = self.rows[self.tree.index(sel)]
            db.execute(sql.SQL("DELETE FROM {} WHERE {}=%s").format(sql.Identifier(self.current_key), sql.Identifier(pk)), (rec[self.cols.index(pk)],))
            self.load_table(meta["title"])
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось удалить запись: {str(e)}")

class QueriesFrame(ctk.CTkFrame):
    def __init__(self, master):
        super().__init__(master)
        self.tabs = ctk.CTkTabview(self)
        self.tabs.pack(expand=True, fill="both", padx=20, pady=20)
        self.simple_tab = self.tabs.add("Простые запросы")
        self.agg_tab = self.tabs.add("Итоговые запросы")
        self.complex_tab = self.tabs.add("Запросы из курсового")
        for q in queries.SIMPLE_QUERIES:
            ctk.CTkButton(self.simple_tab, text=q["title"], command=lambda q=q: self.prepare_query(q)).pack(fill="x", pady=4)
        for q in queries.AGG_QUERIES:
            ctk.CTkButton(self.agg_tab, text=q["title"], command=lambda q=q: self.prepare_query(q)).pack(fill="x", pady=4)
        for q in queries.COMPLEX_QUERIES:
            ctk.CTkButton(self.complex_tab, text=q["title"], command=lambda q=q: self.prepare_query(q)).pack(fill="x", pady=4)
        self.result_box = ctk.CTkTextbox(self, height=200)
        self.result_box.pack(fill="both", padx=20, pady=(0,20), expand=True)
        ctk.CTkButton(self, text="На главное меню", command=lambda: master.show("MenuFrame")).pack(pady=10)

    def prepare_query(self, q):
        if "params" not in q:
            self.run_query(q, [])
            return
        win = ctk.CTkToplevel(self)
        win.title("Параметры запроса")
        win.geometry("340x240")
        entries = {}
        for param in q["params"]:
            ctk.CTkLabel(win, text=param["label"]).pack(anchor="w", padx=10, pady=(10,0))
            if param["type"] == "date":
                de = DateEntry(win, date_pattern="yyyy-mm-dd")
                de.pack(fill="x", padx=10, pady=3)
                entries[param["name"]] = de
            elif param["type"] == "select":
                rows = db.fetch(f"SELECT {param['field']} FROM {param['source']} ORDER BY {param['field']}")
                values = [r[0] for r in rows]
                import tkinter as tk
                var = tk.StringVar()
                cb = ttk.Combobox(win, values=values, textvariable=var, state="readonly")
                cb.pack(fill="x", padx=10, pady=3)
                entries[param["name"]] = cb
            else:
                ent = ctk.CTkEntry(win)
                ent.pack(fill="x", padx=10, pady=3)
                entries[param["name"]] = ent
        def submit():
            values = []
            for param in q["params"]:
                widget = entries[param["name"]]
                if param["type"] == "date":
                    val = widget.get_date()
                elif param["type"] == "select":
                    val = widget.get()
                else:
                    val = widget.get().strip()
                if not val:
                    ctk.CTkLabel(win, text="Заполните все параметры!", text_color="red").pack()
                    return
                values.append(str(val))
            win.destroy()
            self.run_query(q, values)
        ctk.CTkButton(win, text="Выполнить", command=submit).pack(pady=10)

    def run_query(self, q, params=None):
        try:
            if params is None:
                params = []
            rows = db.fetch(q["sql"], params)
            cols = q["columns"]
            headers = q.get("headers", cols)
            self.result_box.delete("1.0", "end")
            if not rows:
                self.result_box.insert("end", "Нет данных")
                return
            show_cols = [c for c in cols if not c.lower().endswith('id')]
            show_headers = [h for c, h in zip(cols, headers) if not c.lower().endswith('id')]
            data = [[str(row[cols.index(c)]) for c in show_cols] for row in rows]
            col_widths = [max(len(str(h)), *(len(r[i]) for r in data)) for i, h in enumerate(show_headers)]
            fmt = " | ".join(f"{{:<{w}}}" for w in col_widths)
            self.result_box.insert("end", fmt.format(*show_headers) + "\n")
            self.result_box.insert("end", "-" * (sum(col_widths) + 3 * (len(col_widths)-1)) + "\n")
            for row in data:
                self.result_box.insert("end", fmt.format(*row) + "\n")
        except Exception as e:
            self.result_box.delete("1.0", "end")
            self.result_box.insert("end", f"Ошибка: {e}")

if __name__ == "__main__":
    App().mainloop()
