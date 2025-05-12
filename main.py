import random, datetime as dt, mimetypes
from pathlib import Path
import customtkinter as ctk
from tkinter import ttk, messagebox, filedialog
from tkcalendar import DateEntry
from faker import Faker
import psycopg2
from psycopg2 import sql

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
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
    def execute(self, q, p=None):
        self.cur.execute(q, p or [])
    def fetch(self, q, p=None):
        self.cur.execute(q, p or [])
        return self.cur.fetchall()
    def all_rows(self, table):
        pk = TABLE_META[table]["pk"] or list(TABLE_META[table]["columns"])[0]
        self.cur.execute(sql.SQL("SELECT * FROM {} ORDER BY {}").format(sql.Identifier(table), sql.Identifier(pk)))
        return self.cur.fetchall(), [d.name for d in self.cur.description]

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
        for F in (MenuFrame, GenerateFrame, TablesFrame):
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
        self.tree = ttk.Treeview(self, show="headings", selectmode="browse")
        self.tree.pack(expand=True, fill="both", padx=10, pady=4)
        sb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        bar = ctk.CTkFrame(self)
        bar.pack(fill="x", pady=6)
        for txt, cmd in (("Добавить", self.add), ("Изменить", self.edit), ("Удалить", self.delete), ("Обновить", lambda: self.load_table(self.cbo.get()))):
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
        cols = [c for c in self.cols if c != pk]
        win = ctk.CTkToplevel(self)
        win.minsize(500, 680)
        win.geometry("500x680")
        scroll = ctk.CTkScrollableFrame(win)
        scroll.pack(expand=True, fill="both", padx=22, pady=22)
        widgets = {}
        for col in cols:
            ctk.CTkLabel(scroll, text=meta["columns"].get(col, col)).pack(anchor="w")
            if col in meta["fkeys"]:
                ref_tbl, ref_disp = meta["fkeys"][col]
                ref_pk = TABLE_META[ref_tbl]["pk"]
                rows = db.fetch(sql.SQL("SELECT {}, {} FROM {} ORDER BY {}").format(sql.Identifier(ref_pk), sql.Identifier(ref_disp), sql.Identifier(ref_tbl), sql.Identifier(ref_disp)))
                mapping = {f"{name} (id={rid})": rid for rid, name in rows}
                cb = ttk.Combobox(scroll, values=list(mapping), state="readonly")
                cb.pack(fill="x", pady=3)
                widgets[col] = ("fk", cb, mapping)
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
                    data_holder = rest[0]
                    if data_holder["bytes"] is None:
                        messagebox.showwarning("Важно", "Не выбран файл для загрузки")
                        return
                    fields.append(col)
                    values.append(psycopg2.Binary(data_holder["bytes"]))
                else:
                    txt = rest[0].get().strip() or None
                    fields.append(col)
                    values.append(txt)
            if record:
                cond = sql.SQL(" WHERE {}=%s").format(sql.Identifier(pk))
                set_clause = sql.SQL(", ").join(sql.Composed([sql.Identifier(f), sql.SQL("=%s")]) for f in fields)
                db.execute(sql.SQL("UPDATE {} SET {}{}").format(sql.Identifier(self.current_key), set_clause, cond), values + [record[self.cols.index(pk)]])
            else:
                db.execute(sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(sql.Identifier(self.current_key), sql.SQL(", ").join(map(sql.Identifier, fields)), sql.SQL(", ").join(sql.Placeholder() * len(values))), values)
            win.destroy()
            self.load_table(meta["title"])
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
            return
        if not messagebox.askyesno("Подтвердите", "Удалить выбранную запись?"):
            return
        meta = TABLE_META[self.current_key]
        pk = meta["pk"]
        if pk is None:
            messagebox.showinfo("Нельзя", "Удаление для этой таблицы не реализовано")
            return
        rec = self.rows[self.tree.index(sel)]
        db.execute(sql.SQL("DELETE FROM {} WHERE {}=%s").format(sql.Identifier(self.current_key), sql.Identifier(pk)), (rec[self.cols.index(pk)],))
        self.load_table(meta["title"])

if __name__ == "__main__":
    App().mainloop()
