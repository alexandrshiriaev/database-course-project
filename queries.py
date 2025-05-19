SIMPLE_QUERIES = [
    {
        "title": "Книги и отделы (по отделу)",
        "sql": '''
            SELECT b.title, d.name AS department, bii.receipt_date
            FROM books_in_inventory bii
            JOIN books b ON bii.book_id = b.isbn
            JOIN departments d ON bii.department_id = d.id
            WHERE d.name = %s
        ''',
        "columns": ["title", "department", "receipt_date"],
        "headers": ["Название книги", "Отдел", "Дата поступления"],
        "params": [
            {"name": "department", "label": "Отдел", "type": "select", "source": "departments", "field": "name"}
        ]
    },
    {
        "title": "Студенты и факультеты (по факультету)",
        "sql": '''
            SELECT s.first_name, s.last_name, f.name AS faculty
            FROM students s
            JOIN faculties f ON s.faculty_id = f.id
            WHERE f.name = %s
        ''',
        "columns": ["first_name", "last_name", "faculty"],
        "headers": ["Имя", "Фамилия", "Факультет"],
        "params": [
            {"name": "faculty", "label": "Факультет", "type": "select", "source": "faculties", "field": "name"}
        ]
    },
    {
        "title": "Книги, выданные после даты",
        "sql": '''
            SELECT s.first_name, s.last_name, b.title, sb.issue_date
            FROM students_books sb
            JOIN students s ON sb.student_id = s.id
            JOIN books_in_inventory bii ON sb.book_in_inventory_id = bii.inventory_id
            JOIN books b ON bii.book_id = b.isbn
            WHERE sb.issue_date > %s
        ''',
        "columns": ["first_name", "last_name", "title", "issue_date"],
        "headers": ["Имя", "Фамилия", "Книга", "Дата выдачи"],
        "params": [
            {"name": "date_after", "label": "Дата после", "type": "date"}
        ]
    },
    {
        "title": "Книги, выданные до даты",
        "sql": '''
            SELECT s.first_name, s.last_name, b.title, sb.issue_date
            FROM students_books sb
            JOIN students s ON sb.student_id = s.id
            JOIN books_in_inventory bii ON sb.book_in_inventory_id = bii.inventory_id
            JOIN books b ON bii.book_id = b.isbn
            WHERE sb.issue_date < %s
        ''',
        "columns": ["first_name", "last_name", "title", "issue_date"],
        "headers": ["Имя", "Фамилия", "Книга", "Дата выдачи"],
        "params": [
            {"name": "date_before", "label": "Дата до", "type": "date"}
        ]
    },
    {
        "title": "Все книги и их отделы (без условия)",
        "sql": '''
            SELECT b.title, d.name AS department
            FROM books_in_inventory bii
            JOIN books b ON bii.book_id = b.isbn
            JOIN departments d ON bii.department_id = d.id
        ''',
        "columns": ["title", "department"],
        "headers": ["Название книги", "Отдел"]
    },
    {
        "title": "Все студенты и их факультеты (без условия)",
        "sql": '''
            SELECT s.first_name, s.last_name, f.name AS faculty
            FROM students s
            JOIN faculties f ON s.faculty_id = f.id
        ''',
        "columns": ["first_name", "last_name", "faculty"],
        "headers": ["Имя", "Фамилия", "Факультет"]
    },
    {
        "title": "Все книги и авторы (без условия)",
        "sql": '''
            SELECT b.title, a.first_name, a.last_name
            FROM books_authors ba
            JOIN books b ON ba.book_id = b.isbn
            JOIN authors a ON ba.author_id = a.id
        ''',
        "columns": ["title", "first_name", "last_name"],
        "headers": ["Название книги", "Имя автора", "Фамилия автора"]
    },
    {
        "title": "Левое соединение: все студенты и факультеты (включая без факультета)",
        "sql": '''
            SELECT s.first_name, s.last_name, f.name AS faculty
            FROM students s
            LEFT JOIN faculties f ON s.faculty_id = f.id
        ''',
        "columns": ["first_name", "last_name", "faculty"],
        "headers": ["Имя", "Фамилия", "Факультет"]
    },
    {
        "title": "Правое соединение: все факультеты и студенты (включая без студентов)",
        "sql": '''
            SELECT f.name AS faculty, s.first_name, s.last_name
            FROM students s
            RIGHT JOIN faculties f ON s.faculty_id = f.id
        ''',
        "columns": ["faculty", "first_name", "last_name"],
        "headers": ["Факультет", "Имя", "Фамилия"]
    },
    {
        "title": "Запрос на запросе: студенты с книгами (левое соединение)",
        "sql": '''
            SELECT s.first_name, s.last_name, sub.title
            FROM students s
            LEFT JOIN (
                SELECT sb.student_id, b.title
                FROM students_books sb
                JOIN books_in_inventory bii ON sb.book_in_inventory_id = bii.inventory_id
                JOIN books b ON bii.book_id = b.isbn
            ) AS sub ON s.id = sub.student_id
        ''',
        "columns": ["first_name", "last_name", "title"],
        "headers": ["Имя", "Фамилия", "Книга"]
    },
]

AGG_QUERIES = [
    {
        "title": "Общее количество книг в библиотеке (без условия)",
        "sql": '''
            SELECT COUNT(*) AS total_books
            FROM books_in_inventory
        ''',
        "columns": ["total_books"],
        "headers": ["Всего книг"]
    },
    {
        "title": "Количество книг на русском языке",
        "sql": '''
            SELECT COUNT(*) AS russian_books
            FROM books
            WHERE language = 'русский'
        ''',
        "columns": ["russian_books"],
        "headers": ["Книг на русском"]
    },
    {
        "title": "Количество студентов по факультетам",
        "sql": '''
            SELECT f.name AS faculty, COUNT(s.id) AS student_count
            FROM faculties f
            LEFT JOIN students s ON s.faculty_id = f.id
            GROUP BY f.name
        ''',
        "columns": ["faculty", "student_count"],
        "headers": ["Факультет", "Количество студентов"]
    },
    {
        "title": "Количество книг по отделам (только с даты)",
        "sql": '''
            SELECT d.name AS department, COUNT(bii.inventory_id) AS books_count
            FROM departments d
            LEFT JOIN books_in_inventory bii ON bii.department_id = d.id AND bii.receipt_date >= %s
            GROUP BY d.name
        ''',
        "columns": ["department", "books_count"],
        "headers": ["Отдел", "Книг с даты"],
        "params": [
            {"name": "date_from", "label": "Дата с", "type": "date"}
        ]
    },
    {
        "title": "Среднее количество книг на студента (итоговый запрос на запросе)",
        "sql": '''
            SELECT AVG(book_count) AS avg_books_per_student
            FROM (
                SELECT COUNT(sb.book_in_inventory_id) AS book_count
                FROM students_books sb
                GROUP BY sb.student_id
            ) AS sub
        ''',
        "columns": ["avg_books_per_student"],
        "headers": ["Среднее книг на студента"]
    },
    {
        "title": "Студенты, взявшие больше одной книги (подзапрос)",
        "sql": '''
            SELECT s.first_name, s.last_name, cnt
            FROM (
                SELECT student_id, COUNT(*) AS cnt
                FROM students_books
                GROUP BY student_id
                HAVING COUNT(*) > 1
            ) AS sub
            JOIN students s ON s.id = sub.student_id
        ''',
        "columns": ["first_name", "last_name", "cnt"],
        "headers": ["Имя", "Фамилия", "Кол-во книг"]
    },
]

COMPLEX_QUERIES = [
    {
        "title": "Топ-5 самых раритетных изданий по каждому отделу и по всей библиотеке",
        "sql": '''
            WITH ranked_books AS (
                SELECT
                    d.name AS department,
                    b.title,
                    b.publication_year,
                    ROW_NUMBER() OVER (PARTITION BY d.name ORDER BY b.publication_year ASC NULLS LAST) AS rn
                FROM books_in_inventory bii
                JOIN books b ON bii.book_id = b.isbn
                JOIN departments d ON bii.department_id = d.id
            )
            SELECT department, title, publication_year
            FROM ranked_books
            WHERE rn <= 5
            UNION ALL
            SELECT 'Вся библиотека' AS department, b.title, b.publication_year
            FROM books b
            ORDER BY department, publication_year
        ''',
        "columns": ["department", "title", "publication_year"],
        "headers": ["Отдел/Вся библиотека", "Название книги", "Год издания"]
    },
    {
        "title": "Средний срок пребывания книги у читателя по отделам и по ВУЗу",
        "sql": '''
            WITH book_days AS (
                SELECT
                    d.name AS department,
                    (COALESCE(sb.return_date, CURRENT_DATE) - sb.issue_date)::int AS days
                FROM students_books sb
                JOIN books_in_inventory bii ON sb.book_in_inventory_id = bii.inventory_id
                JOIN departments d ON bii.department_id = d.id
            )
            SELECT department, ROUND(AVG(days), 1) AS avg_days
            FROM book_days
            GROUP BY department
            UNION ALL
            SELECT 'Весь ВУЗ' AS department, ROUND(AVG(days), 1) AS avg_days
            FROM book_days
            ORDER BY department
        ''',
        "columns": ["department", "avg_days"],
        "headers": ["Отдел/Весь ВУЗ", "Средний срок (дней)"]
    },
    {
        "title": "Стоимость и количество книг, поступивших в отдел за период",
        "sql": '''
            SELECT
                d.name AS department,
                COUNT(bii.inventory_id) AS books_count,
                COALESCE(SUM(b.price), 0) AS total_price
            FROM departments d
            LEFT JOIN books_in_inventory bii ON bii.department_id = d.id
            LEFT JOIN books b ON bii.book_id = b.isbn
            WHERE bii.receipt_date BETWEEN %s AND %s
            GROUP BY d.name
            ORDER BY d.name
        ''',
        "columns": ["department", "books_count", "total_price"],
        "headers": ["Отдел", "Кол-во книг", "Суммарная стоимость"],
        "params": [
            {"name": "date_from", "label": "Дата с", "type": "date"},
            {"name": "date_to", "label": "Дата по", "type": "date"}
        ]
    },
] 