CREATE DOMAIN isbn13 AS varchar(17)
    CHECK (VALUE ~ '^(97[89])?\d{9}[\dX]$');

CREATE DOMAIN e164_phone AS varchar(15)
    CHECK (VALUE ~ '^\+\d{9,14}$');

CREATE DOMAIN positive_int AS integer
    CHECK (VALUE > 0);

CREATE TYPE book_version AS ENUM ('PRINTED', 'DIGITAL');

CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    file_name varchar(255),
    mime varchar(100),
    file_data bytea NOT NULL
);

CREATE TABLE faculties (
    id SERIAL PRIMARY KEY,
    name varchar(255) UNIQUE NOT NULL,
    groups text[]
);

CREATE TABLE students (
    id SERIAL PRIMARY KEY,
    library_card_id varchar(30) UNIQUE NOT NULL,
    first_name varchar(100) NOT NULL,
    middle_name varchar(100),
    last_name varchar(100) NOT NULL,
    address varchar(255),
    phone e164_phone,
    birth_date date,
    faculty_id int REFERENCES faculties(id) ON UPDATE CASCADE ON DELETE SET NULL,
    speciality varchar(255),
    group_code varchar(50),
    photo_id int REFERENCES files(id) ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    first_name varchar(100),
    second_name varchar(100),
    last_name varchar(100),
    birth_date date,
    birth_country varchar(100)
);

CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    building varchar(100) NOT NULL,
    name varchar(255) NOT NULL
);

CREATE TABLE books (
    isbn isbn13 PRIMARY KEY,
    title varchar(500) NOT NULL,
    page_number positive_int,
    publication_year int,
    publisher varchar(255),
    language varchar(50),
    description text,
    price int
);

CREATE TABLE books_in_inventory (
    inventory_id varchar(30) PRIMARY KEY,
    book_id isbn13 NOT NULL,
    department_id int,
    receipt_date date NOT NULL,
    version book_version NOT NULL,
    CONSTRAINT fk_bii_book FOREIGN KEY (book_id) REFERENCES books(isbn) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_bii_department FOREIGN KEY (department_id) REFERENCES departments(id) ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE students_books (
    student_id int NOT NULL,
    book_in_inventory_id varchar(30) NOT NULL,
    books_number positive_int NOT NULL,
    issue_date date NOT NULL,
    return_date date,
    PRIMARY KEY (student_id, book_in_inventory_id, issue_date),
    CONSTRAINT fk_sb_student FOREIGN KEY (student_id) REFERENCES students(id) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_sb_inventory FOREIGN KEY (book_in_inventory_id) REFERENCES books_in_inventory(inventory_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CHECK (return_date IS NULL OR return_date >= issue_date)
);

CREATE TABLE books_authors (
    book_id isbn13 NOT NULL,
    author_id int NOT NULL,
    PRIMARY KEY (book_id, author_id),
    FOREIGN KEY (book_id) REFERENCES books(isbn) ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (author_id) REFERENCES authors(id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE INDEX idx_books_title ON books USING gin (to_tsvector('simple', title));
CREATE INDEX idx_students_last_name ON students(last_name);
CREATE INDEX idx_inventory_department ON books_in_inventory(department_id);