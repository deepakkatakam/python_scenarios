from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pyodbc
import configparser

app = FastAPI()

# Read config
config = configparser.ConfigParser()
config.read("config.ini")

db_config = config["database"]

# Build connection string
conn_str = (
    f"DRIVER={db_config['driver']};"
    f"SERVER={db_config['server']};"
    f"DATABASE={db_config['database']};"
    f"UID={db_config['username']};"
    f"PWD={db_config['password']}"
)

def get_connection():
    return pyodbc.connect(conn_str)

class Student(BaseModel):
    id: int
    name: str
    age: int
    class_name: str

@app.post("/students/")
def create_student(student: Student):
    print("Received:", student)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM students WHERE id = ?", (student.id,))
    if cursor.fetchone():
        raise HTTPException(status_code=400, detail="Student ID already exists")

    cursor.execute(
        "INSERT INTO students (id, name, age, class_name) VALUES (?, ?, ?, ?)",
        (student.id, student.name, student.age, student.class_name)
    )
    conn.commit()
    print("Inserted")
    return {"message": "Student created successfully"}


@app.get("/students/{student_id}")
def get_student(student_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM students WHERE id = ?", (student_id,))
    row = cursor.fetchone()
    if row:
        return {"id": row[0], "name": row[1], "age": row[2], "class_name": row[3]}
    raise HTTPException(status_code=404, detail="Student not found")
@app.get("/students/")
def get_all_students():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM students")
    rows = cursor.fetchall()
    return [{"id": row[0], "name": row[1], "age": row[2], "class_name": row[3]} for row in rows]


@app.put("/students/{student_id}")
def update_student(student_id: int, student: Student):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE students SET name = ?, age = ?, class_name = ? WHERE id = ?",
        (student.name, student.age, student.class_name, student_id)
    )
    conn.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Student not found")
    return {"message": "Student updated successfully"}

# Delete student
@app.delete("/students/{student_id}")
def delete_student(student_id: int):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM students WHERE id = ?", (student_id,))
    conn.commit()
    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Student not found")
    return {"message": "Student deleted successfully"}
