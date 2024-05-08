import sqlite3

def setup_database():
    conn = sqlite3.connect('scheduler.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS task (
            id INTEGER PRIMARY KEY,
            cron_expression TEXT,
            function TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS result (
            id INTEGER PRIMARY KEY,
            task_id INTEGER,
            execution_date_time TEXT,
            result TEXT,
            FOREIGN KEY(task_id) REFERENCES tasks(id)
        )
    ''')
    conn.commit()
    conn.close()



if __name__ == "__main__":
    setup_database()
    