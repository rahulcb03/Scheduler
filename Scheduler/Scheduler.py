import sqlite3
import time
from croniter import croniter
from datetime import datetime

# setup_databse is used to set the SQLite database and insert the tasks into the table
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

    #insert the task into the task table
    c.execute('''
        INSERT INTO task(id, cron_expression, function)
        VALUES (1, '*/5 * * * *', 'print("this is a log line")')
    ''')
    
    c.execute('''
        INSERT INTO task(id, cron_expression, function)
        VALUES (2, '0 10 * * *', 'print("this runs at 10AM every day")')
    ''')

    conn.commit()
    conn.close()

# return: list of tasks from task table : [(id, cron_expression, func)]
def retrieve_tasks(): 
    conn = sqlite3.connect('scheduler.db')
    c = conn.cursor()
   
    c.execute('''
        SELECT *
        FROM task 
    ''')

    tasks = c.fetchall()
    conn.commit()
    conn.close()

    return tasks

# paramters task_id: the id of thetask to execute , func: the func to execute
# runs the func provided and adds a row to results with the current time and status of the execution
def run_task(task_id, func): 
    conn = sqlite3.connect('scheduler.db')
    c = conn.cursor()
    curr_time = datetime.now()

    try:
        exec(func)
        result = "success"
    except:
        result = "failure"

    c.execute("""
        INSERT INTO result(task_id, execution_date_time, result)
        VALUES (?, ?, ?)
    """, (task_id, curr_time.strftime(("%d/%m/%Y, %H:%M:%S")), result))

    conn.commit()
    conn.close()


# paramter tasks : the tasks in the task table [(id, cron_expresion, function)]]
# checks the tasks every 60 secs if the next run time is >= current time then run the task 
def scheduler_loop(tasks): 
    tasks_with_croniter = []

    # initialize the tasks_with_croniter and add a croniter and the next run time to each task
    for task_id, cron_expres, func in tasks: 
        iter = croniter(cron_expres, datetime.now())
        tasks_with_croniter.append( (task_id, iter, iter.get_next(datetime), func)) 

    # loop that periodictly checks if the tasks need to be run
    while True:
        for n in range(len(tasks_with_croniter)):
            id, iter, next_run, func = tasks_with_croniter[n]

            #check if task needs to be run and updates the next run time
            if next_run <= datetime.now(): 
                run_task(id, func)
                tasks_with_croniter[n] = (id, iter, iter.get_next(datetime), func)
        
        time.sleep(60) 



if __name__ == "__main__":
    setup_database()
    scheduler_loop(retrieve_tasks())
