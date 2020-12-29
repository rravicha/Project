# sudo systemctl start mysql
import mariadb

def dbconnect():
    rows=[]
    conn = mariadb.connect(user="root",password="susi",host="localhost",port=3306,database="test")
    cursor=conn.cursor()
    cursor.execute("select * from global.logtab")
    users=cursor.fetchall()

    for user in users:
        rows.append(user)
    return rows

def main():
    print(dbconnect())

if __name__ == '__main__':
    main()