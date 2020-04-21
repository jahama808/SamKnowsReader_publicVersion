import MySQLdb


def connection():
    host = <mysql database host>
    port = <port>
    dbUser = <user_id>
    database = <database_name>
    dbPass= <password>

    conn = MySQLdb.connect(host=host,port=port,user=dbUser,passwd=dbPass,db=database)
    c =  conn.cursor()
    
    return c,conn
