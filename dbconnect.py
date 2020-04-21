import MySQLdb


def connection():
    host = "cafdatabasehawaii.cmkcaklh2zi6.us-east-1.rds.amazonaws.com"
    port = 3306
    dbUser = "jahama"
    database = "cafdatabasehawaii"
    dbPass="Kaizen1!"

    conn = MySQLdb.connect(host=host,port=port,user=dbUser,passwd=dbPass,db=database)
    c =  conn.cursor()
    
    return c,conn