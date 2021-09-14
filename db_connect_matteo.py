
import psycopg2

def connect_HAIG_ROMA():
    # Connect to an existing database
    conn = psycopg2.connect(user="postgres", password="superuser", host="10.1.0.1",
                            port="5432", database="HAIG_ROMA", sslmode="disable", gssencmode="disable")
    return (conn)