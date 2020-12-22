from connexion import connect_db_target

con = connect_db_target()

data = (
    [1,'Presidente',1,"automático"],
    [2,'1er Vicepresidente',1,'automático'],
    [3,'2do Vicepresidente',1,'automático'],
    [4,'Congresista',1,'automático'],
)

cur = con.cursor()
cur.executemany("INSERT INTO cargo(id, cargo, estado, usuario_registro) VALUES(%s, %s, %s, %s)",data)
con.commit()
con.close()



