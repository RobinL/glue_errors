def get_two():
    return 2

def read_sql(path):
    with open(path, "r") as f:
        sql = f.readlines()
        sql = "".join(sql)
    return sql