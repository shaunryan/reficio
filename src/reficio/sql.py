def find_sql_files(path):
    sql_files = []
    for file_info in dbutils.fs.ls(path):
        if file_info.isDir():
            sql_files.extend(find_sql_files(file_info.path))
        elif file_info.path.lower().endswith('.sql'):
            sql_files.append(file_info.path)
    return sql_files