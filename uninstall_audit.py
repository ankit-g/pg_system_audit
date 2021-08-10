from system_audit import engine

def drop_audit():

    try:
        engine.execute("DROP SCHEMA audit CASCADE")
    except Exception as ex:
        print(ex)

if __name__ == '__main__':
    drop_audit()
