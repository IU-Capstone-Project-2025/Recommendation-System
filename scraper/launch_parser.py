from parse import parse
if __name__ == "__main__":
    for i in range(1, 110):
        try:
            parse(i)
        except:
            pass