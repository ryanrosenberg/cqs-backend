def slug(str):
    new_str = str.lower().replace(' ', '-')
    return new_str

def string(int):
    new_str = str(int).replace('.0', '')
    return new_str