
class InMemoryDB:
    def __init__(self):
        self.storage = {
            'prices_dict': {}
        }

    def set(self, key, value, expire=None):
        self.storage[key] = value

    def get(self, key):
        return self.storage.get(key)

    def delete(self, key):
        if key in self.storage:
            del self.storage[key]

    def clear(self):
        self.storage.clear()

    def exists(self, key):
        return key in self.storage

in_memory_db = None
def get_in_memory_db() -> InMemoryDB:
    global in_memory_db
    if in_memory_db is None:
        in_memory_db = InMemoryDB()
    return in_memory_db