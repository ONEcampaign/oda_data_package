class _OdaList(list):
    def __repr__(self):
        # Return the elements with a line break between them
        return "[\n" + ",\n".join(str(x) for x in self) + "\n]"


class _OdaDict(dict):
    def __repr__(self):
        # Return the elements with a line break between them
        return "{\n" + ",\n".join(f"{k}: {v}" for k, v in self.items()) + "\n}"
