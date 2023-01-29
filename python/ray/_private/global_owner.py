import os


class HAObjectGlobalOwner:
    def __init__(self):
        print(f"Finished initializing global owner, pid: {os.getpid()}")

    def warmup(self):
        pass
