import math


class Baseline:
    def __init__(self, alpha=0.08):
        self.alpha = alpha
        self.mean = None
        self.var = 0.0
        self.n = 0

    def update(self, x):
        self.n += 1
        if self.mean is None:
            self.mean = x
            return
        diff = x - self.mean
        self.mean += self.alpha * diff
        self.var = (1 - self.alpha) * (self.var + self.alpha * diff * diff)

    @property
    def std(self):
        return math.sqrt(self.var)
