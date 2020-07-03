import math


class ProcessBar(object):
    """一个打印进度条的类"""

    def __init__(self, total):  # 初始化传入总数
        self.shape = ['▏', '▎', '▍', '▋', '▊', '▉']
        self.shape_num = len(self.shape)
        self.row_num = 30
        self.now = 0
        self.total = total

    def print_next(self, now=-1):  # 默认+1
        if now == -1:
            self.now += 1
        else:
            self.now = now

        rate = math.ceil((self.now / self.total) * (self.row_num * self.shape_num))
        head = rate // self.shape_num
        tail = rate % self.shape_num
        info = self.shape[-1] * head
        if tail != 0:
            info += self.shape[tail - 1]
        full_info = '[%s%s] [%.2f%%]' % (info, (self.row_num - len(info)) * '  ', 100 * self.now / self.total)

        print("\r", end='', flush=True)
        print(full_info, end='', flush=True)

        if self.now == self.total:
            print('')


if __name__ == '__main__':
    pb = ProcessBar(10000000)
    for i in range(10000000):
        pb.print_next()
