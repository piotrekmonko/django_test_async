
STOPBIT = 0xffff

BLACK = 0
RED = 1
GREEN = 2
YELLOW = 3

CTABLE = []


def colorized(colour_index, s, term=None):
    if not term:
        try:
            from blessings import Terminal
            term = Terminal()
        except ImportError:
            return s
    global CTABLE
    if not CTABLE:
        CTABLE = [(x, x) for x in range(term.number_of_colors)]
        for x in range(0, term.number_of_colors, 2):
            CTABLE += [(x, x + 1), (x + 1, x)]
    sl1 = len(s) / 2
    sl2 = len(s) - sl1
    ind = CTABLE[colour_index % len(CTABLE)]
    return term.color(ind[0])(s[:sl1]) + term.color(ind[1])(s[sl2:])