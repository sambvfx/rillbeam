"""
Terminal App

Wrappers around the curses library for building terminal apps.
"""
import logging
import curses
import threading

from typing import *


class App(object):

    COLORS = {
        'black': curses.COLOR_BLACK,
        'blue': curses.COLOR_BLUE,
        'cyan': curses.COLOR_CYAN,
        'green': curses.COLOR_GREEN,
        'magenta': curses.COLOR_MAGENTA,
        'red': curses.COLOR_RED,
        'white': curses.COLOR_WHITE,
        'yellow': curses.COLOR_YELLOW,
    }

    ATTRS = {
        'bold': curses.A_BOLD
    }

    def __init__(self, *args, **kwargs):
        self.win = self.new(*args, **kwargs)

        # Used to make each write thread-safe
        self.batch = threading.RLock()
        self._data = []  # type: List[Tuple[int, int, str, int]]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        curses.endwin()

    @classmethod
    def new(cls, *args, **kwargs):
        curses.setupterm()
        win = curses.initscr(*args, **kwargs)

        # init color
        curses.start_color()
        curses.use_default_colors()
        for i in cls.COLORS.values():
            curses.init_pair(i, i, curses.COLOR_BLACK)
        return win

    @property
    def width(self):
        return self.win.getmaxyx()[1]

    @property
    def height(self):
        return self.win.getmaxyx()[0]

    @property
    def line(self):
        return len(self._data) + 1

    def cull(self):
        self._data = self._data[-(self.height - 2):]

    def update(self):
        self.cull()
        for i, args in enumerate(self._data):
            y, x, value, color = args
            # value = '({}/{}) {}'.format(i, self.height, value)
            value += (' ' * (self.width - len(value) - 4))
            self.win.addstr(y or i + 1, x, value, color)
            # FIXME: this messes with borders
            # self.win.clrtoeol()
        self.win.refresh()

    def write(self, value='', color='white', attrs=(), y=None, x=1):
        if isinstance(color, str):
            color = curses.color_pair(self.COLORS[color])
        for attr in attrs:
            color = color | self.ATTRS[attr]
        with self.batch:
            for v in str(value).split('\n'):
                v = v.strip()
                if len(v) > self.width:
                    v = v[:self.width-5] + '...'
                self._data.append((y, x, v, color))
            self.update()

    def prompt(self, p='> ', y=None, x=1):
        if y is None:
            y = self.line
        self.win.refresh()
        self.win.addstr(y or self.line, x, p)
        self.win.refresh()
        curses.echo()
        msg = self.win.getstr(y, x + 2)
        curses.noecho()
        return msg

    @staticmethod
    def pane(*bounds):
        return Pane(*bounds)


class Pane(App):

    @classmethod
    def new(cls, *args, **kwargs):
        assert args, 'Must pass bounds'
        win = curses.newwin(*args, **kwargs)
        win.clear()
        win.border()
        win.refresh()
        return win

    def write(self, *args, **kwargs):
        super(Pane, self).write(*args, **kwargs)
        self.win.clear()
        self.win.border()

    def rebuild(self, *args, **kwargs):
        inst = self.__class__(*args, **kwargs)
        inst._data = self._data
        inst.update()
