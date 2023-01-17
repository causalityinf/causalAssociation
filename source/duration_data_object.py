import os
import pandas as pd
import multiprocessing as mp

from itertools import chain
from datetime import datetime


class DurationDataObject:
    def __init__(
        self,
        data_path: str,
        cause_col_name: str,
        effect_col_name: str,
        duration_col_name: str,
        window_sizes: list,
        data_size: int = -1,
    ):
        self.dataset = pd.read_csv(data_path)
        self.cause_col_name = cause_col_name
        self.effect_col_name = effect_col_name
        self.duration_col_name = duration_col_name
        self.window_sizes = window_sizes
        if data_size > 0 and data_size < self.dataset.shape[0]:
            self._read_dataset_with_size(data_size)

        if data_size > 0:
            self.cause_col = self.dataset[cause_col_name].iloc[:data_size]
            self.effect_col = self.dataset[effect_col_name].iloc[:data_size]
            self.duration_col = self.dataset[duration_col_name].iloc[:data_size]
        else:
            self.cause_col = self.dataset[cause_col_name]
            self.effect_col = self.dataset[effect_col_name]
            self.duration_col = self.dataset[duration_col_name]

        self.cause_col = self.cause_col.apply(self._nan_to_str)
        self.effect_col = self.effect_col.apply(self._nan_to_str)

        self.cause_set = self._init_cause_set()
        self.effect_set = self._init_effect_set()

        self.necessity = dict()
        self.sufficiency = dict()
        self.N = dict()
        self.D = dict()
        self.p = dict()
        self.T = len(self.cause_col)

        self._init_necessity()
        self._init_sufficiency()
        self._init_D()
        self._init_N()
        self._init_p()

    @staticmethod
    def _nan_to_str(value):
        if isinstance(value, str):
            return value
        return ""

    def _read_dataset_with_size(self, size: int):
        for i in range(self.dataset.shape[0]):
            if self.dataset["end"][i] > size:
                self.dataset = self.dataset.iloc[: i + 1]
                self.dataset[self.duration_col_name][i] -= self.dataset["end"][i] - size
                break
            elif self.dataset["end"][i] == size:
                self.dataset = self.dataset.iloc[: i + 1]
                break

    def _init_cause_set(self):
        cause_list = list(map(lambda items: items.split(", "), set(self.cause_col)))
        cause_set = set(list(chain(*cause_list)))
        cause_set.discard("")
        return cause_set

    def _init_effect_set(self):
        effect_list = list(map(lambda items: items.split(", "), set(self.effect_col)))
        effect_set = set(list(chain(*effect_list)))
        effect_set.discard("")
        return effect_set

    @staticmethod
    def _exist(cause, event):
        return event in cause.split(", ")

    def _init_necessity(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_necessity,
                [
                    (cause, effect, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                ],
            )
        self.necessity.update({result[0]: result[1] for result in results})

    def _calc_necessity(self, *args):
        cause, effect, window_size = args[0]
        window_counts = 0

        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window = self.cause_col[i - window_size + 1 : i + 1]

                if current_window.apply(self._exist, args=(cause,)).any():
                    window_counts += 1
        return (
            (window_size, cause, effect),
            window_counts,
        )

    def _init_sufficiency(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_sufficiency,
                [
                    (cause, effect, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                ],
            )
        self.sufficiency.update({result[0]: result[1] for result in results})

    def _calc_sufficiency(self, *args):
        cause, effect, window_size = args[0]
        window_counts = 0

        for i in range(self.T - window_size + 1):
            if cause in self.cause_col[i].split(", "):
                current_window = self.effect_col[i : i + window_size]

                if current_window.apply(self._exist, args=(effect,)).any():
                    window_counts += 1
        return (
            (window_size, cause, effect),
            window_counts,
        )

    def _init_D(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_D,
                [
                    (cause, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                ],
            )

        self.D.update({result[0]: result[1] for result in results})

    def _calc_D(self, *args):
        cause, window_size = args[0]
        windows_count = 0

        for i in range(self.T - window_size + 1):
            current_window = self.cause_col[i : i + window_size]

            if current_window.apply(self._exist, args=(cause,)).any():
                windows_count += 1
        return (window_size, cause), windows_count

    def _init_N(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_N,
                [event for event in self.cause_set.union(self.effect_set)],
            )
        self.N.update({result[0]: result[1] for result in results})

    def _calc_N(self, event):
        if self.cause_col.apply(self._exist, args=(event,)).any():
            event_col = self.cause_col
        else:
            event_col = self.effect_col

        return event, event_col.apply(self._exist, args=(event,)).sum()

    def _init_p(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_p,
                [event for event in self.cause_set.union(self.effect_set)],
            )
        self.p.update({result[0]: result[1] for result in results})

    def _calc_p(self, event):
        return event, self.N[event] / self.T

    def pw_backward(self, cause, effect, window_size):
        nec = self.necessity[(window_size, cause, effect)]
        return nec / self.T

    def pw_forward(self, cause, effect, window_size):
        suf = self.sufficiency[(window_size, cause, effect)]
        return suf / self.T


if __name__ == "__main__":
    data_path = os.path.join(
        os.getcwd(),
        "dataset",
        "Clinic",
        "Clinic_event_duration.csv",
    )
    window_sizes = [1, 2, 3, 5, 7, 10, 15, 20]
    print(datetime.now())
    data_obj = DurationDataObject(
        data_path, "cause", "effect", "duration", window_sizes, 100
    )
    print(datetime.now())
