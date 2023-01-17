import multiprocessing as mp
from duration_data_object import DurationDataObject


class NSTDurationDataObject(DurationDataObject):
    def __init__(
        self,
        data_path: str,
        cause_col_name: str,
        effect_col_name: str,
        duration_col_name: str,
        window_sizes: list,
        data_size: int = -1,
    ):
        super().__init__(
            data_path,
            cause_col_name,
            effect_col_name,
            duration_col_name,
            window_sizes,
            data_size,
        )
        self.accumulated_cause_durations = dict()
        self.accumulated_effect_durations = dict()

        self._init_accumulated_cause_durations()
        self._init_accumulated_effect_durations()

    def _init_accumulated_cause_durations(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_accumulated_cause_durations,
                [
                    (cause, effect, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                ],
            )
        self.accumulated_cause_durations.update(
            {result[0]: result[1] for result in results}
        )

    def _calc_accumulated_cause_durations(self, *args):
        cause, effect, window_size = args[0]
        sum_accumulated_duration = 0

        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window = self.cause_col[i - window_size + 1 : i + 1]
                sum_accumulated_duration += self.duration_col[
                    i - window_size + 1 : i + 1
                ][current_window.apply(self._exist, args=(cause,))].sum()
        return (window_size, cause, effect), sum_accumulated_duration

    def _init_accumulated_effect_durations(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_accumulated_effect_durations,
                [
                    (cause, effect, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                ],
            )
        self.accumulated_effect_durations.update(
            {result[0]: result[1] for result in results}
        )

    def _calc_accumulated_effect_durations(self, *args):
        cause, effect, window_size = args[0]
        sum_accumulated_duration = 0

        for i in range(self.T - window_size + 1):
            if cause in self.cause_col[i].split(", "):
                current_window = self.effect_col[i : i + window_size]
                sum_accumulated_duration += self.duration_col[i : i + window_size][
                    current_window.apply(self._exist, args=(effect,))
                ].sum()
        return (window_size, cause, effect), sum_accumulated_duration


if __name__ == "__main__":
    from datetime import datetime
    import os

    data_path = os.path.join(
        os.getcwd(),
        "dataset",
        "Air",
        "Air_PM2.5_cluster_duration_dataset.csv",
    )
    window_sizes = [15]
    print(datetime.now())
    data_obj = NSTDurationDataObject(
        data_path, "cause", "effect", "duration", window_sizes, 100
    )
    from pprint import pprint

    pprint(data_obj.sufficiency)
    print(datetime.now())
