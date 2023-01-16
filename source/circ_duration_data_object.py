import multiprocessing as mp
from duration_data_object import DurationDataObject


class CIRCDurationDataObject(DurationDataObject):
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
        self.effect_durations_when_cause_comp = dict()

        self._init_accumulated_cause_durations()
        self._init_effect_durations_when_cause_comp()

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
        """
        Considering (x <- y).
        If y occurs and x occurred in the previous window, accumulate the durations of all x in that window.
        """
        cause, effect, window_size = args[0]
        sum_duration = 0

        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window = self.cause_col[i - window_size + 1 : i + 1]
                sum_duration += self.duration_col[i - window_size + 1 : i + 1][
                    current_window.apply(self._exist, args=(cause,))
                ].sum()
        return (window_size, cause, effect), sum_duration

    def _init_effect_durations_when_cause_comp(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_effect_durations_when_cause_comp,
                [
                    (cause, effect, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                ],
            )
        self.effect_durations_when_cause_comp.update(
            {result[0]: result[1] for result in results}
        )

    def _calc_effect_durations_when_cause_comp(self, *args):
        """
                     _
        Considering (x <- y).
        If y occurs but x didn't occur in the previous window, accumulate the duration of y.
        """
        cause, effect, window_size = args[0]
        sum_duration = 0

        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window = self.cause_col[i - window_size + 1 : i + 1]

                if not current_window.apply(self._exist, args=(cause,)).any():
                    sum_duration += self.duration_col[i]
        return (window_size, cause, effect), sum_duration

    def _init_necessity(self):
        pass

    def _init_sufficiency(self):
        pass

    def _init_D(self):
        pass

    def _init_p(self):
        pass


if __name__ == "__main__":
    import os
    from datetime import datetime

    data_path = os.path.join(
        os.getcwd(),
        "dataset",
        "Traffic",
        "Metro_Interstate_Traffic_Volume_duration.csv",
    )
    window_sizes = [1, 2, 3, 5, 7, 10, 15, 20]
    print(datetime.now())
    data_obj = CIRCDurationDataObject(
        data_path, "cause", "effect", "duration", window_sizes, 100
    )
    print(datetime.now())
