import json
import multiprocessing as mp

from itertools import combinations

from duration_data_object import DurationDataObject


class CIRMDurationDataObject(DurationDataObject):
    def __init__(
        self,
        data_path: str,
        cause_col_name: str,
        effect_col_name: str,
        duration_col_name: str,
        window_sizes: list,
        parent_path: str,
        data_size: int = -1,
    ):
        super().__init__(
            data_path,
            cause_col_name,
            effect_col_name,
            duration_col_name,
            window_sizes,
            data_size=data_size,
        )

        self.single_z_set = self._init_z_set(parent_path)
        self.enumerated_z_set = self._enumerate_z()

        self.accumulated_cause_durations_single_z = dict()
        self.accumulated_cause_durations_enumerated_z = dict()
        self.effect_durations_when_cause_comp_single_z = dict()
        self.effect_durations_when_cause_comp_enumerated_z = dict()

        self._init_accumulated_cause_durations_single_z()
        self._init_accumulated_cause_durations_enumerated_z()
        self._init_effect_durations_when_cause_comp_single_z()
        self._init_effect_durations_when_cause_comp_enumerated_z()

    def _init_z_set(self, path):
        with open(path, "r") as f:
            z_set = json.load(f)
        return z_set
        # return {
        #     "traffic_volume_0": [
        #         "heavy intensity rain",
        #         "mist",
        #         "moderate rain",
        #         "traffic_volume_0",
        #         "traffic_volume_2",
        #     ],
        #     "traffic_volume_1": [
        #         "mist",
        #         "moderate rain",
        #         "traffic_volume_1",
        #         "light rain",
        #     ],
        #     "traffic_volume_2": [
        #         "moderate rain",
        #         "light rain",
        #         "heavy intensity rain",
        #         "traffic_volume_1",
        #         "traffic_volume_2",
        #     ],
        # }

    def _enumerate_z(self):
        new_z_set = dict()
        for effect, z_list in self.single_z_set.items():
            new_z_set[effect] = [
                i for j in range(1, len(z_list) + 1) for i in combinations(z_list, j)
            ]
        return new_z_set

    def _init_accumulated_cause_durations_single_z(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_accumulated_cause_durations_single_z,
                [
                    (cause, effect, z, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                    for z in self.single_z_set[effect]
                ],
            )
        self.accumulated_cause_durations_single_z.update(
            {result[0]: result[1] for result in results}
        )

    def _calc_accumulated_cause_durations_single_z(self, *args):
        """
        Compute:
            sum_duration_y_in_window(Nw(y, z <- x))
            Given x occurs, if y and z occurred in the previous window, accumulate the durations of all y in the previous window.
        Params:
            args[0] = (cause, effect, z, window_size)
        """
        cause, effect, z, window_size = args[0]
        z_cols = dict()
        if self.cause_col.apply(self._exist, args=(z,)).any():
            z_cols[z] = self.cause_col
        else:
            z_cols[z] = self.effect_col

        sum_durations = 0

        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window_cause = self.cause_col[i - window_size + 1 : i + 1]

                if current_window_cause.apply(self._exist, args=(cause,)).any():
                    current_window_z = z_cols[z][i - window_size + 1 : i + 1]

                    if current_window_z.apply(self._exist, args=(z,)).any():
                        current_window_durations = self.duration_col[
                            i - window_size + 1 : i + 1
                        ]
                        sum_durations += current_window_durations[
                            current_window_cause.apply(self._exist, args=(cause,))
                        ].sum()
        return (window_size, cause, effect, z), sum_durations

    def _init_effect_durations_when_cause_comp_single_z(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_effect_durations_when_cause_comp_single_z,
                [
                    (cause, effect, z, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                    for z in self.single_z_set[effect]
                ],
            )
        self.effect_durations_when_cause_comp_single_z.update(
            {result[0]: result[1] for result in results}
        )

    def _calc_effect_durations_when_cause_comp_single_z(self, *args):
        """
        Compute:
                                        _
            sum_duration_x_in_window(Nw(y, z <- x))
            Given x occurs, if y didn't occur but z occurred in the previous window, accumulate the duration of x.
        Params:
            args[0] = (cause, effect, z, window_size)
        """
        cause, effect, z, window_size = args[0]
        z_cols = dict()
        if self.cause_col.apply(self._exist, args=(z,)).any():
            z_cols[z] = self.cause_col
        else:
            z_cols[z] = self.effect_col

        sum_durations = 0

        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window_cause = self.cause_col[i - window_size + 1 : i + 1]

                if not current_window_cause.apply(self._exist, args=(cause,)).any():
                    current_window_z = z_cols[z][i - window_size + 1 : i + 1]

                    if current_window_z.apply(self._exist, args=(z,)).any():
                        sum_durations += self.duration_col[i]
        return (window_size, cause, effect, z), sum_durations

    def _init_accumulated_cause_durations_enumerated_z(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_accumulated_cause_durations_enumerated_z,
                [
                    (cause, effect, z_combination, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                    for z_combination in self.enumerated_z_set[effect]
                ],
            )
        self.accumulated_cause_durations_enumerated_z.update(
            {result[0]: result[1] for result in results}
        )

    def _calc_accumulated_cause_durations_enumerated_z(self, *args):
        """
        Compute:
            sum_duration_y_in_window(Nw(y, z <- x))
            Given x occurs, if y and z occurred in the previous window, accumulate the durations of all y in the previous window.
        Params:
            args[0] = (cause, effect, z, window_size)
        """
        cause, effect, z_combination, window_size = args[0]
        z_cols = dict()
        for z in z_combination:
            if self.cause_col.apply(self._exist, args=(z,)).any():
                z_cols[z] = self.cause_col
            else:
                z_cols[z] = self.effect_col

        sum_durations = 0

        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window_cause = self.cause_col[i - window_size + 1 : i + 1]

                if current_window_cause.apply(self._exist, args=(cause,)).any():
                    z_occurrences = 0

                    for z in z_combination:
                        current_window_z = z_cols[z][i - window_size + 1 : i + 1]
                        if current_window_z.apply(self._exist, args=(z,)).any():
                            z_occurrences += 1

                    if z_occurrences == len(z_combination):
                        current_window_durations = self.duration_col[
                            i - window_size + 1 : i + 1
                        ]
                        sum_durations += current_window_durations[
                            current_window_cause.apply(self._exist, args=(cause,))
                        ].sum()

        return (window_size, cause, effect, z_combination), sum_durations

    def _init_effect_durations_when_cause_comp_enumerated_z(self):
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(
                self._calc_effect_durations_when_cause_comp_enumerated_z,
                [
                    (cause, effect, z_combination, window_size)
                    for window_size in self.window_sizes
                    for cause in self.cause_set
                    for effect in self.effect_set
                    for z_combination in self.enumerated_z_set[effect]
                ],
            )
        self.effect_durations_when_cause_comp_enumerated_z.update(
            {result[0]: result[1] for result in results}
        )

    def _calc_effect_durations_when_cause_comp_enumerated_z(self, *args):
        """
                                    _
        sum_duration_x_in_window(Nw(y, z <- x))
            Given x occurs, if y didn't occur but z occurred in the previous window, accumulate duration of x.
        Params:
            args[0] = (cause, effect, z, window_size)
        """
        cause, effect, z_combination, window_size = args[0]
        z_cols = dict()
        for z in z_combination:
            if self.cause_col.apply(self._exist, args=(z,)).any():
                z_cols[z] = self.cause_col
            else:
                z_cols[z] = self.effect_col

        sum_durations = 0
        for i in range(window_size - 1, self.T):
            if effect in self.effect_col[i].split(", "):
                current_window_cause = self.cause_col[i - window_size + 1 : i + 1]

                if not current_window_cause.apply(self._exist, args=(cause,)).any():
                    z_occurrences = 0

                    for z in z_combination:
                        current_window_z = z_cols[z][i - window_size + 1 : i + 1]
                        if current_window_z.apply(self._exist, args=(z,)).any():
                            z_occurrences += 1

                    if z_occurrences == len(z_combination):
                        sum_durations += self.duration_col[i]
        return (window_size, cause, effect, z_combination), sum_durations

    def _init_necessity(self):
        pass

    def _init_sufficiency(self):
        pass

    def _init_D(self):
        pass

    def _init_p(self):
        pass

    def _init_N(self):
        pass


if __name__ == "__main__":
    from datetime import datetime
    import os

    data_path = os.path.join(
        os.getcwd(), "dataset", "Clinic", "Clinic_event_duration_nopush.csv"
    )
    window_sizes = [1]
    print(datetime.now())
    data_obj = CIRMDurationDataObject(
        data_path,
        "cause",
        "effect",
        "duration",
        window_sizes,
        os.path.join(os.getcwd(), "parent", "Clinic", "parent.json"),
    )
    print(datetime.now())
