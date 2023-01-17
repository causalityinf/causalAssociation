from circ_duration_data_object import CIRCDurationDataObject


def lambda_(
    data_obj: CIRCDurationDataObject, cause: str, effect: str, window_size: int
):
    sum_duration_y_in_window = data_obj.accumulated_cause_durations[
        (window_size, cause, effect)
    ]
    total_duration = data_obj.duration_col[
        data_obj.cause_col.apply(data_obj._exist, args=(cause,))
    ].sum()

    if total_duration == 0:
        return 0
    return sum_duration_y_in_window / total_duration


def lambda_comp(
    data_obj: CIRCDurationDataObject, cause: str, effect: str, window_size: int
):
    sum_duration_x_in_window = data_obj.effect_durations_when_cause_comp[
        (window_size, cause, effect)
    ]
    total_duration_x = data_obj.duration_col[
        data_obj.effect_col.apply(data_obj._exist, args=(effect,))
    ].sum()
    if total_duration_x == 0:
        return 0
    return sum_duration_x_in_window / total_duration_x


def circ(
    data_obj: CIRCDurationDataObject, cause: str, effect: str, window_size: int
) -> float:
    nominator = lambda_(data_obj, cause, effect, window_size)
    denominator = lambda_comp(data_obj, cause, effect, window_size)

    if denominator == 0:
        return 0
    return nominator / denominator
