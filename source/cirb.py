from cirb_duration_data_object import CIRBDurationDataObject


def lambda_(
    data_obj: CIRBDurationDataObject, cause: str, effect: str, window_size: int
):
    """
    Compute:
              w        sum_duration_y_in_window(Nw(y <- x))
        lambda    = -----------------------------------------
              x|y           total_duration(y)

        In which:
            sum_duration_y_in_window(Nw(y <- x)):
                Given x occurs and y occurred in the previous window,
                accumulate the durations of all y that window.

            total_duration(y): All durations of y in the entire dataset.
    """
    sum_duration_y_in_window = data_obj.accumulated_cause_durations[
        (cause, effect, window_size)
    ]
    total_duration_y = data_obj.duration_col[
        data_obj.cause_col.apply(data_obj._exist, args=(cause,))
    ].sum()

    if total_duration_y == 0:
        return 0
    return sum_duration_y_in_window / total_duration_y


def cirb(
    data_obj: CIRBDurationDataObject, cause: str, effect: str, window_size: int
) -> float:
    """
    Compute:
                            w
                      lambda
                            y|x
        CIRb(x, y) = -----------
                      lambda
                            y
    """
    nominator = lambda_(data_obj, cause, effect, window_size)
    denominator = data_obj.N[effect] / data_obj.T

    if denominator == 0:
        return 0
    return nominator / denominator
