from cirm_duration_data_object import CIRMDurationDataObject


def lambda_42(
    data_obj: CIRMDurationDataObject, cause: str, effect: str, z: str, window_size: int
):
    """
    Compute:
              w         sum_duration_y_in_window(Nw(y, z <- x))
        lambda      = --------------------------------------------
              x|y,z             total_duration(y)

        In which:
            sum_duration_y_in_window(Nw(y, z <- x)):
                Given x occurs, if y, z occurred in the previous window, accumulate all durations of y in the previous window.
            total_duration(y): all durations of y in the entire dataset.
    """
    sum_duration_y_in_window = data_obj.accumulated_cause_durations_single_z[
        (window_size, cause, effect, z)
    ]
    total_duration_y = data_obj.duration_col[
        data_obj.cause_col.apply(data_obj._exist, args=(cause,))
    ].sum()

    if total_duration_y == 0:
        return 0
    return sum_duration_y_in_window / total_duration_y


def lambda_comp_43(
    data_obj: CIRMDurationDataObject, cause: str, effect: str, z: str, window_size: int
):
    """
    Compute:
                                                    _
              w         sum_duration_x_in_window(Nw(y, z <- x))
        lambda  _   = --------------------------------------------
              x|y,z             total_duration(x)

        In which:
                                        _
            sum_duration_x_in_window(Nw(y, z <- x)):
                Given x occurs, if y didn't occur but z occurred in the previous window, accumulate all durations of y in the previous window.
            total_duration(x): all durations of x in the entire dataset.
    """
    sum_duration_x_in_window = data_obj.effect_durations_when_cause_comp_single_z[
        (window_size, cause, effect, z)
    ]
    total_duration_x = data_obj.duration_col[
        data_obj.effect_col.apply(data_obj._exist, args=(effect,))
    ].sum()

    if total_duration_x == 0:
        return 0
    return sum_duration_x_in_window / total_duration_x


def cirm_single_z(
    data_obj: CIRMDurationDataObject, cause, effect, window_size
) -> float:
    """
    Compute:
                      --      w     --
                      | lambda       |
                      |       x|y,z  |
        CIRM(y, x) = g|--------------|
                      |       w      |
                      | lambda  _    |
                      --      x|y,z --
    """
    results = []
    for z in data_obj.single_z_set[effect]:
        nominator = lambda_42(data_obj, cause, effect, z, window_size)
        denominator = lambda_comp_43(data_obj, cause, effect, z, window_size)
        if denominator == 0:
            results.append(0)
        else:
            results.append(nominator / denominator)

    return {
        "max": max(results) if len(results) > 0 else 0,
        "min": min(results) if len(results) > 0 else 0,
        "avg": sum(results) / len(results) if len(results) > 0 else 0,
    }


def lambda_44(
    data_obj: CIRMDurationDataObject,
    cause: str,
    effect: str,
    z_combination: tuple,
    window_size: int,
):
    """
    Compute:
              w         sum_duration_y_in_window(Nw(y, z <- x))
        lambda      = --------------------------------------------
              x|y,z             total_duration(y)

        In which:
            sum_duration_y_in_window(Nw(y, z <- x)):
                Given x occurs, if y, z occurred in the previous window, accumulate all durations of y in the previous window.
            total_duration(y): all durations of y in the entire dataset.
    """
    sum_duration_y_in_window = data_obj.accumulated_cause_durations_enumerated_z[
        (window_size, cause, effect, z_combination)
    ]
    total_duration_y = data_obj.duration_col[
        data_obj.cause_col.apply(data_obj._exist, args=(cause,))
    ].sum()

    if total_duration_y == 0:
        return 0
    return sum_duration_y_in_window / total_duration_y


def lambda_comp_45(
    data_obj: CIRMDurationDataObject,
    cause: str,
    effect: str,
    z_combination: tuple,
    window_size: int,
):
    """
    Compute:
                                                    _
              w         sum_duration_x_in_window(Nw(y, z <- x))
        lambda  _   = --------------------------------------------
              x|y,z             total_duration(x)

        In which:
                                        _
            sum_duration_x_in_window(Nw(y, z <- x)):
                Given x occurs, if y didn't occur but z occurred in the previous window, accumulate all durations of y in the previous window.
            total_duration(x): all durations of x in the entire dataset.
    """
    sum_duration_x_in_window = data_obj.effect_durations_when_cause_comp_enumerated_z[
        (window_size, cause, effect, z_combination)
    ]
    total_duration_x = data_obj.duration_col[
        data_obj.effect_col.apply(data_obj._exist, args=(effect,))
    ].sum()

    if total_duration_x == 0:
        return 0
    return sum_duration_x_in_window / total_duration_x


def cirm_enumerated_z(
    data_obj: CIRMDurationDataObject, cause, effect, window_size
) -> float:
    """
    Compute:
                      --      w     --
                      | lambda       |
                      |       x|y,z  |
        CIRM(y, x) = g|--------------|
                      |       w      |
                      | lambda  _    |
                      --      x|y,z --
    """
    results = []
    for z_combination in data_obj.enumerated_z_set[effect]:
        nominator = lambda_44(data_obj, cause, effect, z_combination, window_size)
        denominator = lambda_comp_45(
            data_obj, cause, effect, z_combination, window_size
        )
        if denominator == 0:
            results.append(0)
        else:
            results.append(nominator / denominator)

    return {
        "max": max(results) if len(results) > 0 else 0,
        "min": min(results) if len(results) > 0 else 0,
        "avg": sum(results) / len(results) if len(results) > 0 else 0,
    }
