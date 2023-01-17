from nst_duration_data_object import NSTDurationDataObject


def nst(
    data_obj: NSTDurationDataObject,
    cause: str,
    effect: str,
    window_size: int,
    lambda_const: float,
    alpha_const: float,
):
    pw_backward = data_obj.pw_backward(cause, effect, window_size)
    pw_forward = data_obj.pw_forward(cause, effect, window_size)
    px = data_obj.p[cause]
    py = data_obj.p[effect]
    sum_duration_in_window_x = data_obj.accumulated_cause_durations[
        (window_size, cause, effect)
    ]
    sum_duration_in_window_y = data_obj.accumulated_effect_durations[
        (window_size, cause, effect)
    ]
    total_duration_x = data_obj.duration_col[
        data_obj.cause_col.apply(data_obj._exist, args=(cause,))
    ].sum()
    total_duration_y = data_obj.duration_col[
        data_obj.effect_col.apply(data_obj._exist, args=(effect,))
    ].sum()

    if px == 0 or py == 0 or total_duration_x == 0 or total_duration_y == 0:
        return 0

    left_term = (pw_backward * sum_duration_in_window_x) / (
        px ** alpha_const * py * total_duration_x
    )
    right_term = (pw_forward * sum_duration_in_window_y) / (
        px * py ** alpha_const * total_duration_y
    )
    return left_term ** lambda_const * right_term ** (1 - lambda_const)
