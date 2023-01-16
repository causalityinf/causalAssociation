import os
import sys
import pandas as pd
import datetime
import argparse

import nst
import cirb
import circ
import cirm
from nst_duration_data_object import NSTDurationDataObject
from cirb_duration_data_object import CIRBDurationDataObject
from circ_duration_data_object import CIRCDurationDataObject
from cirm_duration_data_object import CIRMDurationDataObject

VERSION = 3


def parse_args():
    parser = argparse.ArgumentParser(description="Causality analysis")
    parser.add_argument(
        "-sz", "--size", help="Data size", required=False, default=-1, type=int
    )
    parser.add_argument(
        "--nst",
        help="Run NST",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--cirb",
        help="Run CIRB",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--circ",
        help="Run CIRC",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--cirm",
        help="Run CIRM",
        required=False,
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "-I",
        "--infile",
        help="Path to the data file",
        required=True,
        nargs=1,
        dest="in_file",
    )
    parser.add_argument(
        "-O",
        "--outdir",
        help="Path to the directory to store the results",
        required=True,
        nargs=1,
        dest="out_dir",
    )
    parser.add_argument(
        "--cause",
        help="The name of the cause column",
        required=True,
        dest="cause_col_name",
    )
    parser.add_argument(
        "--effect",
        help="The name of the effect column",
        required=True,
        dest="effect_col_name",
    )
    parser.add_argument(
        "--duration",
        help="The name of the duration column",
        required=True,
        dest="duration_col_name",
    )
    parser.add_argument("--parent", help="Path to the parent file", dest="parent_file")
    return parser.parse_args()


if __name__ == "__main__":
    window_sizes = [i for i in range(1, 31)]
    args = parse_args()
    in_file = args.in_file[0]
    out_dir = args.out_dir[0]
    cause_col_name = args.cause_col_name
    effect_col_name = args.effect_col_name
    duration_col_name = args.duration_col_name

    if not args.nst and not args.cirb and not args.circ and not args.cirm:
        print("[-] Please specify at least one score.")
        sys.exit(0)

    if args.nst:
        print("[+] Creating an NST duration data object.", datetime.datetime.now())
        nst_data_obj = NSTDurationDataObject(
            in_file,
            cause_col_name,
            effect_col_name,
            duration_col_name,
            window_sizes,
            args.size,
        )
        print("[+] Created NST data object.", datetime.datetime.now())
        cause_set = nst_data_obj.cause_set
        effect_set = nst_data_obj.effect_set

    if args.cirb:
        print("[+] Creating a CIRB duration data object.", datetime.datetime.now())
        cirb_data_obj = CIRBDurationDataObject(
            in_file,
            cause_col_name,
            effect_col_name,
            duration_col_name,
            window_sizes,
            args.size,
        )
        print("[+] Created CIRB data object.", datetime.datetime.now())
        cause_set = cirb_data_obj.cause_set
        effect_set = cirb_data_obj.effect_set

    if args.circ:
        print("[+] Creating a CIRC duration data object.", datetime.datetime.now())
        circ_data_obj = CIRCDurationDataObject(
            in_file,
            cause_col_name,
            effect_col_name,
            duration_col_name,
            window_sizes,
            args.size,
        )
        print("[+] Created CIRC data object.", datetime.datetime.now())
        cause_set = circ_data_obj.cause_set
        effect_set = circ_data_obj.effect_set

    if args.cirm:
        print("[+] Creating CIRM data object.", datetime.datetime.now())
        if not args.parent_file:
            print("[-] Please specify the parent file, use -h for help.")
            sys.exit(0)

        cirm_data_obj = CIRMDurationDataObject(
            in_file,
            cause_col_name,
            effect_col_name,
            duration_col_name,
            window_sizes,
            args.parent_file,
            args.size,
        )
        print("[+] Created CIRM data object.", datetime.datetime.now())
        cause_set = cirm_data_obj.cause_set
        effect_set = cirm_data_obj.effect_set

    results = {
        "window size": [],
        "cause": [],
        "effect": [],
    }

    if args.nst:
        results["nst"] = []
    if args.cirb:
        results["cirb"] = []
    if args.circ:
        results["circ"] = []
    if args.cirm:
        results["cirm 1 (avg)"] = []
        results["cirm 1 (max)"] = []
        results["cirm 2 (avg)"] = []
        results["cirm 2 (max)"] = []

    for window_size in window_sizes:
        for cause in cause_set:
            for effect in effect_set:
                results["window size"].append(window_size)
                results["cause"].append(cause)
                results["effect"].append(effect)

                if args.nst:
                    nst_score = nst.nst(
                        nst_data_obj, cause, effect, window_size, 0.5, 0.5
                    )
                    results["nst"].append(nst_score)

                if args.cirb:
                    cirb_score = cirb.cirb(cirb_data_obj, cause, effect, window_size)
                    results["cirb"].append(cirb_score)

                if args.circ:
                    circ_score = circ.circ(circ_data_obj, cause, effect, window_size)
                    results["circ"].append(circ_score)

                if args.cirm:
                    cirm_score = cirm.cirm_single_z(
                        cirm_data_obj, cause, effect, window_size
                    )
                    results["cirm 1 (avg)"].append(cirm_score["avg"])
                    results["cirm 1 (max)"].append(cirm_score["max"])
                    cirm_score = cirm.cirm_enumerated_z(
                        cirm_data_obj, cause, effect, window_size
                    )
                    results["cirm 2 (avg)"].append(cirm_score["avg"])
                    results["cirm 2 (max)"].append(cirm_score["max"])

    if not os.path.isdir(out_dir):
        os.makedirs(out_dir)

    printed_score = f'{"nst-" if args.nst else ""}{"cirb-" if args.cirb else ""}{"circ-" if args.circ else ""}{"cirm-" if args.cirm else ""}'
    df = pd.DataFrame(results)
    df.to_csv(
        os.path.join(
            out_dir,
            f"v-{VERSION}-sz-{args.size if args.size > 0 else 'full'}-{printed_score}.csv",
        ),
        index=False,
    )
    print("[+] Finished.", datetime.datetime.now())
