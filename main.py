import polars as pl
import os
import json  # if proves to be too slow or memory consuming look into https://github.com/ICRAR/ijson + https://github.com/lloyd/yajl
from collections import Counter
import argparse
from functools import reduce, partial, lru_cache
from collections.abc import Iterable
from operator import itemgetter
import datetime
import logging
from dateutil.parser import parse

parser = argparse.ArgumentParser(description="")

parser.add_argument(
    "--pt_record_csv",
    type=str,
    help="CSV containing patient MRNs and earliest dates",
)

parser.add_argument(
    "--fields",
    type=str,
    nargs="+",
    default=["SUBJECT", "PROVIDER_TYPE", "SPECIALTY_NAME"],
    help="Fields for which we want to get the totals",
)

parser.add_argument(
    "--notes_dir",
    type=str,
    help="Directory containing nested directories of notes contained in JSON files",
)

parser.add_argument(
    "--output_dir",
    type=str,
    default=".",
    help="Directory for outputting table",
)
logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s -   %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
    level=logging.INFO,
)


# Keep it simple by avoiding time information for now
# we can fold it back in if we need that degree of granularity
@lru_cache
def parse_and_normalize_date(dt_str: str) -> datetime.date:
    parsed_dt = parse(dt_str, fuzzy=True)
    return parsed_dt.date()


@lru_cache
def is_before(pt_earliest: str, note_date: str) -> bool:
    return parse_and_normalize_date(pt_earliest) <= parse_and_normalize_date(note_date)


def mrn_if_note_and_time_compatible(
    mrn_to_earliest_date: dict[int, str], note_json: dict[str, str | int]
) -> int | None:
    mrn = int(note_json["DFCI_MRN"])
    # note_id = int(note_json["id"])
    if mrn not in mrn_to_earliest_date.keys():
        return None
    pt_earliest = mrn_to_earliest_date.get(mrn)
    if pt_earliest is None:
        raise ValueError(
            f"{mrn} is not associated with a date despite nulls having been dropped earlier"
        )
        return None
    note_date = note_json.get("EVENT_DATE")
    if note_date is None:
        raise ValueError(f"{note_json} missing an event date")
        return None
    if is_before(pt_earliest, note_date):
        # logger.info(
        #     f"QUALIFIES - MRN {mrn} with earliest occurrence {pt_earliest} with note {note_id} with date {note_date}"
        # )
        return mrn
    # logger.info(
    #     f"NOT QUALIFIED - MRN {mrn} with earliest occurrence {pt_earliest} with note {note_id} with date {note_date}"
    # )
    return None  # for type checking


# [
#     "PROVIDER_TYPE",
#     "EVENT_DATE",
#     "INP_RPT_TYPE_CD",
#     "INP_RPT_TYPE",
#     "PROVIDER_DEPARTMENT",
#     "RPT_TEXT_NO_HTML",
#     "RPT_TYPE_STR",
#     "IMG_EXAM_END_DATE",
#     "PROVIDER_TYPE_STR",
#     "PROVIDER_DEPARTMENT_STR",
#     "LAB_STATUS_CD",
#     "ENCOUNTER_TYPE_DESC_STR",
#     "ORD_STATUS_DESC_STR",
#     "id",
#     "PROVIDER_CRED",
#     "DEPT_ID",
#     "LAST_INDEX_DATE",
#     "IMPRESSION_TEXT",
#     "PRACTICE_NAME",
#     "ENCOUNTER_TYPE_CD",
#     "PRACTICE_ID",
#     "RPT_STATUS_STR",
#     "INSTITUTION_STR",
#     "AUTHOR_NAME",
#     "AUTHOR_NAME_STR",
#     "ORD_STATUS_DESC",
#     "SUBJECT_STR",
#     "NARRATIVE_TEXT",
#     "RPT_DATE",
#     "RPT_TYPE_CD",
#     "SOURCE",
#     "ENCOUNTER_TYPE_DESC",
#     "DFCI_MRN",
#     "LAB_STATUS_DESC_STR",
#     "LAST_UPD_PRACTICE_NAME",
#     "RPT_TEXT",
#     "EDW_LAST_MOD_DT",
#     "AUTHOR_ID",
#     "EMR_SOURCE",
#     "SPECIALTY_NAME",
#     "DATA_SOURCE",
#     "PROC_DESC_STR",
#     "RPT_TYPE",
#     "PRACTICE_NAME_STR",
#     "SOURCE_STR",
#     "PROVIDER_CRED_STR",
#     "LAST_UPD_PRACTICE_ID",
#     "RPT_ID",
#     "SPECIALTY_NAME_STR",
#     "SPEC_TAKEN_DATE",
#     "ORD_STATUS_CD",
#     "ENCOUNTER_ID",
#     "RPT_STATUS_CD",
#     "PROVIDER_NAME",
#     "ACCESSION_ID",
#     "LAST_UPD_PRACTICE_NAME_STR",
#     "PROC_DESC",
#     "INSTITUTION",
#     "RPT_STATUS",
#     "INP_RPT_TYPE_STR",
#     "LAB_STATUS_DESC",
#     "_version_",
#     "SPECIALTY_ID",
#     "ADDENDUM_TEXT",
#     "SUBJECT",
#     "PROVIDER_NAME_STR",
#     "PROC_ID",
# ]
# CSV rows for reference - just in imaging?


def get_file_totals_csv(
    mrn_to_earliest_date: dict[int, str], csv_path: str
) -> Counter[int]:
    raise NotImplementedError("Maybe we need to do this maybe we don't")


def get_file_totals_json(
    mrn_to_earliest_date: dict[int, str], json_path: str
) -> Counter[int]:
    with open(json_path) as f:
        note_json_list = json.load(f)["response"]["docs"]
    local_mrn = partial(mrn_if_note_and_time_compatible, mrn_to_earliest_date)
    return Counter(filter(None, map(local_mrn, note_json_list)))


def get_dir_totals(
    mrn_to_earliest_date: dict[int, str], notes_dir: str
) -> Iterable[Counter[int]]:
    for root, dirs, files in os.walk(notes_dir):
        root_total = 0
        for fn in files:
            if fn.lower().endswith("json"):
                file_counter = get_file_totals_json(
                    mrn_to_earliest_date, os.path.join(root, fn)
                )
                yield file_counter
                root_total += sum(file_counter.values())
            if fn.lower().endswith("csv"):
                file_counter = get_file_totals_csv(
                    mrn_to_earliest_date, os.path.join(root, fn)
                )
                yield file_counter
                root_total += sum(file_counter.values())
        if root_total > 0:
            logger.info(
                f"Total qualifying files in {os.path.basename(root)}: {root_total}"
            )


def get_totals(
    mrn_to_earliest_date: dict[int, str], notes_dir: str
) -> Iterable[tuple[int, int]]:
    full_counter = reduce(
        Counter.__add__, get_dir_totals(mrn_to_earliest_date, notes_dir)
    )
    # return sorted by totals
    return sorted(full_counter.items(), key=itemgetter(1), reverse=True)


def collect_notes_and_write_metrics(
    pt_record_csv: str, notes_dir: str, output_dir: str, fields: list[str]
) -> None:
    pt_record_df = pl.read_csv(pt_record_csv)
    mrn_and_date_df = (
        pt_record_df.with_columns(pl.col("mrn").cast(pl.Int64).alias("mrn"))
        .select("mrn", "earliest_date")
        .drop_nulls()
    )
    assert all(mrn_and_date_df.is_unique()), (
        f"Not unique in {mrn_and_date_df} {mrn_and_date_df.is_unique()}"
    )
    mrn_to_earliest_date = {
        mrn: earliest_date
        for mrn, earliest_date in zip(
            mrn_and_date_df["mrn"], mrn_and_date_df["earliest_date"]
        )
    }
    final_sorted_df = pl.DataFrame(
        get_totals(mrn_to_earliest_date, notes_dir),
        schema=["MRN", "TOTAL_AFTER_EARLIEST"],
        orient="row",
    )
    final_sorted_df.write_csv(os.path.join(output_dir, "totals.csv"), separator=",")


def main():
    args = parser.parse_args()
    collect_notes_and_write_metrics(
        args.pt_record_csv, args.notes_dir, args.output_dir, args.fields
    )


if __name__ == "__main__":
    main()
