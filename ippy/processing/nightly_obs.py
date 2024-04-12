import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import MySQLdb

from ippy.misc import infer_inst_from_expname

if sys.version_info[:2] >= (3, 7):
    from ippy.constants import SCIDBS1

    SCIDBS1_HOST = SCIDBS1.node
    SCIDBS1_USER = SCIDBS1.user
    SCIDBS1_PSW = SCIDBS1.password
else:
    from ippy.constants import MYSQL_PSW_READ_ONLY, MYSQL_USER_READ_ONLY, SCIDBS1

    SCIDBS1_HOST = SCIDBS1
    SCIDBS1_USER = MYSQL_USER_READ_ONLY
    SCIDBS1_PSW = MYSQL_PSW_READ_ONLY


class Visit:
    def __init__(
        self,
        exp_name,
        exp_id,
        dateobs,
        object,
        # obs_mode,
        visit_num,
        chip_id=None,
        chip_state=None,
        chip_reduction=None,
        chip_label=None,
        chip_workdir=None,
        chip_dist_group=None,
        chip_data_group=None,
        cam_id=None,
        cam_state=None,
        cam_quality=None,
        cam_fwhm=None,
        warp_id=None,
        warp_state=None,
        dbname=None,
    ):
        self.exp_name = exp_name
        self.exp_id = exp_id
        self.dateobs = dateobs
        self.object = object
        # self.obs_mode = obs_mode
        self.visit_num = visit_num
        self.chip_id = chip_id
        self.chip_state = chip_state
        self.chip_reduction = chip_reduction
        self.chip_label = chip_label
        self.chip_workdir = chip_workdir
        self.chip_dist_group = chip_dist_group
        self.chip_data_group = chip_data_group
        self.cam_id = cam_id
        self.cam_state = cam_state
        self.cam_quality = cam_quality
        self.cam_fwhm = cam_fwhm
        self.warp_id = warp_id
        self.warp_state = warp_state
        self.dbname = dbname
        if self.dbname is None:
            self.dbname = infer_inst_from_expname(self.exp_name)
        self.max_fwhm = 12 if self.dbname == "gpc1" else 100

    def __str__(self):
        return f"<Visit {self.visit_num}: raw={self.exp_name} {self.exp_id} on {self.dateobs.strftime('%H:%M')}, chip={self.chip_id} {self.chip_state}, cam={self.cam_id} {self.cam_state}{' quality '+str(self.cam_quality) if self.cam_quality else ''}{' fwhm '+str(self.cam_fwhm) if self.cam_fwhm is not None and self.cam_fwhm>self.max_fwhm else ''}, warp={self.warp_id} {self.warp_state}>"

    def __repr__(self):
        return self.__str__()

    def is_good_quality(self):
        return self.cam_quality == 0 and self.cam_fwhm <= self.max_fwhm

    def is_poor_quality(self):
        if self.cam_quality is None:
            return False
        else:
            return self.cam_quality > 0 or self.cam_fwhm > self.max_fwhm

    def is_processed(self):
        """
        return True if the visit is processed to warp stage or terminated at cam stage due to poor quality of the exposure

        Returns
        -------
        Bool
        """
        if self.warp_state == "full":
            return True
        elif self.warp_state is None:
            if self.cam_quality is not None and self.cam_quality > 0:
                return True
            else:
                return False
        else:
            return False


class WWDiff:
    def __init__(
        self,
        diff_id,
        diff_state,
        registered,
        pub_id,
        pub_state,
        exp1: Visit,
        exp2: Visit,
    ):
        self.diff_id = diff_id
        self.diff_state = diff_state
        self.registered = registered
        self.pub_id = pub_id
        self.pub_state = pub_state
        self.exp1 = exp1
        self.exp2 = exp2

    def __str__(self):
        return f"<WWDiff {self.diff_id} {self.diff_state} registered on {self.registered.strftime('%H:%M')}: visit {self.exp1.visit_num} - visit {self.exp2.visit_num}, warp1 - warp2 = {self.exp1.warp_id} - {self.exp2.warp_id}, publish={self.pub_id} {self.pub_state}>"

    def __repr__(self):
        return self.__str__()

    def is_processed(self):
        # cannot use diff_state to determine the status because WWdiff could already be cleaned when not checking a recent night
        return self.pub_state == "full"


class Quad:
    def __init__(self, quad_name, dbname, dateobs):
        self.name = quad_name
        self.dbname = dbname
        self.dateobs = dateobs
        self.visits: List[Visit] = None
        self.wwdiffs: List[WWDiff] = None
        self.visit_nums: int = None  # number of total unique visit numbers
        self.needs_desp_diff = None
        self.is_obs_finished = None

    def __str__(self) -> str:
        return f"<Quad {self.name} {'complete' if self.is_complete() else 'incomplete'} and {self.get_proc_status()}: {self.visit_nums} visits, {len(self.wwdiffs)} WWdiffs>"

    def __repr__(self) -> str:
        return self.__str__()

    def is_complete(self):
        self.visit_nums = len(set(v.visit_num for v in self.visits))
        return self.visit_nums == 4

    def get_proc_status(self):
        # first check if all exposures are processed to warp stage or terminated at cam stage due to poor quality
        if not all([v.is_processed() for v in self.visits]):
            # in this case, simply assume that the quad does not need desperate diff to err on the side of early alerts
            self.needs_desp_diff = False
            return "partially processed"
        # then check if all expected WWdiffs are queued and published
        expected_diff_pairs = self.expected_diff_pairs()
        if len(self.wwdiffs) < len(expected_diff_pairs):
            return "partially processed"
        elif self.wwdiffs and expected_diff_pairs:
            warp_id_pairs = [(d.exp1.warp_id, d.exp2.warp_id) for d in self.wwdiffs]
            expected_warp_id_pairs = [
                (p[0].warp_id, p[1].warp_id) for p in expected_diff_pairs
            ]
            is_exepcted_diffs_made = all(
                [p in warp_id_pairs for p in expected_warp_id_pairs]
            )
            if is_exepcted_diffs_made:
                is_exepcted_diffs_done = all(
                    [
                        d.is_processed()
                        for d in self.wwdiffs
                        if (d.exp1.warp_id, d.exp2.warp_id) in expected_warp_id_pairs
                    ]
                )
                if is_exepcted_diffs_done:
                    return (
                        "processed"
                        if len(self.wwdiffs) == len(expected_diff_pairs)
                        else "over processed"
                    )
                else:
                    return "partially processed"
            else:
                return "partially processed"
        elif len(self.wwdiffs) == 0:
            return "processed"
        else:
            return "over processed"

    def expected_diff_pairs(self) -> List[Tuple[Visit, Visit]]:
        """
        Return the expected diff pairs for a quad based on the current status and determine if the quad needs desperate diff

        Returns
        -------
        list of tuples of visit1 and visit2 (Visit object) for diff pairs visit1 - visit2
        """
        not_bad_visits = [
            v for v in self.visits if v.is_good_quality() or not v.is_poor_quality()
        ]  # include the visits that are not fully processed to cam stage yet and treat them as good quality exposures
        not_bad_visits.sort(
            key=lambda v: (v.visit_num, v.dateobs)
        )  # sort by visit_num and dateobs
        not_bad_visits = {
            v.visit_num: v for v in not_bad_visits
        }  # overwrite the duplicate visits if there are any
        not_bad_visits = list(not_bad_visits.values())
        if self.is_obs_finished:
            if len(not_bad_visits) == 3 and all(
                v.is_processed() for v in not_bad_visits
            ):
                self.needs_desp_diff = True
            else:
                self.needs_desp_diff = False
        else:
            self.needs_desp_diff = False
        if self.is_obs_finished:
            # if observation is finished, make as many diff pairs as possible (allow desp. diffs)
            if len(not_bad_visits) == 4:
                return [
                    (not_bad_visits[0], not_bad_visits[1]),
                    (not_bad_visits[2], not_bad_visits[3]),
                ]
            elif len(not_bad_visits) == 3:
                return [
                    (not_bad_visits[0], not_bad_visits[1]),
                    (not_bad_visits[1], not_bad_visits[2]),
                ]
            elif len(not_bad_visits) == 2:
                return [(not_bad_visits[0], not_bad_visits[1])]
            elif len(not_bad_visits) <= 1:
                return []
        else:
            # if observation is not finished, always stick to v1-v2 and v3-v4
            not_bad_visits = {v.visit_num: v for v in not_bad_visits}
            visit1 = not_bad_visits.get(1)
            visit2 = not_bad_visits.get(2)
            visit3 = not_bad_visits.get(3)
            visit4 = not_bad_visits.get(4)
            if len(not_bad_visits) == 4:
                return [
                    (visit1, visit2),
                    (visit3, visit4),
                ]
            elif len(not_bad_visits) == 3 or len(not_bad_visits) == 2:
                if not None in (visit1, visit2):
                    return [(visit1, visit2)]
                elif not None in (visit3, visit4):
                    return [(visit3, visit4)]
                else:
                    return []
            elif len(not_bad_visits) <= 1:
                return []

    def queue_wwdiffs(self, pretend=True, verbose=False):
        """
        queue the remaining diff pairs for a quad based on the current status
        """
        expected_diff_pairs = self.expected_diff_pairs()
        diffs_to_queue = [
            pair
            for pair in expected_diff_pairs
            if pair not in [(d.exp1, d.exp2) for d in self.wwdiffs]
        ]
        count_diffs_to_queue = len(diffs_to_queue)
        count_diffs_can_be_queued = 0
        for pair in diffs_to_queue:
            if pair[0].warp_state == "full" and pair[1].warp_state == "full":
                count_diffs_can_be_queued += 1
                run_difftool_cmd = [
                    "difftool",
                    "-dbname",
                    self.dbname,
                    "-definewarpwarp",
                    "-warp_id",
                    str(pair[0].warp_id),
                    "-template_warp_id",
                    str(pair[1].warp_id),
                    "-backwards",
                    "-set_workdir",
                    pair[0].chip_workdir,
                    "-set_dist_group",
                    (
                        pair[0].chip_dist_group
                        if pair[0].chip_dist_group is not None
                        else "NULL"
                    ),
                    "-set_label",
                    pair[0].chip_label,
                    "-set_data_group",
                    (
                        pair[0].chip_data_group
                        if pair[0].chip_data_group is not None
                        else "NULL"
                    ),
                    "-set_reduction",
                    pair[0].chip_reduction,
                    "-simple",
                    "-rerun",
                    "-good_frac",
                    "0.1",
                ]
                if pretend:
                    run_difftool_cmd.append("-pretend")
                if verbose:
                    print(" ".join(run_difftool_cmd))
                try:
                    run_difftool = subprocess.run(
                        run_difftool_cmd, check=True, text=True, capture_output=True
                    )
                    if verbose:
                        print(run_difftool.stdout)
                except subprocess.CalledProcessError as e:
                    print(
                        f"Command '{' '.join(e.cmd)}' returned non-zero exit status, please check its stderr below."
                    )
                    print(e.stderr)
        return count_diffs_to_queue, count_diffs_can_be_queued


class Chunk:
    def __init__(
        self,
        chunk_name,
        dbname,
        dateobs=None,
        label=None,
        data_group=None,
        ref_exp_id=None,
    ):
        if dateobs is None:
            dateobs = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        if label is None:
            label = "%.nightlyscience"
        self.chunk_name = chunk_name
        self.dbname = dbname
        self.dateobs = dateobs
        self.label = label
        self.data_group = data_group
        self.quads: List[Quad] = None
        self._ref_exp_id = ref_exp_id
        self.last_visit: Visit = None
        self.obs_status = None
        self.done = None
        self.over_done = None
        self.not_done = None
        self.needs_desp_diff = None
        self.quads = None
        self.get_quads()

    def __str__(self) -> str:
        return f"<Chunk {self.chunk_name} {self.obs_status}: {len(self.select_quads(completed=True))}/{len(self.quads)} quads completed, {len(self.select_quads(processed=True))}/{len(self.select_quads(over_processed=True))}/{len(self.select_quads(partially_processed=True))} fully/over/partially processed, {self.dbname} on {self.dateobs}>"

    def __repr__(self) -> str:
        return self.__str__()

    def select_quads(
        self,
        completed=None,
        processed=None,
        over_processed=None,
        partially_processed=None,
    ) -> List[Quad]:
        if self.quads is None:
            self.get_quads()
        if processed:
            over_processed = partially_processed = False
        if over_processed:
            processed = partially_processed = False
        if partially_processed:
            processed = over_processed = False
        if isinstance(completed, bool):
            selected_quads = [q for q in self.quads if q.is_complete() == completed]
        elif completed is None:
            selected_quads = self.quads
        else:
            raise ValueError("completed must be a boolean or None")
        if isinstance(processed, bool):
            selected_quads = [
                q
                for q in selected_quads
                if (q.get_proc_status() == "processed") == processed
            ]
        elif processed is not None:
            raise ValueError("processed must be a boolean or None")
        if isinstance(over_processed, bool):
            selected_quads = [
                q
                for q in selected_quads
                if (q.get_proc_status() == "over processed") == over_processed
            ]
        elif over_processed is not None:
            raise ValueError("over_processed must be a boolean or None")
        if isinstance(partially_processed, bool):
            selected_quads = [
                q
                for q in selected_quads
                if (q.get_proc_status() == "partially processed") == partially_processed
            ]
        elif partially_processed is not None:
            raise ValueError("partially_processed must be a boolean or None")
        return selected_quads

    def get_obs_status(self):
        if self._ref_exp_id is None:
            self._ref_exp_id = Night._get_first_last_exp_id(
                dbname=self.dbname, dateobs=self.dateobs
            )
        if None in self._ref_exp_id:
            return None
        # check if the chunk is initialized
        if self.quads is None:
            # get_quads() will call get_obs_status(), so no need to proceed
            self.get_quads()
            return None
        if len(self.quads) == 0:
            return None
        # if the last exposure of the night does not belong to the chunk, then the chunk has finished observing
        if self._ref_exp_id[1] > self.last_visit.exp_id:
            is_obs_finished = True
        else:
            # extra check for cases when the nightly observation stopped at the last exposure of the chunk
            # for various reasons e.g. bad weather, technical issues, etc. in that case if the observation
            # is paused for more than 30 minutes, the chunk observation is considered finished (abandoned).
            if (datetime.now(timezone.utc) - self.last_visit.dateobs) > timedelta(
                minutes=30
            ):
                is_obs_finished = True
            else:
                is_obs_finished = False
        for q in self.quads:
            q.is_obs_finished = is_obs_finished
        is_all_quads_completed = all([q.is_complete() for q in self.quads])
        if is_obs_finished:
            if is_all_quads_completed:
                self.obs_status = "complete"
            else:
                self.obs_status = "truncated"
        else:
            self.obs_status = "in progress"

    def get_proc_status(self):
        # check if the chunk is initialized
        if self.quads is None:
            # get_quads() will call get_proc_status(), so no need to proceed
            self.get_quads()
            return None
        if len(self.quads) == 0:
            return None
        quad_proc_status = [q.get_proc_status() for q in self.quads]
        if "partially processed" in quad_proc_status:
            self.not_done = True
        else:
            self.not_done = False
        if "over processed" in quad_proc_status:
            self.over_done = True
        else:
            self.over_done = False
        if self.not_done or self.over_done:
            self.done = False
        else:
            self.done = True
        if self.obs_status is None:
            self.get_obs_status()
        # only after the chunk observation is finished, we can determine if the chunk needs desperate diff
        # otherwise, we assume that it does not need desperate diff
        if self.obs_status == "in progress":
            self.needs_desp_diff = False
        else:
            self.needs_desp_diff = any([q.needs_desp_diff for q in self.quads])

    def get_quads(self):
        # print(self.chunk_name)
        """query for the up to date quads in the chunk"""
        if self._ref_exp_id is None:
            self._ref_exp_id = Night._get_first_last_exp_id(
                dbname=self.dbname, dateobs=self.dateobs
            )
        # query for exposures and their processing status from chip to warp stage
        query = f"""
        select exp_name, exp_id, dateobs, object, substring_index(comment,' ',-1) visit,
        chip_id, chipRun.state chip_state, chipRun.reduction chip_reduction, chipRun.label chip_label, chipRun.workdir chip_workdir,
        chipRun.dist_group chip_dist_group, chipRun.data_group chip_data_group,
        cam_id, camRun.state cam_state, camProcessedExp.quality cam_quality, camProcessedExp.fwhm_major cam_fwhm_major,
        warp_id, warpRun.state warp_state 
        from rawExp 
        left join chipRun using (exp_id)
        left join camRun using (chip_id) 
        left join camProcessedExp using (cam_id)
        left join fakeRun using (cam_id) 
        left join warpRun using (fake_id) 
        where exp_id between {self._ref_exp_id[0]} and {self._ref_exp_id[1]} and dateobs like '{self.dateobs}%'
        and (obs_mode like '%SS%' or obs_mode like '%BRIGHT%') and obs_mode not like 'ENGINEERING' and obs_mode not like 'MANUAL'
        and exp_type like "OBJECT" and comment like "{self.chunk_name}% visit _"
        and (chipRun.label is NULL or chipRun.label like "{self.label}" or 
        camRun.label like "{self.label}" or warpRun.label like "{self.label}")
        """
        if self.data_group is not None:
            query += f"and chipRun.data_group like '{self.data_group}'"
        query += f"order by dateobs"
        db_conn = MySQLdb.connect(
            host=SCIDBS1_HOST,
            db=self.dbname,
            user=SCIDBS1_USER,
            passwd=SCIDBS1_PSW,
        )
        db_cursor = db_conn.cursor()
        db_cursor.execute(query)
        result = db_cursor.fetchall()
        if result:
            self.label = result[0][8]
            quad_names = set(r[3] for r in result)
            self.quads = [
                Quad(q, dbname=self.dbname, dateobs=self.dateobs) for q in quad_names
            ]
            visits = [
                Visit(
                    exp_name=r[0],
                    exp_id=r[1],
                    dateobs=r[2].replace(tzinfo=timezone.utc),
                    object=r[3],
                    visit_num=int(r[4]),
                    chip_id=r[5],
                    chip_state=r[6],
                    chip_reduction=r[7],
                    chip_label=r[8],
                    chip_workdir=r[9],
                    chip_dist_group=r[10],
                    chip_data_group=r[11],
                    cam_id=r[12],
                    cam_state=r[13],
                    cam_quality=r[14],
                    cam_fwhm=r[15],
                    warp_id=r[16],
                    warp_state=r[17],
                    dbname=self.dbname,
                )
                for r in result
            ]
            for quad in self.quads:
                quad.visits = [v for v in visits if v.object == quad.name]
            self.last_visit = visits[-1]
        else:
            db_cursor.close()
            db_conn.close()
            self.quads = []
            self.get_obs_status()
            self.get_proc_status()
            return None

        # query for WWdiffs
        # check if there are any visits of the chunk have been processed to warp stage
        # if not, then no need to query for WWdiffs
        warp_ids = [v.warp_id for v in visits]
        if not any(warp_ids):
            for quad in self.quads:
                quad.wwdiffs = []
            db_cursor.close()
            db_conn.close()
            self.get_obs_status()
            self.get_proc_status()
            return None
        warp_ids_for_query = [i for i in warp_ids if i is not None]
        # below is a dirty and lazy way to get the query to work when there is only one warp_id
        # str(tuple(warp_ids_for_query)) will return (id,) instead of (id), which breaks the mysql query.
        # it will not produce duplicate rows in the query result since the *in* clause is equivalent to
        # warp_id = id1 or warp_id = id2 or ...
        if len(warp_ids_for_query) == 1:
            warp_ids_for_query.append(warp_ids_for_query[0])
        # I found that query for diffInputSkyfile with warp_ids first then join other tables is as efficient as
        # limiting diff% tables with max and min diff_id on the given night first and then querying them with warp_ids
        # So, I am using the former method here. This provides extra benefit that we can check for diffs
        # that are not processed in time, e.g., delayed until another UTC date. Moreover, this saves the query time for max/min diff_ids.
        query = f"""
        select diff_id, diff_state, warp1, warp2, registered, pub_id, publishRun.state pub_state 
        from (
            select diff_id, state diff_state, warp1, warp2, registered from diffInputSkyfile
            left join diffRun using (diff_id) 
            where warp1 in {tuple(warp_ids_for_query)} and stack2 is NULL group by diff_id
            ) as WWdiff 
            left join publishRun on (diff_id = stage_id)
        """
        if self.label.endswith(".nightlyscience"):
            query += f" where client_id is NULL or client_id = {17 if self.dbname == 'gpc1' else 3}"
        # print(query)
        db_cursor.execute(query)
        result = db_cursor.fetchall()
        db_cursor.close()
        db_conn.close()
        if result:
            wwdiffs = [
                WWDiff(
                    diff_id=r[0],
                    diff_state=r[1],
                    registered=r[4].replace(tzinfo=timezone.utc),
                    pub_id=r[5],
                    pub_state=r[6],
                    exp1=visits[warp_ids.index(r[2])],
                    exp2=visits[warp_ids.index(r[3])],
                )
                for r in result
            ]
            for quad in self.quads:
                quad.wwdiffs = [
                    wwdiff
                    for wwdiff in wwdiffs
                    if wwdiff.exp1.object == quad.name
                    or wwdiff.exp2.object == quad.name
                ]
        else:
            for quad in self.quads:
                quad.wwdiffs = []
        self.get_obs_status()
        self.get_proc_status()

    def queue_wwdiffs(self, pretend=True):
        count_diffs_to_queue = 0
        count_diffs_can_be_queued = 0
        for quad in self.quads:
            count_diffs_to_queue += quad.queue_wwdiffs(pretend=pretend)[0]
            count_diffs_can_be_queued += quad.queue_wwdiffs(pretend=pretend)[1]
        return count_diffs_to_queue, count_diffs_can_be_queued


class Night:
    def __init__(self, dateobs=None, dbname="gpc1"):
        if dateobs is None:
            dateobs = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        self.dateobs = dateobs
        self.dbname = dbname
        self._first_exp_id, self._last_exp_id = self._get_first_last_exp_id(
            self.dateobs, self.dbname
        )
        # self._first_diff_id, self._last_diff_id = self._get_first_last_diff_id(
        #     self.dateobs, self.dbname
        # )
        self.chunks: List[Chunk] = None
        self.get_chunks()

    def __str__(self) -> str:
        return f"<Night {self.dateobs} in {self.dbname}: {len(self.chunks)} chunks>"

    def __repr__(self) -> str:
        return self.__str__()

    def get_chunks(self):
        """Get all chunks of the night."""
        if self._first_exp_id is None or self._last_exp_id is None:
            self.chunks = []
            return None
        query = f"""
        select exp_name, exp_id, substring_index(comment,' ',1) as chunk_name from rawExp
        where exp_id between {self._first_exp_id} and {self._last_exp_id}
        and (obs_mode like '%SS%' or obs_mode like '%BRIGHT%') and obs_mode not like 'ENGINEERING' and obs_mode not like 'MANUAL'
        and exp_type like "OBJECT" and comment like '%visit%' 
        group by chunk_name 
        order by dateobs
        """
        db_conn = MySQLdb.connect(
            host=SCIDBS1_HOST,
            db=self.dbname,
            user=SCIDBS1_USER,
            passwd=SCIDBS1_PSW,
        )
        db_cursor = db_conn.cursor()
        db_cursor.execute(query)
        result = db_cursor.fetchall()
        db_cursor.close()
        db_conn.close()
        if result:
            chunk_names = [r[2] for r in result]
            self.chunks = [
                Chunk(
                    c,
                    dbname=self.dbname,
                    dateobs=self.dateobs,
                    ref_exp_id=(self._first_exp_id, self._last_exp_id),
                )
                for c in chunk_names
            ]
        else:
            self.chunks = []

    @staticmethod
    def _get_first_last_exp_id(dateobs, dbname):
        query = f"""
        select min(exp_id), max(exp_id) from rawExp where dateobs like '{dateobs}%'
        """
        db_conn = MySQLdb.connect(
            host=SCIDBS1_HOST,
            db=dbname,
            user=SCIDBS1_USER,
            passwd=SCIDBS1_PSW,
        )
        db_cursor = db_conn.cursor()
        db_cursor.execute(query)
        result = db_cursor.fetchone()
        db_cursor.close()
        db_conn.close()
        if result:
            return result[0], result[1]
        else:
            return None, None

    @staticmethod
    def _get_first_last_diff_id(dateobs, dbname):
        query = f"""
        select min(diff_id), max(diff_id) from diffRun where registered like '{dateobs}%'
        """
        db_conn = MySQLdb.connect(
            host=SCIDBS1_HOST,
            db=dbname,
            user=SCIDBS1_USER,
            passwd=SCIDBS1_PSW,
        )
        db_cursor = db_conn.cursor()
        db_cursor.execute(query)
        result = db_cursor.fetchone()
        if result:
            return result[0], result[1]
        else:
            return None, None
