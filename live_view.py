"""Live view of a running Spark job: a scrolling timeline of tasks, one lane per executor.

Top chart    one horizontal bar per task, in the lane of the executor that ran it.
             green = completed, blue = running, red = failed/killed.
             Dotted lines mark where each stage began. A red line marks a lost executor.
Bottom chart running tasks (blue) vs. total cores (dashed) over time, i.e. how busy the cluster is.

Run on the host while a job is running:  python live_view.py   (python3 on Mac)
Needs: pip install requests matplotlib
"""
import time
import requests
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle

BASE = "http://localhost:4040/api/v1"
COLORS = {"done": "#2e9e6b", "run": "#3b6fd4", "fail": "#e4572e"}
WINDOW = 45.0    # seconds of history on screen
POLL = 0.5       # seconds between polls


def get(path):
    r = requests.get(BASE + path, timeout=2)
    r.raise_for_status()
    return r.json()


def kind(status):
    if status == "SUCCESS":
        return "done"
    if status == "RUNNING":
        return "run"
    if status in ("FAILED", "KILLED"):
        return "fail"
    return None   # pending / unknown: not drawn


class Tracker:
    """Builds the timeline from successive polls. All times come from this machine's clock,
    so there is no clock skew between the host and the containers."""

    def __init__(self, app):
        self.app = app
        self.tasks = {}        # (stage, attempt, taskId) -> {"ex", "start", "end", "kind"}
        self.stage_sig = {}    # last-seen counters per stage, to skip unchanged stages
        self.dead_at = {}      # executor id -> time we noticed it die (None = already dead at start)
        self.history = []      # (time, running tasks, total cores)
        self.last_active = time.time()
        self.view_now = time.time()
        self.first = True
        self.execs = []
        self.running = self.cores = self.queued = self.done = self.failed = 0

    def poll(self):
        now = time.time()
        stages = get(f"/applications/{self.app}/stages")
        execs = [e for e in get(f"/applications/{self.app}/allexecutors") if e["id"] != "driver"]
        self.execs = sorted(execs, key=lambda e: int(e["id"]))

        for e in self.execs:
            if not e["isActive"] and e["id"] not in self.dead_at:
                self.dead_at[e["id"]] = None if self.first else now

        for s in stages:
            key = (s["stageId"], s["attemptId"])
            sig = (s["status"], s.get("numCompleteTasks"), s.get("numFailedTasks"),
                   s.get("numKilledTasks"), s.get("numActiveTasks"))
            if self.first and s["status"] != "ACTIVE":
                self.stage_sig[key] = sig       # history from before we started watching
                continue
            if s["status"] != "ACTIVE" and self.stage_sig.get(key) == sig:
                continue
            self.stage_sig[key] = sig
            tl = get(f"/applications/{self.app}/stages/{key[0]}/{key[1]}/taskList?length=5000")
            for t in tl:
                self._update(key, t, now)

        self.first = False
        self.running = sum(1 for r in self.tasks.values() if r["kind"] == "run")
        self.cores = sum(e.get("totalCores", 0) for e in self.execs if e["isActive"])
        self.done = sum(e.get("completedTasks", 0) for e in self.execs)
        self.failed = sum(e.get("failedTasks", 0) for e in self.execs)
        self.queued = sum(max(0, s["numTasks"] - s.get("numCompleteTasks", 0) - s.get("numActiveTasks", 0))
                          for s in stages if s["status"] == "ACTIVE")

        active = self.running > 0 or any(s["status"] == "ACTIVE" for s in stages)
        if active:
            self.last_active = now
        self.view_now = now if active else self.last_active   # freeze the picture when the job is over
        self.history.append((now, self.running, self.cores))
        self._prune(now)

    def _update(self, stage_key, t, now):
        k = kind(t["status"])
        if k is None:
            return
        key = (*stage_key, t["taskId"])
        rec = self.tasks.get(key)
        if rec is None:
            rec = {"ex": t.get("executorId"), "start": now, "end": None, "kind": k}
            self.tasks[key] = rec
        if k == "run":
            rec["kind"], rec["end"] = "run", None
        else:
            if rec["end"] is None:
                rec["end"] = now
            rec["kind"] = k
            if t.get("duration") is not None:                # duration is relative, so it is skew-free
                rec["start"] = rec["end"] - t["duration"] / 1000.0

    def _prune(self, now):
        cutoff = now - WINDOW - 30
        self.tasks = {k: r for k, r in self.tasks.items() if r["end"] is None or r["end"] > cutoff}
        self.history = [h for h in self.history if h[0] > cutoff]


def draw(tr, fig, ax1, ax2):
    vn = tr.view_now
    lanes = {e["id"]: i for i, e in enumerate(tr.execs)}
    n = max(1, len(lanes))
    ax1.clear()
    ax2.clear()

    # Grey out lanes of dead executors from the moment they were lost
    for e in tr.execs:
        if not e["isActive"]:
            t = tr.dead_at.get(e["id"])
            x = -WINDOW if t is None else max(-WINDOW, t - vn)
            ax1.add_patch(Rectangle((x, lanes[e["id"]] - 0.5), 0.5 - x, 1, color="#e6e6e6", zorder=0))

    # Task bars
    for kd in ("done", "fail", "run"):
        rows = [(lanes[r["ex"]], r["start"] - vn, (vn if r["end"] is None else r["end"]) - r["start"])
                for r in tr.tasks.values()
                if r["kind"] == kd and r["ex"] in lanes and (r["end"] is None or r["end"] > vn - WINDOW)]
        if rows:
            ys, lefts, widths = zip(*rows)
            ax1.barh(ys, [max(w, 0.12) for w in widths], left=lefts, height=0.72,
                     color=COLORS[kd], edgecolor="white", linewidth=0.4, zorder=2)

    # Stage start markers
    starts = {}
    for (sid, _att, _tid), r in tr.tasks.items():
        starts[sid] = min(starts.get(sid, r["start"]), r["start"])
    for sid, st in starts.items():
        x = st - vn
        if -WINDOW < x <= 0:
            ax1.axvline(x, color="#999", ls=":", lw=1, zorder=1)
            ax1.text(x + 0.2, -0.9, f"stage {sid}", fontsize=8, color="#777", va="center")

    # Lost-executor markers
    for eid, t in tr.dead_at.items():
        if t is None:
            continue
        x = t - vn
        if -WINDOW < x <= 0.5:
            for a in (ax1, ax2):
                a.axvline(x, color="#c0392b", lw=2, alpha=0.85, zorder=3)
            ax1.text(x + 0.2, n - 0.6, f"exec {eid} lost", color="#c0392b", fontsize=9,
                     fontweight="bold", va="bottom")

    ax1.set_xlim(-WINDOW, 0.5)
    ax1.set_ylim(n - 0.5, -1.3)
    ax1.set_yticks(range(n))
    ax1.set_yticklabels([f"exec {e['id']}" + ("" if e["isActive"] else " (DEAD)") for e in tr.execs])
    for lbl, e in zip(ax1.get_yticklabels(), tr.execs):
        lbl.set_color("#555" if e["isActive"] else "#c0392b")
    ax1.set_xticklabels([])
    for side in ("top", "right"):
        ax1.spines[side].set_visible(False)

    # Utilization
    h = [(t - vn, r, c) for t, r, c in tr.history if t > vn - WINDOW - 2]
    if h:
        xs, rs, cs = zip(*h)
        ax2.fill_between(xs, rs, step="post", color=COLORS["run"], alpha=0.3)
        ax2.step(xs, rs, where="post", color=COLORS["run"], lw=2)
        ax2.step(xs, cs, where="post", color="#555", ls="--", lw=1.5)
    ax2.set_xlim(-WINDOW, 0.5)
    ax2.set_ylim(0, max([c for _, _, c in tr.history] + [1]) + 1)
    ax2.set_ylabel("busy cores", fontsize=9)
    ax2.set_xlabel("seconds ago")
    for side in ("top", "right"):
        ax2.spines[side].set_visible(False)

    alive = sum(1 for e in tr.execs if e["isActive"])
    fig.suptitle(f"executors alive: {alive}/{len(tr.execs)}   |   running: {tr.running} of {tr.cores} cores   |   "
                 f"queued ~ {tr.queued}   |   completed: {tr.done}   failed: {tr.failed}", fontsize=12)


plt.ion()
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7), gridspec_kw={"height_ratios": [3, 1]})
fig.subplots_adjust(left=0.08, right=0.98, top=0.9, bottom=0.14, hspace=0.2)
fig.legend(handles=[Line2D([], [], color=COLORS["done"], lw=6, label="completed task"),
                    Line2D([], [], color=COLORS["run"], lw=6, label="running task"),
                    Line2D([], [], color=COLORS["fail"], lw=6, label="failed / killed task"),
                    Line2D([], [], color="#555", lw=2, ls="--", label="total cores")],
           loc="lower center", ncol=4, frameon=False)
fig.suptitle("Waiting for a Spark job on localhost:4040 ...", fontsize=12)

plt.show(block=False)   # show the window once; the loop below never re-raises it

tracker = None
while plt.fignum_exists(fig.number):
    try:
        app = get("/applications")[0]["id"]
        if tracker is None or tracker.app != app:
            tracker = Tracker(app)
        tracker.poll()
        draw(tracker, fig, ax1, ax2)
    except Exception:
        pass   # job not started yet, or just ended: keep showing the last frame
    # Not plt.pause(): on macOS it calls show() every time, which pops the window back to the front.
    fig.canvas.draw_idle()
    fig.canvas.start_event_loop(POLL)