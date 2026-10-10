import os
import time
import tracemalloc
import threading
from queue import Queue
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class PureSequence:
    seq_id: int
    seq: List[int]
    ulist: List[float]
    rlist: List[float]
    swu: float

    def next_itemset_start_after(self, index: int) -> int:
        pos = index + 1
        while pos < len(self.seq) and self.seq[pos] != 0:
            pos += 1
        return pos + 1


@dataclass
class IndexEntry:
    index: int
    utility: float


class MemoryLogger:
    _instance = None

    def __init__(self):
        self.max_memory = 0.0

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = MemoryLogger()
        return cls._instance

    def reset(self):
        self.max_memory = 0.0
        if not tracemalloc.is_tracing():
            tracemalloc.start()

    def check_memory(self):
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        current, _ = tracemalloc.get_traced_memory()
        current_mb = current / 1024.0 / 1024.0
        if current_mb > self.max_memory:
            self.max_memory = current_mb
        return current_mb

    def get_max_memory(self):
        return self.max_memory


class AlgoAHUS:
    def __init__(self):
        self.min_utility = 0.0
        self.max_pattern_length = 2**31 - 1
        self.sequence_count = 0
        self.start_timestamp = 0
        self.end_timestamp = 0
        self.candidate_count = 0
        self.pattern_count = 0
        self.database: List[PureSequence] = []
        self.database_by_sid: Dict[int, PureSequence] = {}
        self.external_utility: Dict[int, float] = {}
        self.map_item_to_swu: Dict[int, float] = {}
        self.promising_items: set[int] = set()
        self.one_item_peu: Dict[int, float] = {}
        self.high_utility_patterns: List[Tuple[Tuple[int, ...], float]] = []

    def set_max_pattern_length(self, max_pattern_length: int):
        if max_pattern_length < 1:
            raise ValueError("max_pattern_length must be at least 1")
        self.max_pattern_length = max_pattern_length

    @staticmethod
    def _current_time_millis() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _parse_external_utilities(path: str) -> Dict[int, float]:
        utilities: Dict[int, float] = {}
        with open(path, "r", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                key, value = line.split(":", 1)
                item = int(key.strip())
                utility = float(value.strip())
                if utility < 0:
                    raise ValueError(
                        f"AHUS is reconstructed for the standard positive-utility HUSPM setting. "
                        f"Negative external utility found for item {item}: {utility}."
                    )
                utilities[item] = utility
        return utilities

    @staticmethod
    def _parse_item_token(token: str) -> Tuple[int, float]:
        left = token.index("[")
        right = token.index("]")
        return int(token[:left]), float(token[left + 1:right])

    @staticmethod
    def _parse_sequence_utility(tokens: List[str]) -> Optional[float]:
        for token in reversed(tokens):
            if token.startswith("SUtility:"):
                return float(token.split(":", 1)[1])
        return None

    def _parse_sequence_line(self, line: str) -> Tuple[List[List[Tuple[int, float]]], float]:
        tokens = line.split()
        declared_su = self._parse_sequence_utility(tokens)
        itemsets: List[List[Tuple[int, float]]] = []
        current: List[Tuple[int, float]] = []
        calculated_su = 0.0

        for token in tokens:
            if token == "-1":
                if current:
                    current.sort(key=lambda x: x[0])
                    itemsets.append(current)
                    current = []
            elif token == "-2":
                break
            elif token.startswith("SUtility:"):
                break
            elif token and not token.startswith("-"):
                item, quantity = self._parse_item_token(token)
                if item not in self.external_utility:
                    raise KeyError(f"External utility for item {item} is missing")
                utility = quantity * self.external_utility[item]
                calculated_su += utility
                current.append((item, utility))

        if current:
            current.sort(key=lambda x: x[0])
            itemsets.append(current)

        return itemsets, (declared_su if declared_su is not None else calculated_su)

    def load_data_with_internal_external(
        self,
        external_utility_path: str,
        input_path: str,
        min_utility: float,
        min_utility_is_ratio: bool = False,
    ) -> List[PureSequence]:
        self.external_utility = self._parse_external_utilities(external_utility_path)
        self.map_item_to_swu = {}
        self.sequence_count = 0
        total_sequence_utility = 0.0
        parsed_sequences: List[Tuple[List[List[Tuple[int, float]]], float]] = []

        with open(input_path, "r", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line or line[0] in "#%@":
                    continue
                itemsets, sequence_utility = self._parse_sequence_line(line)
                parsed_sequences.append((itemsets, sequence_utility))
                total_sequence_utility += sequence_utility
                seen_items = set()
                for itemset in itemsets:
                    for item, _ in itemset:
                        seen_items.add(item)
                for item in seen_items:
                    self.map_item_to_swu[item] = self.map_item_to_swu.get(item, 0.0) + sequence_utility
                self.sequence_count += 1

        self.min_utility = float(min_utility) * total_sequence_utility if min_utility_is_ratio else float(min_utility)
        self.promising_items = {item for item, swu in self.map_item_to_swu.items() if swu >= self.min_utility}

        database: List[PureSequence] = []
        sid = 1
        for itemsets, _ in parsed_sequences:
            seq: List[int] = []
            ulist: List[float] = []
            sanitized_utility = 0.0

            for itemset in itemsets:
                kept = [(item, utility) for item, utility in itemset if item in self.promising_items]
                if not kept:
                    continue
                kept.sort(key=lambda x: x[0])
                for item, utility in kept:
                    seq.append(item)
                    ulist.append(float(utility))
                    sanitized_utility += float(utility)
                seq.append(0)
                ulist.append(0.0)

            if not seq:
                continue

            rlist = [0.0] * len(seq)
            remaining = 0.0
            for pos in range(len(seq) - 1, -1, -1):
                if seq[pos] == 0:
                    rlist[pos] = 0.0
                else:
                    rlist[pos] = remaining
                    remaining += ulist[pos]

            database.append(PureSequence(sid, seq, ulist, rlist, sanitized_utility))
            sid += 1

        self.database = database
        self.database_by_sid = {sequence.seq_id: sequence for sequence in database}
        return database

    def run_algorithm(self, output_path: Optional[str] = None):
        MemoryLogger.get_instance().reset()
        self.start_timestamp = self._current_time_millis()
        self.high_utility_patterns = []
        self.pattern_count = 0
        self.candidate_count = 0
        self.one_item_peu = {}

        root_entries_by_item: Dict[int, Dict[int, Dict[int, float]]] = {}

        for sequence in self.database:
            peu_by_item_in_sequence: Dict[int, float] = {}
            for index, item in enumerate(sequence.seq):
                if item == 0:
                    continue
                utility = sequence.ulist[index]
                root_entries_by_item.setdefault(item, {}).setdefault(sequence.seq_id, {})[index] = utility
                candidate_peu = utility + sequence.rlist[index] if sequence.rlist[index] > 0 else 0.0
                if candidate_peu > peu_by_item_in_sequence.get(item, 0.0):
                    peu_by_item_in_sequence[item] = candidate_peu
            for item, value in peu_by_item_in_sequence.items():
                self.one_item_peu[item] = self.one_item_peu.get(item, 0.0) + value

        for item in sorted(root_entries_by_item):
            iulist = self._normalize_iulist(root_entries_by_item[item])
            asu = self._actual_sequence_utility(iulist)
            pattern = (item,)
            if asu >= self.min_utility:
                self._save_pattern(pattern, asu)
            if self._peu(iulist) >= self.min_utility and 1 < self.max_pattern_length:
                self._ahus(pattern, iulist, asu, 1)

        self.end_timestamp = self._current_time_millis()
        MemoryLogger.get_instance().check_memory()
        if output_path is not None:
            self.write_output(output_path)
        return self.high_utility_patterns

    @staticmethod
    def _normalize_iulist(raw: Dict[int, Dict[int, float]]) -> Dict[int, List[IndexEntry]]:
        result: Dict[int, List[IndexEntry]] = {}
        for sid, pos_to_utility in raw.items():
            result[sid] = [IndexEntry(pos, util) for pos, util in sorted(pos_to_utility.items())]
        return result

    @staticmethod
    def _actual_sequence_utility(iulist: Dict[int, List[IndexEntry]]) -> float:
        total = 0.0
        for entries in iulist.values():
            if entries:
                total += max(entry.utility for entry in entries)
        return total

    def _peu(self, iulist: Dict[int, List[IndexEntry]]) -> float:
        total = 0.0
        for sid, entries in iulist.items():
            sequence = self.database_by_sid[sid]
            best = 0.0
            for entry in entries:
                remaining = sequence.rlist[entry.index]
                value = entry.utility + remaining if remaining > 0 else 0.0
                if value > best:
                    best = value
            total += best
        return total

    @staticmethod
    def _last_item(pattern: Tuple[int, ...]) -> int:
        for value in reversed(pattern):
            if value != -1:
                return value
        raise ValueError("Pattern has no item")

    def _ahus(
        self,
        pattern: Tuple[int, ...],
        iulist: Dict[int, List[IndexEntry]],
        asu: float,
        item_count: int,
    ):
        if self._peu(iulist) < self.min_utility:
            return

        last_item = self._last_item(pattern)
        raw_i_entries: Dict[int, Dict[int, Dict[int, float]]] = {}
        raw_s_entries: Dict[int, Dict[int, Dict[int, float]]] = {}
        rsu_i: Dict[int, float] = {}
        rsu_s: Dict[int, float] = {}

        for sid, entries in iulist.items():
            sequence = self.database_by_sid[sid]
            parent_peu = 0.0
            for entry in entries:
                remaining = sequence.rlist[entry.index]
                value = entry.utility + remaining if remaining > 0 else 0.0
                if value > parent_peu:
                    parent_peu = value

            seen_i = set()
            seen_s = set()

            for entry in entries:
                index = entry.index
                base_utility = entry.utility

                pos = index + 1
                while pos < len(sequence.seq) and sequence.seq[pos] != 0:
                    item = sequence.seq[pos]
                    if item > last_item and asu + self.one_item_peu.get(item, 0.0) >= self.min_utility:
                        new_utility = base_utility + sequence.ulist[pos]
                        current = raw_i_entries.setdefault(item, {}).setdefault(sid, {}).get(pos)
                        if current is None or new_utility > current:
                            raw_i_entries[item][sid][pos] = new_utility
                        seen_i.add(item)
                    pos += 1

                pos = sequence.next_itemset_start_after(index)
                while pos < len(sequence.seq):
                    item = sequence.seq[pos]
                    if item != 0 and asu + self.one_item_peu.get(item, 0.0) >= self.min_utility:
                        new_utility = base_utility + sequence.ulist[pos]
                        current = raw_s_entries.setdefault(item, {}).setdefault(sid, {}).get(pos)
                        if current is None or new_utility > current:
                            raw_s_entries[item][sid][pos] = new_utility
                        seen_s.add(item)
                    pos += 1

            for item in seen_i:
                rsu_i[item] = rsu_i.get(item, 0.0) + parent_peu
            for item in seen_s:
                rsu_s[item] = rsu_s.get(item, 0.0) + parent_peu

        for item in sorted(raw_i_entries):
            if rsu_i.get(item, 0.0) < self.min_utility:
                continue
            self.candidate_count += 1
            new_iulist = self._normalize_iulist(raw_i_entries[item])
            new_asu = self._actual_sequence_utility(new_iulist)
            new_pattern = pattern + (item,)
            if new_asu >= self.min_utility:
                self._save_pattern(new_pattern, new_asu)
            if item_count + 1 < self.max_pattern_length and self._peu(new_iulist) >= self.min_utility:
                self._ahus(new_pattern, new_iulist, new_asu, item_count + 1)

        for item in sorted(raw_s_entries):
            if rsu_s.get(item, 0.0) < self.min_utility:
                continue
            self.candidate_count += 1
            new_iulist = self._normalize_iulist(raw_s_entries[item])
            new_asu = self._actual_sequence_utility(new_iulist)
            new_pattern = pattern + (-1, item)
            if new_asu >= self.min_utility:
                self._save_pattern(new_pattern, new_asu)
            if item_count + 1 < self.max_pattern_length and self._peu(new_iulist) >= self.min_utility:
                self._ahus(new_pattern, new_iulist, new_asu, item_count + 1)

    def _save_pattern(self, pattern: Tuple[int, ...], utility: float):
        self.high_utility_patterns.append((pattern, float(utility)))
        self.pattern_count += 1

    @staticmethod
    def _format_number(value: float) -> str:
        if abs(value - round(value)) < 1e-9:
            return str(int(round(value)))
        return f"{value:.10f}".rstrip("0").rstrip(".")

    @staticmethod
    def pattern_to_string(pattern: Tuple[int, ...], utility: float) -> str:
        itemsets: List[List[int]] = [[]]
        for token in pattern:
            if token == -1:
                itemsets.append([])
            else:
                itemsets[-1].append(token)
        inside = "".join("(" + " ".join(str(item) for item in itemset) + ")" for itemset in itemsets if itemset)
        return f"<{inside}>:{AlgoAHUS._format_number(utility)}"

    def write_output(self, output_path: str):
        with open(output_path, "w", encoding="utf-8", newline="") as writer:
            for pattern, utility in self.high_utility_patterns:
                writer.write(self.pattern_to_string(pattern, utility))
                writer.write(os.linesep)

    def print_stats(self):
        print("=============  AHUS RECONSTRUCTED - STATS =============")
        print(f" Sequences count : {self.sequence_count}")
        print(f" Minimum utility : {self._format_number(self.min_utility)}")
        print(f" Promising items : {len(self.promising_items)}")
        print(f" High-utility sequential patterns : {self.pattern_count}")
        print(f" Candidate extensions after pruning : {self.candidate_count}")
        print(f" Maximum memory usage : {MemoryLogger.get_instance().get_max_memory()} MB")
        print(f" Total time ~ : {self.end_timestamp - self.start_timestamp} ms")
        print("=======================================================")


class AlgoAHUSP(AlgoAHUS):
    def __init__(self):
        super().__init__()
        self.max_workers = max(1, (os.cpu_count() or 1))
        self._result_lock = threading.RLock()

    def set_max_workers(self, max_workers: int):
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self.max_workers = max_workers

    def run_algorithm(self, output_path: Optional[str] = None):
        MemoryLogger.get_instance().reset()
        self.start_timestamp = self._current_time_millis()
        self.high_utility_patterns = []
        self.pattern_count = 0
        self.candidate_count = 0
        self.one_item_peu = {}
        self._result_lock = threading.RLock()

        root_entries_by_item: Dict[int, Dict[int, Dict[int, float]]] = {}

        for sequence in self.database:
            peu_by_item_in_sequence: Dict[int, float] = {}
            for index, item in enumerate(sequence.seq):
                if item == 0:
                    continue
                utility = sequence.ulist[index]
                root_entries_by_item.setdefault(item, {}).setdefault(sequence.seq_id, {})[index] = utility
                candidate_peu = utility + sequence.rlist[index] if sequence.rlist[index] > 0 else 0.0
                if candidate_peu > peu_by_item_in_sequence.get(item, 0.0):
                    peu_by_item_in_sequence[item] = candidate_peu
            for item, value in peu_by_item_in_sequence.items():
                self.one_item_peu[item] = self.one_item_peu.get(item, 0.0) + value

        task_queue: Queue = Queue()

        for item in sorted(root_entries_by_item):
            iulist = self._normalize_iulist(root_entries_by_item[item])
            asu = self._actual_sequence_utility(iulist)
            pattern = (item,)
            if asu >= self.min_utility:
                self._save_pattern_threadsafe(pattern, asu)
            if self._peu(iulist) >= self.min_utility and 1 < self.max_pattern_length:
                task_queue.put((pattern, iulist, asu, 1))

        workers = []
        for _ in range(self.max_workers):
            worker = threading.Thread(target=self._worker_loop, args=(task_queue,), daemon=True)
            worker.start()
            workers.append(worker)

        task_queue.join()

        for _ in workers:
            task_queue.put(None)
        for worker in workers:
            worker.join()

        self.high_utility_patterns.sort(key=lambda entry: (entry[0], entry[1]))
        self.pattern_count = len(self.high_utility_patterns)
        self.end_timestamp = self._current_time_millis()
        MemoryLogger.get_instance().check_memory()
        if output_path is not None:
            self.write_output(output_path)
        return self.high_utility_patterns

    def _worker_loop(self, task_queue: Queue):
        while True:
            task = task_queue.get()
            try:
                if task is None:
                    return
                pattern, iulist, asu, item_count = task
                self._ahus_parallel_step(pattern, iulist, asu, item_count, task_queue)
            finally:
                task_queue.task_done()

    def _increment_candidate_count(self):
        with self._result_lock:
            self.candidate_count += 1

    def _save_pattern_threadsafe(self, pattern: Tuple[int, ...], utility: float):
        with self._result_lock:
            self.high_utility_patterns.append((pattern, float(utility)))
            self.pattern_count += 1

    def _ahus_parallel_step(
        self,
        pattern: Tuple[int, ...],
        iulist: Dict[int, List[IndexEntry]],
        asu: float,
        item_count: int,
        task_queue: Queue,
    ):
        if self._peu(iulist) < self.min_utility:
            return

        last_item = self._last_item(pattern)
        raw_i_entries: Dict[int, Dict[int, Dict[int, float]]] = {}
        raw_s_entries: Dict[int, Dict[int, Dict[int, float]]] = {}
        rsu_i: Dict[int, float] = {}
        rsu_s: Dict[int, float] = {}

        for sid, entries in iulist.items():
            sequence = self.database_by_sid[sid]
            parent_peu = 0.0
            for entry in entries:
                remaining = sequence.rlist[entry.index]
                value = entry.utility + remaining if remaining > 0 else 0.0
                if value > parent_peu:
                    parent_peu = value

            seen_i = set()
            seen_s = set()

            for entry in entries:
                index = entry.index
                base_utility = entry.utility

                pos = index + 1
                while pos < len(sequence.seq) and sequence.seq[pos] != 0:
                    item = sequence.seq[pos]
                    if item > last_item and asu + self.one_item_peu.get(item, 0.0) >= self.min_utility:
                        new_utility = base_utility + sequence.ulist[pos]
                        current = raw_i_entries.setdefault(item, {}).setdefault(sid, {}).get(pos)
                        if current is None or new_utility > current:
                            raw_i_entries[item][sid][pos] = new_utility
                        seen_i.add(item)
                    pos += 1

                pos = sequence.next_itemset_start_after(index)
                while pos < len(sequence.seq):
                    item = sequence.seq[pos]
                    if item != 0 and asu + self.one_item_peu.get(item, 0.0) >= self.min_utility:
                        new_utility = base_utility + sequence.ulist[pos]
                        current = raw_s_entries.setdefault(item, {}).setdefault(sid, {}).get(pos)
                        if current is None or new_utility > current:
                            raw_s_entries[item][sid][pos] = new_utility
                        seen_s.add(item)
                    pos += 1

            for item in seen_i:
                rsu_i[item] = rsu_i.get(item, 0.0) + parent_peu
            for item in seen_s:
                rsu_s[item] = rsu_s.get(item, 0.0) + parent_peu

        for item in sorted(raw_i_entries):
            if rsu_i.get(item, 0.0) < self.min_utility:
                continue
            self._increment_candidate_count()
            new_iulist = self._normalize_iulist(raw_i_entries[item])
            new_asu = self._actual_sequence_utility(new_iulist)
            new_pattern = pattern + (item,)
            if new_asu >= self.min_utility:
                self._save_pattern_threadsafe(new_pattern, new_asu)
            if item_count + 1 < self.max_pattern_length and self._peu(new_iulist) >= self.min_utility:
                task_queue.put((new_pattern, new_iulist, new_asu, item_count + 1))

        for item in sorted(raw_s_entries):
            if rsu_s.get(item, 0.0) < self.min_utility:
                continue
            self._increment_candidate_count()
            new_iulist = self._normalize_iulist(raw_s_entries[item])
            new_asu = self._actual_sequence_utility(new_iulist)
            new_pattern = pattern + (-1, item)
            if new_asu >= self.min_utility:
                self._save_pattern_threadsafe(new_pattern, new_asu)
            if item_count + 1 < self.max_pattern_length and self._peu(new_iulist) >= self.min_utility:
                task_queue.put((new_pattern, new_iulist, new_asu, item_count + 1))

    def print_stats(self):
        print("=============  AHUS-P RECONSTRUCTED - STATS =============")
        print(f" Sequences count : {self.sequence_count}")
        print(f" Minimum utility : {self._format_number(self.min_utility)}")
        print(f" Promising items : {len(self.promising_items)}")
        print(f" High-utility sequential patterns : {self.pattern_count}")
        print(f" Candidate extensions after pruning : {self.candidate_count}")
        print(f" Worker threads : {self.max_workers}")
        print(f" Maximum memory usage : {MemoryLogger.get_instance().get_max_memory()} MB")
        print(f" Total time ~ : {self.end_timestamp - self.start_timestamp} ms")
        print("=========================================================")


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    input_file = os.path.join(base_dir, "demo_husp.txt")
    external_utility_file = os.path.join(base_dir, "demo_husp_externalutility.txt")
    output_file = os.path.join(base_dir, "output_ahusp_py.txt")

    min_utility = 1
    min_utility_is_ratio = False
    max_pattern_length = 2**31 - 1

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if not os.path.exists(external_utility_file):
        raise FileNotFoundError(f"External utility file not found: {external_utility_file}")

    algorithm = AlgoAHUSP()
    max_workers = max(1, min(4, os.cpu_count() or 1))
    algorithm.set_max_workers(max_workers)
    algorithm.set_max_pattern_length(max_pattern_length)
    algorithm.load_data_with_internal_external(
        external_utility_file,
        input_file,
        min_utility,
        min_utility_is_ratio=min_utility_is_ratio,
    )
    algorithm.run_algorithm(output_file)
    algorithm.print_stats()


if __name__ == "__main__":
    main()
