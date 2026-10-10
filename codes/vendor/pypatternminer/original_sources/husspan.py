import os
import time
import tracemalloc
from bisect import bisect_left
from typing import Dict, List, Optional, Tuple


class MemoryLogger:
    _instance = None

    def __init__(self):
        self.max_memory = 0.0

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
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


class QMatrixHUSSpan:
    def __init__(self, nb_item: int, nb_itemset: int, item_names: List[int], item_names_length: int, swu: float):
        self.seq_id = 0
        self.matrix_item_utility = [[0.0 for _ in range(nb_itemset)] for _ in range(nb_item)]
        self.matrix_item_remaining_utility = [[0.0 for _ in range(nb_itemset)] for _ in range(nb_item)]
        self.item_names = list(item_names[:item_names_length])
        self.nb_itemsets = nb_itemset
        self.swu = float(swu)

    def register_item(self, item_pos: int, itemset: int, utility: float, remaining_utility: float):
        self.matrix_item_utility[item_pos][itemset] = float(utility)
        self.matrix_item_remaining_utility[item_pos][itemset] = float(remaining_utility)

    def get_item_names(self):
        return self.item_names

    def get_item_utility(self, row: int, column: int):
        return self.matrix_item_utility[row][column]

    def get_remaining_utility(self, row: int, column: int):
        return self.matrix_item_remaining_utility[row][column]


class UtilityList:
    def __init__(self, tid: int = 0, acu: float = 0.0, ru: float = 0.0, link: Optional["UtilityList"] = None):
        self.tid = tid
        self.acu = float(acu)
        self.ru = float(ru)
        self.link = link


class UtilityChain:
    def __init__(self, seq_id: int = 0, peuts: float = 0.0, max_utility: float = 0.0, utility_list: Optional[UtilityList] = None):
        self.seq_id = seq_id
        self.peuts = float(peuts)
        self.max_utility = float(max_utility)
        self.utility_list = utility_list


class HUSSpan:
    def __init__(self):
        self.number_of_patterns = 0.0
        self.debug = False
        self.sequence_count = 0
        self.start_timestamp = 0
        self.end_timestamp = 0
        self.pattern_count = 0
        self.buffers_size = 2000
        self.pattern_buffer = [0] * self.buffers_size
        self.min_utility = 0.0
        self.max_pattern_length = 2**31 - 1
        self.input_path = ""
        self.database: List[QMatrixHUSSpan] = []
        self.map_item_to_swu: Dict[int, float] = {}
        self.high_utility_set: List[Tuple[Tuple[int, ...], float]] = []
        self.external_data: Dict[int, float] = {}

    def set_max_pattern_length(self, max_pattern_length: int):
        self.max_pattern_length = max_pattern_length

    @staticmethod
    def _current_time_millis():
        return int(time.time() * 1000)

    @staticmethod
    def _parse_external_utilities(path: str) -> Dict[int, float]:
        external_data: Dict[int, float] = {}
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                key, value = line.split(":", 1)
                item = int(key)
                utility = float(value)
                if utility < 0:
                    raise ValueError(
                        f"HUSSpan expects positive external utilities. Negative utility found for item {item}: {utility}."
                    )
                external_data[item] = utility
        return external_data

    @staticmethod
    def _parse_item_token(token: str) -> Tuple[int, int]:
        left = token.index("[")
        right = token.index("]")
        return int(token[:left]), int(token[left + 1:right])

    @staticmethod
    def _parse_sequence_utility(tokens: List[str]) -> float:
        sequence_utility_string = tokens[-1]
        return float(sequence_utility_string[sequence_utility_string.index(":") + 1:])

    @staticmethod
    def _binary_search(items: List[int], item: int) -> int:
        pos = bisect_left(items, item)
        if pos != len(items) and items[pos] == item:
            return pos
        return -1

    def load_data_with_internal_external(self, external: str, input_path: str, min_utility: float) -> List[QMatrixHUSSpan]:
        self.external_data = self._parse_external_utilities(external)
        self.input_path = input_path
        self.pattern_buffer = [0] * self.buffers_size
        self.min_utility = float(min_utility)
        self.map_item_to_swu = {}
        self.sequence_count = 0

        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                this_line = line.strip()
                if not this_line or this_line[0] in "#%@":
                    continue
                tokens = this_line.split()
                sequence_utility = self._parse_sequence_utility(tokens)
                seen_items = set()
                for token in tokens[:-3]:
                    if token and token[0] != "-":
                        item, _ = self._parse_item_token(token)
                        if item not in seen_items:
                            self.map_item_to_swu[item] = self.map_item_to_swu.get(item, 0.0) + sequence_utility
                            seen_items.add(item)
                self.sequence_count += 1

        database: List[QMatrixHUSSpan] = []
        seq_id = 0

        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                this_line = line.strip()
                if not this_line or this_line[0] in "#%@":
                    continue

                item_buffer: List[int] = []
                utility_buffer: List[float] = []
                items_sequence_buffer: List[int] = []
                tokens = this_line.split()
                sequence_utility = self._parse_sequence_utility(tokens)
                nb_itemsets = 1
                current_itemset_has_promising_item = False

                for token in tokens[:-3]:
                    if token == "-1":
                        if current_itemset_has_promising_item:
                            item_buffer.append(-1)
                            utility_buffer.append(-1.0)
                            nb_itemsets += 1
                            current_itemset_has_promising_item = False
                    elif token.startswith("-"):
                        continue
                    else:
                        item, internal_utility = self._parse_item_token(token)
                        item_utility = internal_utility * self.external_data[item]
                        if self.map_item_to_swu.get(item, 0.0) >= self.min_utility:
                            current_itemset_has_promising_item = True
                            item_buffer.append(item)
                            utility_buffer.append(float(item_utility))
                            items_sequence_buffer.append(item)
                        else:
                            sequence_utility -= item_utility

                if sequence_utility == 0:
                    self.sequence_count -= 1
                    continue

                unique_items = []
                for item in sorted(items_sequence_buffer):
                    if not unique_items or unique_items[-1] != item:
                        unique_items.append(item)

                nb_items = len(unique_items)
                nb_itemsets -= 1
                matrix = QMatrixHUSSpan(nb_items, nb_itemsets, unique_items, len(unique_items), sequence_utility)
                seq_id += 1
                matrix.seq_id = seq_id
                matrix.nb_itemsets = nb_itemsets
                database.append(matrix)

                pos_buffer = 0
                for itemset in range(nb_itemsets):
                    pos_names = 0
                    while pos_buffer < len(item_buffer):
                        item = item_buffer[pos_buffer]
                        if item == -1:
                            while pos_names < len(matrix.item_names):
                                matrix.register_item(pos_names, itemset, 0.0, sequence_utility)
                                pos_names += 1
                            pos_buffer += 1
                            break
                        elif pos_names < len(matrix.item_names) and item == matrix.item_names[pos_names]:
                            utility = utility_buffer[pos_buffer]
                            sequence_utility -= utility
                            matrix.register_item(pos_names, itemset, utility, sequence_utility)
                            pos_names += 1
                            pos_buffer += 1
                        elif pos_names < len(matrix.item_names) and item > matrix.item_names[pos_names]:
                            matrix.register_item(pos_names, itemset, 0.0, sequence_utility)
                            pos_names += 1
                        else:
                            pos_buffer += 1
                    while pos_names < len(matrix.item_names):
                        matrix.register_item(pos_names, itemset, 0.0, sequence_utility)
                        pos_names += 1

        self.database = database
        return database

    def run_algorithm(self, database: Optional[List[QMatrixHUSSpan]], output: Optional[str]):
        self.high_utility_set = []
        self.pattern_count = 0
        self.number_of_patterns = 0.0
        MemoryLogger.get_instance().reset()
        self.start_timestamp = self._current_time_millis()
        if database is not None:
            self.database = database
        self._husspan_first_time(self.pattern_buffer, 0, self.database)
        self.end_timestamp = self._current_time_millis()
        MemoryLogger.get_instance().check_memory()
        if output is not None:
            self.write_output(output)
        return self.high_utility_set

    def _husspan_first_time(self, prefix: List[int], prefix_length: int, database: List[QMatrixHUSSpan]):
        map_item_swu: Dict[int, float] = {}
        for qmatrix in database:
            for item in qmatrix.item_names:
                map_item_swu[item] = map_item_swu.get(item, 0.0) + qmatrix.swu

        for item, swu in list(map_item_swu.items()):
            if swu >= self.min_utility:
                self.number_of_patterns += 1
                peut = 0.0
                total_utility = 0.0
                list_of_utility_chains: List[UtilityChain] = []

                for qmatrix in database:
                    u_list: Optional[UtilityList] = None
                    max_utility = 0.0
                    row = self._binary_search(qmatrix.item_names, item)
                    if row >= 0:
                        max_peuts = 0.0
                        for itemset in range(len(qmatrix.matrix_item_remaining_utility[row]) - 1, -1, -1):
                            utility = qmatrix.matrix_item_utility[row][itemset]
                            if utility > 0:
                                if utility > max_utility:
                                    max_utility = utility
                                remaining = qmatrix.matrix_item_remaining_utility[row][itemset]
                                peuts = utility + remaining
                                if peuts > max_peuts:
                                    max_peuts = peuts
                                u_list = UtilityList(itemset, utility, remaining, u_list)
                        if u_list is not None:
                            list_of_utility_chains.append(UtilityChain(qmatrix.seq_id, max_peuts, max_utility, u_list))
                            peut += max_peuts
                            total_utility += max_utility

                prefix[0] = item
                if total_utility >= self.min_utility:
                    self._save_pattern(prefix[:1], total_utility)
                if peut >= self.min_utility and 1 < self.max_pattern_length:
                    self._husspan(prefix, 1, list_of_utility_chains, 1)

    def _husspan(self, prefix: List[int], prefix_length: int, utility_chains: List[UtilityChain], item_count: int):
        i_list: Dict[int, float] = {}
        s_list: Dict[int, float] = {}
        last_item = prefix[prefix_length - 1]

        for utility_chain in utility_chains:
            utility_list = utility_chain.utility_list
            qmatrix = self.database[utility_chain.seq_id - 1]
            iitems: List[int] = []
            sitems: List[int] = []

            while utility_list is not None:
                icolumn = utility_list.tid
                irow = self._binary_search(qmatrix.item_names, last_item) + 1
                while irow < len(qmatrix.item_names):
                    item = qmatrix.item_names[irow]
                    if qmatrix.get_item_utility(irow, icolumn) != 0:
                        if item not in iitems:
                            iitems.append(item)
                            i_list[item] = i_list.get(item, 0.0) + utility_chain.peuts
                    irow += 1

                for srow, item in enumerate(qmatrix.item_names):
                    for scolumn in range(utility_list.tid + 1, len(qmatrix.matrix_item_utility[srow])):
                        if qmatrix.get_item_utility(srow, scolumn) != 0:
                            if item not in sitems:
                                sitems.append(item)
                                s_list[item] = s_list.get(item, 0.0) + utility_chain.peuts
                            break
                utility_list = utility_list.link

        for item, rsu in list(i_list.items()):
            if rsu >= self.min_utility:
                self.number_of_patterns += 1
                peut = 0.0
                total_utility = 0.0
                list_i_chains: List[UtilityChain] = []

                for utility_chain in utility_chains:
                    utility_list = utility_chain.utility_list
                    qmatrix = self.database[utility_chain.seq_id - 1]
                    max_utility = 0.0
                    max_peuts = 0.0
                    tmp_list: List[UtilityList] = []
                    row = self._binary_search(qmatrix.item_names, item)
                    if row >= 0:
                        while utility_list is not None:
                            column = utility_list.tid
                            utility = qmatrix.matrix_item_utility[row][column]
                            if utility > 0:
                                acu = utility_list.acu + utility
                                if acu > max_utility:
                                    max_utility = acu
                                remaining = qmatrix.matrix_item_remaining_utility[row][column]
                                peuts = acu + remaining
                                if peuts > max_peuts:
                                    max_peuts = peuts
                                tmp_list.append(UtilityList(column, acu, remaining))
                            utility_list = utility_list.link
                    if tmp_list:
                        for i in range(len(tmp_list) - 1):
                            tmp_list[i].link = tmp_list[i + 1]
                        list_i_chains.append(UtilityChain(qmatrix.seq_id, max_peuts, max_utility, tmp_list[0]))
                        peut += max_peuts
                        total_utility += max_utility

                prefix[prefix_length] = item
                if total_utility >= self.min_utility:
                    self._save_pattern(prefix[:prefix_length + 1], total_utility)
                if peut >= self.min_utility and item_count + 1 < self.max_pattern_length:
                    self._husspan(prefix, prefix_length + 1, list_i_chains, item_count + 1)

        for item, rsu in list(s_list.items()):
            if rsu >= self.min_utility:
                self.number_of_patterns += 1
                peut = 0.0
                total_utility = 0.0
                list_s_chains: List[UtilityChain] = []

                for utility_chain in utility_chains:
                    utility_list = utility_chain.utility_list
                    qmatrix = self.database[utility_chain.seq_id - 1]
                    max_utility = 0.0
                    max_peuts = 0.0
                    tmp_list: List[UtilityList] = []
                    row = self._binary_search(qmatrix.item_names, item)
                    if row >= 0:
                        while utility_list is not None:
                            for column in range(utility_list.tid + 1, len(qmatrix.matrix_item_utility[row])):
                                utility = qmatrix.matrix_item_utility[row][column]
                                if utility > 0:
                                    acu = utility_list.acu + utility
                                    if acu > max_utility:
                                        max_utility = acu
                                    remaining = qmatrix.matrix_item_remaining_utility[row][column]
                                    peuts = acu + remaining
                                    if peuts > max_peuts:
                                        max_peuts = peuts
                                    tmp_list.append(UtilityList(column, acu, remaining))
                            utility_list = utility_list.link
                    if tmp_list:
                        for i in range(len(tmp_list) - 1):
                            tmp_list[i].link = tmp_list[i + 1]
                        list_s_chains.append(UtilityChain(qmatrix.seq_id, max_peuts, max_utility, tmp_list[0]))
                        peut += max_peuts
                        total_utility += max_utility

                prefix[prefix_length] = -1
                prefix[prefix_length + 1] = item
                if total_utility >= self.min_utility:
                    self._save_pattern(prefix[:prefix_length + 2], total_utility)
                if peut >= self.min_utility and item_count + 1 < self.max_pattern_length:
                    self._husspan(prefix, prefix_length + 2, list_s_chains, item_count + 1)

    def _save_pattern(self, pattern: List[int], utility: float):
        self.high_utility_set.append((tuple(pattern), float(utility)))
        self.pattern_count += 1

    @staticmethod
    def _format_number(value: float) -> str:
        if abs(value - int(value)) < 1e-9:
            return str(int(value))
        text = f"{value:.10f}".rstrip("0").rstrip(".")
        return text if text else "0"

    @staticmethod
    def pattern_to_string(pattern: Tuple[int, ...], utility: float) -> str:
        parts = ["<("]
        for item in pattern:
            if item == -1:
                parts.append(")(")
            else:
                parts.append(str(item))
        parts.append(")>:")
        parts.append(HUSSpan._format_number(utility))
        return "".join(parts)

    def write_output(self, output_path: str):
        with open(output_path, "w", encoding="utf-8", newline="") as f:
            for pattern, utility in self.high_utility_set:
                f.write(self.pattern_to_string(pattern, utility))
                f.write(os.linesep)

    def print_stats(self):
        print("=============  HUSSpan - STATS =============")
        print(f" Sequences count : {self.sequence_count}")
        print(f" Candidate/pattern expansions : {int(self.number_of_patterns)}")
        print(f" High-utility sequential patterns : {self.pattern_count}")
        print(f" Maximum memory usage : {MemoryLogger.get_instance().get_max_memory()} MB")
        print(f" Total time ~ : {self.end_timestamp - self.start_timestamp} ms")
        print("===================================================")


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    input_file = os.path.join(base_dir, "demo_husp.txt")
    external_utility_file = os.path.join(base_dir, "demo_husp_externalutility.txt")
    output_file = os.path.join(base_dir, "output_husspan.txt")

    min_utility = 80
    max_pattern_length = 2**31 - 1

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    if not os.path.exists(external_utility_file):
        raise FileNotFoundError(f"External utility file not found: {external_utility_file}")

    algorithm = HUSSpan()
    algorithm.set_max_pattern_length(max_pattern_length)
    database = algorithm.load_data_with_internal_external(external_utility_file, input_file, min_utility)
    algorithm.run_algorithm(database, output_file)
    algorithm.print_stats()


if __name__ == "__main__":
    main()
