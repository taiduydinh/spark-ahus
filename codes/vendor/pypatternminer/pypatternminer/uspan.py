from __future__ import annotations
import time
import tracemalloc
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, Iterable, List, Sequence, TypeVar


class MemoryLogger:
    _instance: "MemoryLogger | None" = None

    def __init__(self) -> None:
        self.max_memory = 0.0
        if not tracemalloc.is_tracing():
            tracemalloc.start()

    @classmethod
    def getInstance(cls) -> "MemoryLogger":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def getMaxMemory(self) -> float:
        return self.max_memory

    def reset(self) -> None:
        self.max_memory = 0.0
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        tracemalloc.reset_peak()

    def checkMemory(self) -> float:
        current, peak = tracemalloc.get_traced_memory()
        current_memory = max(current, peak) / 1024.0 / 1024.0
        if current_memory > self.max_memory:
            self.max_memory = current_memory
        return current_memory


@dataclass
class MatrixPosition:
    row: int
    column: int
    utility: int


class QMatrix:
    def __init__(self, nb_item: int, nb_itemset: int, item_names: Sequence[int], item_names_length: int, swu: int) -> None:
        self.matrixItemUtility = [[0] * nb_itemset for _ in range(nb_item)]
        self.matrixItemRemainingUtility = [[0] * nb_itemset for _ in range(nb_item)]
        self.itemNames = list(item_names[:item_names_length])
        self.swu = swu

    def registerItem(self, item_pos: int, itemset: int, utility: int, remaining_utility: int) -> None:
        self.matrixItemUtility[item_pos][itemset] = utility
        self.matrixItemRemainingUtility[item_pos][itemset] = remaining_utility

    def __str__(self) -> str:
        parts = [" MATRIX "]
        for i, item_name in enumerate(self.itemNames):
            row = [f"\n  item: {item_name}  "]
            for j in range(len(self.matrixItemUtility[i])):
                row.append(f"  {self.matrixItemUtility[i][j]}[{self.matrixItemRemainingUtility[i][j]}]")
            parts.append("".join(row))
        parts.append(f"   swu: {self.swu}\n")
        return "".join(parts)


class QMatrixProjection:
    def __init__(self, matrix: QMatrix | "QMatrixProjection", positions: List[MatrixPosition]) -> None:
        self.originalMatrix = matrix.originalMatrix if isinstance(matrix, QMatrixProjection) else matrix
        self.positions = positions

    def getItemNames(self) -> List[int]:
        return self.originalMatrix.itemNames

    def getLocalSequenceUtility(self, position: MatrixPosition) -> int:
        return self.originalMatrix.matrixItemRemainingUtility[position.row][position.column]

    def getItemUtility(self, row_or_position: int | MatrixPosition, column: int | None = None) -> int:
        if isinstance(row_or_position, MatrixPosition):
            return self.originalMatrix.matrixItemUtility[row_or_position.row][row_or_position.column]
        assert column is not None
        return self.originalMatrix.matrixItemUtility[row_or_position][column]

    def getRemainingUtility(self, row: int, column: int) -> int:
        return self.originalMatrix.matrixItemRemainingUtility[row][column]


K = TypeVar("K")
V = TypeVar("V")


@dataclass
class _JavaHashNode(Generic[K, V]):
    key: K
    value: V
    hash_value: int


class JavaHashMap(Generic[K, V]):
    DEFAULT_CAPACITY = 16
    LOAD_FACTOR = 0.75

    def __init__(self, initial_capacity: int = DEFAULT_CAPACITY) -> None:
        capacity = 1
        while capacity < max(1, initial_capacity):
            capacity <<= 1
        self.capacity = max(self.DEFAULT_CAPACITY, capacity)
        self.threshold = int(self.capacity * self.LOAD_FACTOR)
        self.table: List[List[_JavaHashNode[K, V]]] = [[] for _ in range(self.capacity)]
        self.size = 0

    @staticmethod
    def _spread_hash(key: K) -> int:
        hash_code = int(key) & 0xFFFFFFFF
        return (hash_code ^ (hash_code >> 16)) & 0xFFFFFFFF

    def _index(self, hash_value: int) -> int:
        return (self.capacity - 1) & hash_value

    def get(self, key: K) -> V | None:
        hash_value = self._spread_hash(key)
        bucket = self.table[self._index(hash_value)]
        for node in bucket:
            if node.key == key:
                return node.value
        return None

    def put(self, key: K, value: V) -> V | None:
        old_value = self._put_internal(key, value)
        if self.size > self.threshold:
            self._resize()
        return old_value

    def _put_internal(self, key: K, value: V) -> V | None:
        hash_value = self._spread_hash(key)
        bucket = self.table[self._index(hash_value)]
        for node in bucket:
            if node.key == key:
                old_value = node.value
                node.value = value
                return old_value
        bucket.append(_JavaHashNode(key, value, hash_value))
        self.size += 1
        return None

    def _resize(self) -> None:
        old_items = list(self.items())
        self.capacity *= 2
        self.threshold = int(self.capacity * self.LOAD_FACTOR)
        self.table = [[] for _ in range(self.capacity)]
        self.size = 0
        for key, value in old_items:
            self._put_internal(key, value)

    def clear(self) -> None:
        self.table = [[] for _ in range(self.capacity)]
        self.size = 0

    def items(self) -> Iterable[tuple[K, V]]:
        for bucket in self.table:
            for node in bucket:
                yield node.key, node.value


@dataclass
class Pair:
    swu: int = 0
    lastSID: QMatrixProjection | None = None


class AlgoUSpan:
    BUFFERS_SIZE = 2000
    DEBUG = False
    SAVE_RESULT_EASIER_TO_READ_FORMAT = False

    def __init__(self) -> None:
        self.startTimestamp = 0.0
        self.endTimestamp = 0.0
        self.patternCount = 0
        self.writer = None
        self.patternBuffer: List[int] = []
        self.minUtility = 0
        self.maxPatternLength = 1000
        self.input = ""

    def setMaxPatternLength(self, max_pattern_length: int) -> None:
        self.maxPatternLength = max_pattern_length

    def runAlgorithm(self, input_path: str, output_path: str, min_utility: int) -> None:
        MemoryLogger.getInstance().reset()
        self.input = input_path
        self.patternBuffer = [0] * self.BUFFERS_SIZE
        self.patternCount = 0
        self.startTimestamp = time.time()
        self.minUtility = min_utility

        map_item_to_swu: JavaHashMap[int, int] = JavaHashMap()
        considered_items: List[int] = []
        sequence_count = 0

        with open(input_path, "r", encoding="utf-8") as reader:
            for raw_line in reader:
                this_line = raw_line.strip()
                if not this_line or this_line[0] in "#%@":
                    continue
                tokens = this_line.split(" ")
                sequence_utility_string = tokens[-1]
                position_colons = sequence_utility_string.index(":")
                sequence_utility = int(sequence_utility_string[position_colons + 1 :])
                for current_token in tokens[:-3]:
                    if current_token and current_token[0] != "-":
                        position_left_bracket = current_token.index("[")
                        item = int(current_token[:position_left_bracket])
                        if item not in considered_items:
                            considered_items.append(item)
                            swu = map_item_to_swu.get(item)
                            map_item_to_swu.put(item, sequence_utility if swu is None else swu + sequence_utility)
                considered_items.clear()
                sequence_count += 1

        database: List[QMatrix] = []
        item_buffer = [0] * self.BUFFERS_SIZE
        utility_buffer = [0] * self.BUFFERS_SIZE
        items_sequence_buffer = [0] * self.BUFFERS_SIZE

        with open(input_path, "r", encoding="utf-8") as reader:
            for raw_line in reader:
                this_line = raw_line.strip()
                if not this_line or this_line[0] in "#%@":
                    continue

                item_buffer_length = 0
                items_length = 0
                tokens = this_line.split(" ")
                sequence_utility_string = tokens[-1]
                position_colons = sequence_utility_string.index(":")
                sequence_utility = int(sequence_utility_string[position_colons + 1 :])
                nb_itemsets = 1
                current_itemset_has_promising_item = False

                for current_token in tokens[:-3]:
                    if not current_token:
                        continue
                    if current_token == "-1":
                        if current_itemset_has_promising_item:
                            # Grow both aligned buffers together for long sequences.
                            if item_buffer_length >= len(item_buffer):
                                increase = max(1, len(item_buffer))
                                item_buffer.extend([0] * increase)
                                utility_buffer.extend([0] * increase)
                            item_buffer[item_buffer_length] = -1
                            utility_buffer[item_buffer_length] = -1
                            item_buffer_length += 1
                            nb_itemsets += 1
                            current_itemset_has_promising_item = False
                    else:
                        position_left_bracket = current_token.index("[")
                        position_right_bracket = current_token.index("]")
                        item = int(current_token[:position_left_bracket])
                        item_utility = int(current_token[position_left_bracket + 1 : position_right_bracket])
                        item_swu = map_item_to_swu.get(item)
                        if item_swu is not None and item_swu >= min_utility:
                            current_itemset_has_promising_item = True
                            if item_buffer_length >= len(item_buffer):
                                increase = max(1, len(item_buffer))
                                item_buffer.extend([0] * increase)
                                utility_buffer.extend([0] * increase)
                            item_buffer[item_buffer_length] = item
                            utility_buffer[item_buffer_length] = item_utility
                            item_buffer_length += 1
                            if items_length >= len(items_sequence_buffer):
                                items_sequence_buffer.extend([0] * max(1, len(items_sequence_buffer)))
                            items_sequence_buffer[items_length] = item
                            items_length += 1
                        else:
                            sequence_utility -= item_utility

                if sequence_utility == 0:
                    continue

                sorted_items = sorted(items_sequence_buffer[:items_length])
                new_items: List[int] = []
                last_item_seen = -999
                for item in sorted_items:
                    if item != last_item_seen:
                        new_items.append(item)
                        last_item_seen = item

                matrix = QMatrix(len(new_items), nb_itemsets, new_items, len(new_items), sequence_utility)
                database.append(matrix)

                pos_buffer = 0
                current_sequence_utility = sequence_utility
                for itemset in range(nb_itemsets):
                    pos_names = 0
                    while pos_buffer < item_buffer_length:
                        item = item_buffer[pos_buffer]
                        if item == -1:
                            pos_buffer += 1
                            break
                        if item == matrix.itemNames[pos_names]:
                            utility = utility_buffer[pos_buffer]
                            current_sequence_utility -= utility
                            matrix.registerItem(pos_names, itemset, utility, current_sequence_utility)
                            pos_names += 1
                            pos_buffer += 1
                        elif item > matrix.itemNames[pos_names]:
                            matrix.registerItem(pos_names, itemset, 0, current_sequence_utility)
                            pos_names += 1
                        else:
                            matrix.registerItem(pos_names, itemset, 0, current_sequence_utility)
                            pos_buffer += 1

        MemoryLogger.getInstance().checkMemory()

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8", newline="\n") as writer:
            self.writer = writer
            self.uspanFirstTime(self.patternBuffer, 0, database)

        MemoryLogger.getInstance().checkMemory()
        self.endTimestamp = time.time()

    def uspanFirstTime(self, prefix: List[int], prefix_length: int, database: List[QMatrix]) -> None:
        del prefix_length
        map_item_swu: JavaHashMap[int, int] = JavaHashMap()
        for qmatrix in database:
            for item in qmatrix.itemNames:
                current_swu = map_item_swu.get(item)
                map_item_swu.put(item, qmatrix.swu if current_swu is None else current_swu + qmatrix.swu)

        for item, item_swu in map_item_swu.items():
            if item_swu >= self.minUtility:
                total_utility = 0
                total_remaining_utility = 0
                matrix_projections: List[QMatrixProjection] = []
                for qmatrix in database:
                    try:
                        row = qmatrix.itemNames.index(item)
                    except ValueError:
                        row = -1
                    if row >= 0:
                        positions: List[MatrixPosition] = []
                        max_utility = 0
                        max_remaining_utility = 0
                        for itemset in range(len(qmatrix.matrixItemRemainingUtility[row])):
                            utility = qmatrix.matrixItemUtility[row][itemset]
                            if utility > 0:
                                positions.append(MatrixPosition(row, itemset, utility))
                                if utility > max_utility:
                                    max_utility = utility
                                    remaining = qmatrix.matrixItemRemainingUtility[row][itemset]
                                    if remaining > 0 and max_remaining_utility == 0:
                                        max_remaining_utility = remaining
                        total_utility += max_utility
                        total_remaining_utility += max_remaining_utility
                        matrix_projections.append(QMatrixProjection(qmatrix, positions))

                prefix[0] = item
                if total_utility >= self.minUtility:
                    self.writeOut(prefix, 1, total_utility)

                if total_utility + total_remaining_utility >= self.minUtility and 1 < self.maxPatternLength:
                    self.uspan(prefix, 1, matrix_projections, 1)

        MemoryLogger.getInstance().checkMemory()

    def uspan(self, prefix: List[int], prefix_length: int, projected_database: List[QMatrixProjection], item_count: int) -> None:
        map_item_swu: JavaHashMap[int, Pair] = JavaHashMap()
        for qmatrix in projected_database:
            for position in qmatrix.positions:
                row = position.row + 1
                column = position.column
                local_sequence_utility = qmatrix.getLocalSequenceUtility(position)
                while row < len(qmatrix.getItemNames()):
                    item = qmatrix.getItemNames()[row]
                    if qmatrix.getItemUtility(row, column) > 0:
                        current_swu = map_item_swu.get(item)
                        temp_swu = position.utility + local_sequence_utility
                        if current_swu is None:
                            map_item_swu.put(item, Pair(temp_swu, qmatrix))
                        elif current_swu.lastSID is not qmatrix:
                            current_swu.lastSID = qmatrix
                            current_swu.swu += temp_swu
                        elif temp_swu > current_swu.swu:
                            current_swu.swu = temp_swu
                    row += 1

        for item, item_swu in map_item_swu.items():
            if item_swu.swu >= self.minUtility:
                total_utility = 0
                total_remaining_utility = 0
                matrix_projections: List[QMatrixProjection] = []
                for qmatrix in projected_database:
                    item_names = qmatrix.getItemNames()
                    try:
                        row_item = item_names.index(item)
                    except ValueError:
                        row_item = -1
                    if row_item >= 0:
                        max_utility = 0
                        max_remaining_utility = 0
                        positions: List[MatrixPosition] = []
                        for position in qmatrix.positions:
                            column = position.column
                            new_item_utility = qmatrix.getItemUtility(row_item, column)
                            if new_item_utility > 0:
                                new_prefix_utility = position.utility + new_item_utility
                                positions.append(MatrixPosition(row_item, column, new_prefix_utility))
                                if new_prefix_utility > max_utility:
                                    max_utility = new_prefix_utility
                                    remaining = qmatrix.getRemainingUtility(row_item, column)
                                    if remaining > 0 and max_remaining_utility == 0:
                                        max_remaining_utility = remaining
                        total_utility += max_utility
                        total_remaining_utility += max_remaining_utility
                        matrix_projections.append(QMatrixProjection(qmatrix, positions))

                prefix[prefix_length] = item
                if total_utility >= self.minUtility:
                    self.writeOut(prefix, prefix_length + 1, total_utility)

                if total_utility + total_remaining_utility >= self.minUtility and item_count + 1 < self.maxPatternLength:
                    self.uspan(prefix, prefix_length + 1, matrix_projections, item_count + 1)

        map_item_swu.clear()
        for qmatrix in projected_database:
            for position in qmatrix.positions:
                local_sequence_utility = qmatrix.getLocalSequenceUtility(position)
                for row in range(len(qmatrix.getItemNames())):
                    item = qmatrix.getItemNames()[row]
                    for column in range(position.column + 1, len(qmatrix.originalMatrix.matrixItemUtility[row])):
                        if qmatrix.getItemUtility(row, column) > 0:
                            current_swu = map_item_swu.get(item)
                            temp_swu = position.utility + local_sequence_utility
                            if current_swu is None:
                                map_item_swu.put(item, Pair(temp_swu, qmatrix))
                            elif current_swu.lastSID is not qmatrix:
                                current_swu.lastSID = qmatrix
                                current_swu.swu += temp_swu
                            elif temp_swu > current_swu.swu:
                                current_swu.swu = temp_swu
                            break

        for item, item_swu in map_item_swu.items():
            if item_swu.swu >= self.minUtility:
                total_utility = 0
                total_remaining_utility = 0
                matrix_projections: List[QMatrixProjection] = []
                for qmatrix in projected_database:
                    item_names = qmatrix.getItemNames()
                    try:
                        row_item = item_names.index(item)
                    except ValueError:
                        row_item = -1
                    if row_item >= 0:
                        max_utility = 0
                        max_remaining_utility = 0
                        positions: List[MatrixPosition] = []
                        for position in qmatrix.positions:
                            for column in range(position.column + 1, len(qmatrix.originalMatrix.matrixItemUtility[row_item])):
                                new_item_utility = qmatrix.getItemUtility(row_item, column)
                                if new_item_utility > 0:
                                    new_prefix_utility = position.utility + new_item_utility
                                    positions.append(MatrixPosition(row_item, column, new_prefix_utility))
                                    if new_prefix_utility > max_utility:
                                        max_utility = new_prefix_utility
                                        remaining = qmatrix.getRemainingUtility(row_item, column)
                                        if remaining > 0 and max_remaining_utility == 0:
                                            max_remaining_utility = remaining
                        total_utility += max_utility
                        total_remaining_utility += max_remaining_utility
                        matrix_projections.append(QMatrixProjection(qmatrix, positions))

                prefix[prefix_length] = -1
                prefix[prefix_length + 1] = item
                if total_utility >= self.minUtility:
                    self.writeOut(prefix, prefix_length + 2, total_utility)

                if total_utility + total_remaining_utility >= self.minUtility and item_count + 1 < self.maxPatternLength:
                    self.uspan(prefix, prefix_length + 2, matrix_projections, item_count + 1)

        MemoryLogger.getInstance().checkMemory()

    def writeOut(self, prefix: List[int], prefix_length: int, utility: int) -> None:
        self.patternCount += 1
        buffer: List[str] = []
        if not self.SAVE_RESULT_EASIER_TO_READ_FORMAT:
            for i in range(prefix_length):
                buffer.append(str(prefix[i]))
                buffer.append(" ")
            buffer.append("-1 #UTIL: ")
            buffer.append(str(utility))
        else:
            buffer.append("<(")
            for i in range(prefix_length):
                if prefix[i] == -1:
                    buffer.append(")(")
                else:
                    buffer.append(str(prefix[i]))
            buffer.append(")>:")
            buffer.append(str(utility))
        assert self.writer is not None
        self.writer.write("".join(buffer))
        self.writer.write("\r\n")

        if self.DEBUG:
            self.checkIfUtilityOfPatternIsCorrect(prefix, prefix_length, utility)

    def checkIfUtilityOfPatternIsCorrect(self, prefix: List[int], prefix_length: int, utility: int) -> None:
        calculated_utility = 0
        with open(self.input, "r", encoding="utf-8") as reader:
            for raw_line in reader:
                this_line = raw_line.strip()
                if not this_line or this_line[0] in "#%@":
                    continue
                tokens = this_line.split(" ")
                tokens_length = len(tokens) - 3
                sequence = [0] * tokens_length
                sequence_utility = [0] * tokens_length
                for i, current_token in enumerate(tokens[:tokens_length]):
                    if not current_token:
                        continue
                    if current_token == "-1":
                        item = -1
                        item_utility = 0
                    else:
                        position_left_bracket = current_token.index("[")
                        position_right_bracket = current_token.index("]")
                        item = int(current_token[:position_left_bracket])
                        item_utility = int(current_token[position_left_bracket + 1 : position_right_bracket])
                    sequence[i] = item
                    sequence_utility[i] = item_utility
                util = self.tryToMatch(sequence, sequence_utility, prefix, prefix_length, 0, 0, 0)
                calculated_utility += util
        if calculated_utility != utility:
            raise ValueError(f"Wrong utility for pattern {prefix[:prefix_length]}: {utility} != {calculated_utility}")

    def tryToMatch(
        self,
        sequence: List[int],
        sequence_utility: List[int],
        prefix: List[int],
        prefix_length: int,
        prefix_pos: int,
        seq_pos: int,
        utility: int,
    ) -> int:
        other_utility_values: List[int] = []
        pos_p = prefix_pos
        pos_s = seq_pos
        previous_prefix_pos = prefix_pos
        itemset_utility = 0
        while pos_p < prefix_length and pos_s < len(sequence):
            if prefix[pos_p] == -1 and sequence[pos_s] == -1:
                pos_s += 1
                other_utility = self.tryToMatch(sequence, sequence_utility, prefix, prefix_length, previous_prefix_pos, pos_s, utility)
                other_utility_values.append(other_utility)
                pos_p += 1
                utility += itemset_utility
                itemset_utility = 0
                previous_prefix_pos = pos_p
            elif prefix[pos_p] == -1:
                while pos_s < len(sequence) and sequence[pos_s] != -1:
                    pos_s += 1
                other_utility = self.tryToMatch(sequence, sequence_utility, prefix, prefix_length, previous_prefix_pos, pos_s, utility)
                other_utility_values.append(other_utility)
                utility += itemset_utility
                itemset_utility = 0
                previous_prefix_pos = pos_p
            elif sequence[pos_s] == -1:
                pos_p = previous_prefix_pos
                itemset_utility = 0
                pos_s += 1
            elif prefix[pos_p] == sequence[pos_s]:
                pos_p += 1
                itemset_utility += sequence_utility[pos_s]
                pos_s += 1
                if pos_p == prefix_length:
                    while pos_s < len(sequence) and sequence[pos_s] != -1:
                        pos_s += 1
                    other_utility = self.tryToMatch(sequence, sequence_utility, prefix, prefix_length, previous_prefix_pos, pos_s, utility)
                    other_utility_values.append(other_utility)
                    utility += itemset_utility
            else:
                pos_s += 1

        max_utility = utility if pos_p == prefix_length else 0
        for util_value in other_utility_values:
            if util_value > utility:
                max_utility = util_value
        return max_utility

    def printStatistics(self) -> None:
        total_time_ms = int((self.endTimestamp - self.startTimestamp) * 1000)
        print("=============  USPAN ALGORITHM v2.14 - STATS ==========")
        print(f" Total time ~ {total_time_ms} ms")
        print(f" Max Memory ~ {MemoryLogger.getInstance().getMaxMemory()} MB")
        print(f" High-utility sequential pattern count : {self.patternCount}")
        print("========================================================")


def fileToPath(filename: str) -> Path:
    return Path(__file__).resolve().parent / filename


def main() -> None:
    test_dir = Path(__file__).resolve().parent
    project_root = test_dir.parents[3]
    del test_dir

    # Change these values directly here. This Python file is independent of Java now.
    minutil = 30
    max_pattern_length = 4

    input_path = fileToPath("DataBase_HUSRM.txt")
    output_python = project_root / "output_python.txt"

    algo = AlgoUSpan()
    algo.setMaxPatternLength(max_pattern_length)
    algo.runAlgorithm(str(input_path), str(output_python), minutil)
    algo.printStatistics()


if __name__ == "__main__":
    main()
