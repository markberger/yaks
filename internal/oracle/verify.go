package oracle

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
)

func verify(oracleDB *sql.DB, expectedCounts map[PartKey]int64, allConsumed map[PartKey][]ConsumedRecord, logger func(string, ...interface{})) error {
	if err := checkCompleteness(oracleDB, allConsumed); err != nil {
		return err
	}
	logger("[oracle] Check 1: Completeness passed")

	if err := checkNoPhantomWrites(expectedCounts, allConsumed); err != nil {
		return err
	}
	logger("[oracle] Check 2: No phantom writes passed")

	if err := checkNoDuplicateOffsets(allConsumed); err != nil {
		return err
	}
	logger("[oracle] Check 3: No duplicate offsets passed")

	if err := checkOffsetContiguity(allConsumed); err != nil {
		return err
	}
	logger("[oracle] Check 4: Offset contiguity passed")

	if err := checkProducerOrdering(allConsumed); err != nil {
		return err
	}
	logger("[oracle] Check 5: Per-producer ordering passed")

	if err := checkDataIntegrity(oracleDB, allConsumed); err != nil {
		return err
	}
	logger("[oracle] Check 6: Data integrity passed")

	return nil
}

func checkCompleteness(oracleDB *sql.DB, allConsumed map[PartKey][]ConsumedRecord) error {
	consumedValues := make(map[string]bool)
	for _, records := range allConsumed {
		for _, r := range records {
			consumedValues[string(r.Value)] = true
		}
	}

	rows, err := oracleDB.Query("SELECT producer_id, sequence, topic, partition, value FROM produced_records")
	if err != nil {
		return fmt.Errorf("completeness: query oracle: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pid, seq int
		var topic string
		var partition int32
		var value []byte
		if err := rows.Scan(&pid, &seq, &topic, &partition, &value); err != nil {
			return fmt.Errorf("completeness: scan: %w", err)
		}
		if !consumedValues[string(value)] {
			return fmt.Errorf("completeness: record not found in consumed output: producer=%d seq=%d topic=%s partition=%d", pid, seq, topic, partition)
		}
	}
	return rows.Err()
}

func checkNoPhantomWrites(expectedCounts map[PartKey]int64, allConsumed map[PartKey][]ConsumedRecord) error {
	for pk, count := range expectedCounts {
		got := len(allConsumed[pk])
		if int(count) != got {
			return fmt.Errorf("phantom writes: %s/%d expected %d got %d", pk.Topic, pk.Partition, count, got)
		}
	}
	return nil
}

func checkNoDuplicateOffsets(allConsumed map[PartKey][]ConsumedRecord) error {
	for pk, records := range allConsumed {
		offsets := make(map[int64]bool)
		for _, r := range records {
			if offsets[r.Offset] {
				return fmt.Errorf("duplicate offset %d in %s/%d", r.Offset, pk.Topic, pk.Partition)
			}
			offsets[r.Offset] = true
		}
	}
	return nil
}

func checkOffsetContiguity(allConsumed map[PartKey][]ConsumedRecord) error {
	for pk, records := range allConsumed {
		offsets := make([]int64, len(records))
		for i, r := range records {
			offsets[i] = r.Offset
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		for i, o := range offsets {
			if int64(i) != o {
				return fmt.Errorf("offset gap in %s/%d: expected %d got %d", pk.Topic, pk.Partition, i, o)
			}
		}
	}
	return nil
}

func checkProducerOrdering(allConsumed map[PartKey][]ConsumedRecord) error {
	type producerPartKey struct {
		producerID int
		topic      string
		partition  int32
	}
	producerRecords := make(map[producerPartKey][]ConsumedRecord)

	for pk, records := range allConsumed {
		sorted := make([]ConsumedRecord, len(records))
		copy(sorted, records)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Offset < sorted[j].Offset })

		for _, r := range sorted {
			var payload recordPayload
			if err := json.Unmarshal(r.Value, &payload); err != nil {
				return fmt.Errorf("producer ordering: unmarshal %s/%d offset %d: %w", pk.Topic, pk.Partition, r.Offset, err)
			}
			ppk := producerPartKey{payload.PID, pk.Topic, pk.Partition}
			producerRecords[ppk] = append(producerRecords[ppk], r)
		}
	}

	for ppk, records := range producerRecords {
		prevSeq := -1
		for _, r := range records {
			var payload recordPayload
			if err := json.Unmarshal(r.Value, &payload); err != nil {
				return fmt.Errorf("producer ordering: unmarshal: %w", err)
			}
			if payload.Seq <= prevSeq {
				return fmt.Errorf("ordering violation for producer %d in %s/%d: seq %d not > prev %d",
					ppk.producerID, ppk.topic, ppk.partition, payload.Seq, prevSeq)
			}
			prevSeq = payload.Seq
		}
	}
	return nil
}

func checkDataIntegrity(oracleDB *sql.DB, allConsumed map[PartKey][]ConsumedRecord) error {
	oracleByKey := make(map[string][]byte)
	rows, err := oracleDB.Query("SELECT producer_id, sequence, value FROM produced_records")
	if err != nil {
		return fmt.Errorf("data integrity: query oracle: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pid, seq int
		var value []byte
		if err := rows.Scan(&pid, &seq, &value); err != nil {
			return fmt.Errorf("data integrity: scan: %w", err)
		}
		oracleByKey[fmt.Sprintf("%d:%d", pid, seq)] = value
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("data integrity: rows: %w", err)
	}

	for _, records := range allConsumed {
		for _, r := range records {
			var payload recordPayload
			if err := json.Unmarshal(r.Value, &payload); err != nil {
				return fmt.Errorf("data integrity: unmarshal: %w", err)
			}
			key := fmt.Sprintf("%d:%d", payload.PID, payload.Seq)
			oracleValue, ok := oracleByKey[key]
			if !ok {
				return fmt.Errorf("data integrity: consumed record not found in oracle: %s", key)
			}
			if string(oracleValue) != string(r.Value) {
				return fmt.Errorf("data integrity failure for %s: oracle and consumed values differ", key)
			}
		}
	}
	return nil
}
