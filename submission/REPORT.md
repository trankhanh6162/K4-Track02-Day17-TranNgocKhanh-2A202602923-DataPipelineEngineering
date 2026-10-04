# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Trần Ngọc Khánh / 2A202602923

**Repo:** https://github.com/trankhanh6162/K4-Track02-Day17-TranNgocKhanh-2A202602923-DataPipelineEngineering

**Commit bài nộp:** `270b1d733537ebab0fb1fe49c349633e83e3aed4`

**AI đã dùng và phạm vi hỗ trợ:** OpenCode (GPT-5.6-sol) hỗ trợ đọc repository, chẩn đoán ba lỗi, đề xuất/chỉnh sửa code, chạy kiểm thử và soạn báo cáo; tôi chịu trách nhiệm review, hiểu thay đổi và kiểm tra output thực tế.

**Nguồn tham khảo khác:** README và tài liệu `docs/` của bài lab; không dùng nguồn bên ngoài.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets` có 24 dòng cho 12 ticket; T-91 có cả trạng thái cũ/mới và checksum đổi khi rerun. | u05 ngày 12/08 chỉ có 2 event, 1 click, 0 feedback down thay vì 5/3/1; các check reconciliation/lookback fail. | Delete của T-97 không xuất hiện trong Silver nên ticket còn trong training snapshot mới nhất và RAG index. |
| **Nguyên nhân gốc** | Batch đã deduplicate nội bộ nhưng vẫn `INSERT` vào bảng entity, không upsert theo `ticket_id` và không chặn LSN cũ. | `LOOKBACK_DAYS = 0` nên batch ngày 15/08 không tính lại event-time partition 12/08 dù P99 lateness là 3 ngày. | Debezium `op='d'` có `after=null`; staging chỉ đọc `ticket_id` từ `after`, rồi lọc khóa null nên làm rơi delete record. |
| **Cách sửa** | `pipeline/silver.py`: dùng `MERGE` theo `ticket_id`, update khi `s._lsn > t._lsn`, insert khi chưa có. | `pipeline/config.py`: đặt `LOOKBACK_DAYS = ceil(P99) = 3`; Gold overwrite cửa sổ `[day-3, day]`. | `pipeline/staging.py`: lấy khóa bằng `coalesce(after.ticket_id, before.ticket_id)`; MERGE ghi tombstone và xóa các cột PII. |
| **Khái niệm trên slide** | Một hàng/một entity, keyed upsert, latest-LSN-wins và rerun idempotent. | Event time khác ingest time; measured lookback và overwrite partition để xử lý late data. | Đọc đúng Debezium envelope, phân biệt delete record với Kafka tombstone và bảo đảm “xoá phải lan”. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- Parity: `PARITY` — `silver_tickets` và `gold_feature_daily` khớp giữa Python và dbt

## 3. Lựa chọn công cụ / kỹ thuật

- Dùng MERGE theo khóa cho `silver_tickets` vì đây là bảng entity cần một trạng thái hiện tại; dùng overwrite-partition cho `gold_feature_daily` vì feature theo ngày phải được tính lại trọn cửa sổ khi có late event.
- Giữ tombstone thay vì hard delete để lưu LSN xóa, ngăn replay batch cũ làm dữ liệu sống lại và cho phép truyền trạng thái xóa xuống Gold.
- Dựng snapshot training từ Bronze “as of” ngày snapshot để tránh leakage; snapshot cũ bất biến nhằm tái lập đúng dữ liệu từng dùng huấn luyện.
- DuckDB phù hợp dữ liệu lab nhỏ, chạy local và zero-key; dbt cung cấp model/test/parity khai báo rõ ràng, còn Spark sẽ tăng chi phí vận hành mà không có lợi ích ở quy mô này.

## 4. Hai câu hỏi suy ngẫm

1. Bất biến không được ưu tiên hơn quyền xóa. Tôi sẽ có deletion ledger theo subject/ticket, chặn truy cập ngay, crypto-shred hoặc redact payload nhạy cảm, rồi rebuild snapshot thành version thay thế có manifest/audit liên kết với version cũ; chỉ giữ metadata không chứa PII để chứng minh lineage, không tiếp tục phục vụ snapshot cũ.
2. Tôi đặt chốt PII ở đầu Silver, trước khi free text rời Bronze: kết hợp regex với NER/DLP cho tên, địa chỉ và định danh, quarantine trường hợp độ tin cậy thấp, đồng thời chặn publish nếu scan còn PII. Chất lượng được đo bằng precision/recall/F1 trên tập gán nhãn và leakage rate trên mẫu Silver/Gold định kỳ.

## 5. Output thực tế (Windows PowerShell)

### Verify

```text
$ .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt
```

### Pytest

```text
$ .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.68s
```

### Rerun

```text
$ .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums
```

### Lateness

```text
$ .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3
```

### dbt build

```text
$ $env:DO_NOT_TRACK = '1'
$ Push-Location dbt_project
$ ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
06:52:23  Running with dbt=1.12.5
06:52:23  Registered adapter: duckdb=1.11.0
06:52:24  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
06:52:24
06:52:24  Concurrency: 1 threads (target='dev')
06:52:24
06:52:24  1 of 19 START sql view model main.stg_events ................................... [RUN]
06:52:24  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
06:52:24  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
06:52:24  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.03s]
06:52:24  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
06:52:24  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.11s]
06:52:24  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
06:52:24  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.11s]
06:52:24  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
06:52:24  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.13s]
06:52:24  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
06:52:24  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.04s]
06:52:24  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
06:52:24  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.02s]
06:52:24  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
06:52:24  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
06:52:24  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
06:52:24  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
06:52:24  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
06:52:24  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.02s]
06:52:24  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
06:52:24  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.03s]
06:52:24  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
06:52:24  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.02s]
06:52:24  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
06:52:24  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.02s]
06:52:24  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
06:52:24  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.02s]
06:52:24  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
06:52:24  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
06:52:24  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
06:52:24  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
06:52:24  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
06:52:24  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
06:52:25  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.03s]
06:52:25  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
06:52:25  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.03s]
06:52:25  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
06:52:25  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.03s]
06:52:25  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
06:52:25  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.03s]
06:52:25  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
06:52:25  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.03s]
06:52:25  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
06:52:25  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.03s]
06:52:25  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.23s]
06:52:25  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
06:52:25  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.03s]
06:52:25  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
06:52:25  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
06:52:25  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
06:52:25  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.02s]
06:52:25
06:52:25  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 1.14 seconds (1.14s).
06:52:25
06:52:25  Completed successfully
06:52:25
06:52:25  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19
$ Pop-Location
```

### Parity

```text
$ .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
