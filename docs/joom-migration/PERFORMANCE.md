# Проверка производительности в принятом стиле Comet

Пользовательское решение2026-11-17: действовать так, как принято в проекте;
не создавать специальную методику/инфраструктуру тестов ради миграции.

## Существующие инструменты

- Scala microbenchmarks: `spark/src/test/scala/org/apache/spark/sql/benchmark`,
  `CometBenchmarkBase` на Spark `SqlBasedBenchmark`; Makefile `benchmark-%`.
- Rust microbenchmarks: существующие Criterion benches в
  `native/{core,operators,shuffle,spark-expr}/benches`; Makefile `bench` / Cargo bench.
- Macro benchmarks: существующие TPC-H/TPC-DS runners в `benchmarks/tpc`.
- Общая инструкция: `docs/source/contributor-guide/benchmarking.md`;
  учитывать macOS-ограничения из `benchmarking_macos.md`.

Для каждого изменения выбирать подходящий существующий suite/case, проверять,
что действительно исполняется изменённый путь, и сохранять команды, параметры,
версии, результаты и ограничения. При необходимости расширять соответствующий
существующий benchmark обычным для проекта способом; отдельный новый framework,
нестандартный генератор или специальную систему сравнения не создавать.

| Группа | Подходящие существующие suites/benches |
|---|---|
| G01 | Штатные read suites и доступный baseline; packaging не является benchmark |
| G02 | `CometStringFfiImportBenchmark`, temporal suites и Rust cast/timestamp benches |
| G03 | `CometExecBenchmark`, `CometTopKBenchmark`, Rust `sort_row_partitions` |
| G04 | `CometAggregateExpressionBenchmark`, Rust `aggregate`, `collect`, `aggregate_misc`, `min_max` |
| G05 | `CometSortMergeJoinBenchmark` |
| G06 | `CometWindowBenchmark`, Rust operators `window` |
| G07 | `CometShuffleBenchmark`, `CometColumnarShuffleWriteBenchmark`, `CometColumnarToRowBenchmark`, `CometC2RIsolatedBench`, Rust shuffle benches |
| G08 | `CometCodegenDispatchBenchmark`, `CometScalaUDFDispatchBenchmark`, `CometOperatorSerdeBenchmark` |
| G09 | Доступные TPC-H/DS и operator suites; учитывать фактические планы/выбор engines |

## Границы проверки

`CometBenchmarkBase` устанавливает ANSI=false из-за invalid-input fixtures.
Эти измерения не заменяют обязательные ANSI=true correctness/regression tests.
Разрешённые ранее новые correctness-тесты остаются частью реализации; ограничение
на новую performance-инфраструктуру их не отменяет.

Focused suite/bench предпочтительнее бесконтрольного `setup/all/publish`:
`benchmarks/micro/run.py` может устанавливать пакеты, клонировать/обновлять исходники,
строить release и публиковать изменения. Не запускать эти побочные действия.
Перед запуском смотреть фактический Makefile/runner: некоторые targets сначала
делают release; старые README примеры не гарантируют актуальность флагов/версий.

Не копировать механически пример генерации100GB из macOS guide. Выбирать
ограниченный репрезентативный размер и фиксировать созданные данные в cleanup ledger.
AWS provisioning не входит в GCP-only работу. Полные измерения на реальных GCP jobs
пользователь проведёт позже. Непроверенное ускорение не объявлять подтверждённым.
