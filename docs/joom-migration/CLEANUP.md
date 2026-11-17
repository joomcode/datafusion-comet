# Учёт диска и очистка после миграции

Статус2026-11-17: реализация и новые сборочные загрузки не начаты.
До первой загрузки/сборки сохранены `resource-baseline.json` и
`cleanup-ledger.json`. Этот файл будет дополнен точными командами после работы.
Сейчас ничего удалять не требуется; массовая очистка общих caches не выполняется.
Ненужные промежуточные файлы текущей задачи можно удалять по общим пользовательским
правилам, фиксируя удаление в ledger. Tool-managed caches сохраняют штатное поведение.

## Что фиксировать во время работы

Для каждого ресурса: тип; точный path/image ID/tag/volume/builder/cache ID;
группа и команда создания; существовал ли раньше; принадлежит ли только этой
работе; размер до/после; надо ли сохранить; точечная команда очистки и условия.

Учитывать:

- Maven module `target`, Cargo `native/target` и release `cargo-target/{arm64,amd64}`;
- release outputs: JAR, native `.so`, checksums, commit/build manifests;
- скачанные archives/tools и одноразовые каталоги `/Users/skotlov/d/temp`;
- Maven wrapper/JVM dependency cache, Cargo registry/git cache, Rustup toolchains;
- Docker images/layers, containers, named volumes, buildx builders/build cache;
- benchmark datasets/reports, Spark event logs/warehouse/spill и test outputs.

Не создавать отдельные копии штатных shared caches ради упрощения учёта.
В них различать «использовано» и «достоверно создано этой работой». Сам по себе
mtime не доказывает принадлежность. Если точное разделение невозможно, явно
пометить shared cache и не рекомендовать его полное удаление.

## Что сохранить

Итоговые JAR, SHA256, Git SHA/build manifests и результаты qualification.
Для воспроизведения — утверждённые решения/план и нужные коммиты.
Документация миграции целиком находится в `docs/joom-migration`; пользователь
решит отдельно, оставлять ли этот каталог в итоговых исходниках.

## Что не считать нашими disposable outputs

- Пользовательские checkouts `/Users/skotlov/d/temp/src/*` и Joom repositories.
- Rust/Rustup/Homebrew, которые пользователь установил сам, и существующие JDK.
- Предсуществующие Docker images/volumes/builders и shared caches.

Не предлагать общий `docker system prune -a --volumes`, полную очистку Maven/Cargo/
Homebrew caches или shared builder. Финальная инструкция должна указывать только
проверенные ресурсы и точечные команды, сохраняя финальные артефакты.

Старые материалы аудита в temp перечисляются как исторические кандидаты на
ручную очистку. Они не должны удаляться только на основании имени каталога.
