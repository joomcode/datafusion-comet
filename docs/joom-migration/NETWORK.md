# Согласованный сетевой scope

Статус: APPROVED_WITH_USER_SAFETY_CONDITION, 2026-11-17.
Прямое согласие и его условие записаны в `decisions.json`.

Пользователь выдал одно разрешение на весь утверждённый перенос G01–G09 и необходимую
квалификацию G10 до получения локальных JAR, включая повторные сборки и тесты.
Оно охватывает перечисленные загрузки и необходимые обращения дочерних процессов
на host и внутри Docker; не только одну буквальную команду.

| Цель | Семейства команд | Ожидаемые hosts |
|---|---|---|
| Maven wrapper/JVM dependencies/plugins, Spark test build | `./mvnw ...`, Maven compile/test/verify/package/install и Spark `build/sbt ...` | `repo.maven.apache.org`, `repo1.maven.org`; при необходимости `repo.scala-sbt.org`, `repo.typesafe.com` и их штатные redirects |
| Rust compiler/components и crates | `rustup toolchain/component ...`, `cargo fetch/build/test/bench/fmt/clippy ...` | `sh.rustup.rs`, `static.rust-lang.org`, `index.crates.io`, `static.crates.io`, `crates.io` |
| Закреплённые Cargo git dependencies и бинарные build tools | Cargo fetch/build, скачивание Protobuf release binary; read-only GitHub API | `github.com` (в частности `apache/iceberg-rust` на закреплённом commit), `api.github.com`, `raw.githubusercontent.com`, `codeload.github.com`, `release-assets.githubusercontent.com`, `objects.githubusercontent.com` и их штатные CDN redirects |
| Linux builder images и build/run с перечисленными загрузками внутри контейнера | `docker pull/build/buildx build/run ...` | Docker Hub: `registry-1.docker.io`, `auth.docker.io`, `production.cloudflare.docker.com` и штатные blob/CDN endpoints |
| Ubuntu build packages внутри builder | `apt-get update/install ...` | `archive.ubuntu.com`, `security.ubuntu.com`, `ports.ubuntu.com` и штатные Ubuntu mirrors/redirects |

Согласование включает обращения для разрешения зависимостей, загрузку archives,
metadata/checksums/signatures, а также публичных build tools и fixtures, являющихся
зависимостями выбранной сборки/тестов. Фиксированные зависимости и Cargo.lock сохраняются;
произвольное обновление зависимостей в объём разрешения не входит.
Результаты, логи и скачанные исполнителем вспомогательные файлы — в разрешённых
каталогах; обычные Maven/Cargo/Rustup/Docker caches могут использовать штатные пути.

Примеры ожидаемых команд после подготовки нужных файлов (выбирать по текущей стадии; это согласование сети, а не команда запустить все примеры):

```sh
./mvnw -B -Pspark-4.2,delta -Dspark.version=4.2.1 -Ddelta.spark.version=4.4.1 verify
cargo fetch --locked --manifest-path native/Cargo.toml
cargo test --locked --manifest-path native/Cargo.toml -p datafusion-comet-spark-expr
rustup toolchain install 1.99.0 --profile minimal
docker pull --platform linux/amd64 ubuntu:20.04
docker pull --platform linux/arm64 ubuntu:20.04
```

Точные flags/выбор тестовых пакетов уточняются по исходникам и текущей стадии;
пример не заменяет обязательный native-before-JVM build order. Отдельные vendor-unit
tests должны использовать тот же workspace/lock/patch graph, что release.
Действующий wrapper закрепляет Maven3.9.6. Старый Joom Linux builder закрепляет
Rust1.99.0, Ubuntu20.04/GCC10/Java17/Protobuf30.2; его адаптация — часть G01.

Не входит: Git push/публикация PR, Maven deploy/JFrog publish, Docker push,
production GCP queries/jobs/writes, изменение Spark image, отправка сообщений людям,
host Homebrew/SDKMAN installations, privileged binfmt/system setup, очистка общих
caches, произвольное клонирование новых исходников. Недостающий checkout для
исследования просить у пользователя. Если реально понадобится иной private registry
или иной источник, сначала назвать его и получить расширение scope.

Прямое согласие пользователя:

> Разрешаю на всю миграцию Comet1.2/Spark4.2.1 сетевые операции, перечисленные в docs/joom-migration/NETWORK.md: Maven/SBT, Rustup/Cargo, закреплённые git-зависимости и build tools с GitHub, Docker Hub и Ubuntu packages, включая дочерние процессы внутри контейнеров. При условии, что все скачиваемое на мой ноут из сети и запускаемое - является известными и широкоиспользуемыми артефактами, в безопасности которых для моей машины не приходится сомневаться. Без push, внешней публикации и production операций.

Условие исполнения: официальные источники, известные широко используемые артефакты, закреплённые версии/commit и доступные publisher checksums; внешние installer/build scripts проверять до исполнения. Registry/lockfile не отменяют пользовательское условие безопасности. Нестандартные или сомнительные инструменты/источники согласовывать отдельно. Исходный перечень операций и hosts этим обновлением не расширен.
