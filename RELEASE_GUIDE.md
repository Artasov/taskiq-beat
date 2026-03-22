# Release Guide

Этот файл описывает текущий процесс релиза `taskiq-beat`.

## Как релизится проект сейчас

- На `pull request` и `push` в `master` GitHub Actions гоняет `ruff`, `mypy`, `pytest`, `build`, `twine check`.
- На `push` в `master` после успешного CI GitHub Actions автоматически публикует пакет в production PyPI, но только если
  реально изменился `__version__`.
- Публикация настроена через PyPI Trusted Publishing, то есть без постоянного `PYPI_TOKEN` в секретах.
- Версия пакета берётся из [`taskiq_beat/_version.py`](./taskiq_beat/_version.py).

## SemVer: когда patch, minor, major

- `patch`: только обратно-совместимые багфиксы, небольшие внутренние правки, документация без изменения публичного API.
  Пример: `0.1.0 -> 0.1.1`
- `minor`: новый обратно-совместимый функционал, новые параметры, новые публичные возможности без ломающих изменений.
  Пример: `0.1.0 -> 0.2.0`
- `major`: ломающие изменения API, форматов, поведения по умолчанию, миграций или требований к интеграции.
  Пример: `0.9.0 -> 1.0.0`

Если сомнение между `patch` и `minor`, обычно это `minor`. Если пользователю нужно менять свой код, это уже кандидат в
`major`.

## Команды для проверки перед релизом

Минимальный набор:

```bash
python -m pip install --upgrade build twine pytest ruff mypy
python -m ruff check .
python -m mypy
python -m pytest -q
```

Если нужен чистый rebuild, перед сборкой удалить старые артефакты:

```powershell
Remove-Item -Recurse -Force .\build, .\dist, .\taskiq_beat.egg-info -ErrorAction Ignore
```

## Рекомендуемый порядок релиза

1. Довести изменения до merge-ready состояния.
2. Убедиться, что версия выбрана правильно: `patch`, `minor` или `major`.
3. Поднять версию в [`taskiq_beat/_version.py`](./taskiq_beat/_version.py).
4. Обновить `README`, если менялся API, запуск, конфиг или ограничения.
5. Прогнать `ruff`, `mypy`, `pytest`.
6. Собрать пакет: `python -m build`
7. Проверить метаданные: `python -m twine check dist/*`
8. Закоммитить version bump и запушить в `master`.
9. Дождаться успешного workflow [`ci.yml`](./.github/workflows/ci.yml).
10. Проверить страницу пакета в PyPI и установку конкретной версии в чистом окружении.

## Что проверить руками после публикации

- Открывается страница пакета в PyPI
- Версия в PyPI совпадает с версией из релиза
- `README.md` нормально отрендерился на PyPI
- Пакет ставится командой:

```bash
pip install taskiq-beat==X.Y.Z
```

- Базовый импорт работает:

```bash
python -c "import taskiq_beat; print(taskiq_beat.__version__)"
```